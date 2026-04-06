"""
Pipeline orchestrator.

Chains the four analysis stages (detect → diagnose → remediate → validate)
and manages output to both terminal and files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule

from taproot_rca.connectors.postgres import SchemaSnapshot
from taproot_rca.config import PromptRole, TaprootConfig
from taproot_rca.ollama_client import ChatResponse, OllamaClient
from taproot_rca.prompt_engine import PromptContext, PromptEngine
from taproot_rca.schema_diff import SchemaDiff

console = Console()


@dataclass
class StageResult:
    """Output from a single pipeline stage."""
    role: PromptRole
    content: str
    duration_seconds: Optional[float] = None
    eval_count: Optional[int] = None
    model: str = ""


@dataclass
class PipelineResult:
    """Complete output from all pipeline stages."""
    source_name: str
    stages: list[StageResult] = field(default_factory=list)
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def get_stage(self, role: PromptRole) -> Optional[StageResult]:
        return next((s for s in self.stages if s.role == role), None)

    @property
    def remediation_content(self) -> Optional[str]:
        stage = self.get_stage(PromptRole.REMEDIATE)
        return stage.content if stage else None

    @property
    def validation_verdict(self) -> Optional[str]:
        stage = self.get_stage(PromptRole.VALIDATE)
        return stage.content if stage else None

    @property
    def total_duration(self) -> float:
        return sum(s.duration_seconds or 0 for s in self.stages)

    @property
    def total_tokens(self) -> int:
        return sum(s.eval_count or 0 for s in self.stages)


STAGE_LABELS = {
    PromptRole.DETECT: ("1/4", "Detecting drift", "yellow"),
    PromptRole.DIAGNOSE: ("2/4", "Diagnosing root cause", "blue"),
    PromptRole.REMEDIATE: ("3/4", "Generating remediation", "magenta"),
    PromptRole.VALIDATE: ("4/4", "Validating migration safety", "green"),
}


class Pipeline:
    """
    Orchestrates the full analysis pipeline.

    Runs each stage in sequence, passing context forward:
      detect  → diff text + schema DDL
      diagnose → diff text + detect output as context
      remediate → diff text + database type as context
      validate → remediation output as context
    """

    def __init__(
        self,
        config: TaprootConfig,
        client: OllamaClient,
        stream: bool = True,
    ):
        self.config = config
        self.client = client
        self.engine = PromptEngine(config)
        self.stream = stream

    def run(
        self,
        diff: SchemaDiff,
        before: SchemaSnapshot,
        after: SchemaSnapshot,
    ) -> PipelineResult:
        """
        Run all available pipeline stages in sequence.

        Only runs stages that have prompt templates defined in the config.
        Always runs detect first, then chains the rest.
        """
        result = PipelineResult(source_name=after.source_name)

        # Base context shared by all stages
        base_ctx = PromptContext(
            source_name=after.source_name,
            schema_before=before.to_ddl(),
            schema_after=after.to_ddl(),
            diff=diff.to_diff_text(),
        )

        # Stage 1: Detect
        if self.engine.has_role(PromptRole.DETECT):
            detect_result = self._run_stage(PromptRole.DETECT, base_ctx)
            result.stages.append(detect_result)
        else:
            console.print("[yellow]⚠[/yellow] No detect prompt defined, skipping.")

        # Stage 2: Diagnose
        if self.engine.has_role(PromptRole.DIAGNOSE):
            diagnose_ctx = PromptContext(
                source_name=after.source_name,
                diff=diff.to_diff_text(),
                context=detect_result.content if result.get_stage(PromptRole.DETECT) else "",
            )
            diagnose_result = self._run_stage(PromptRole.DIAGNOSE, diagnose_ctx)
            result.stages.append(diagnose_result)

        # Stage 3: Remediate
        if self.engine.has_role(PromptRole.REMEDIATE):
            # Determine database type from config
            source_cfg = next(
                (s for s in self.config.sources if s.name == after.source_name),
                None,
            )
            db_type = source_cfg.type.value if source_cfg else "postgres"

            remediate_ctx = PromptContext(
                source_name=after.source_name,
                diff=diff.to_diff_text(),
                context=db_type,
            )
            remediate_result = self._run_stage(PromptRole.REMEDIATE, remediate_ctx)
            result.stages.append(remediate_result)

        # Stage 4: Validate
        if self.engine.has_role(PromptRole.VALIDATE):
            remediation = result.remediation_content or "No remediation generated."
            validate_ctx = PromptContext(
                source_name=after.source_name,
                context=remediation,
            )
            validate_result = self._run_stage(PromptRole.VALIDATE, validate_ctx)
            result.stages.append(validate_result)

        # Print summary
        self._print_summary(result)

        return result

    def _run_stage(self, role: PromptRole, ctx: PromptContext) -> StageResult:
        """Run a single pipeline stage."""
        step, label, color = STAGE_LABELS[role]

        console.print()
        console.print(Rule(f"[bold {color}][{step}] {label}[/bold {color}]"))
        console.print()

        prompt = self.engine.hydrate(role, ctx)

        response = self.client.chat(
            system=prompt.system,
            user=prompt.user,
            stream=self.stream,
        )

        if not self.stream:
            console.print(response.content)

        return StageResult(
            role=role,
            content=response.content,
            duration_seconds=response.duration_seconds,
            eval_count=response.eval_count,
            model=response.model,
        )

    def _print_summary(self, result: PipelineResult) -> None:
        """Print a final summary of all pipeline stages."""
        console.print()
        console.print(Rule("[bold]Pipeline Complete[/bold]"))
        console.print()

        for stage in result.stages:
            step, label, color = STAGE_LABELS[stage.role]
            time_str = f"{stage.duration_seconds:.1f}s" if stage.duration_seconds else "?"
            tokens_str = str(stage.eval_count) if stage.eval_count else "?"
            console.print(
                f"  [{color}]✓[/{color}] {label}: "
                f"[dim]{time_str} | {tokens_str} tokens[/dim]"
            )

        console.print(
            f"\n  [bold]Total:[/bold] "
            f"{result.total_duration:.1f}s | "
            f"{result.total_tokens} tokens"
        )


def save_pipeline_report(
    result: PipelineResult,
    diff: SchemaDiff,
    output_dir: str = ".taproot/reports",
) -> Path:
    """
    Save the full pipeline output as a Markdown report.

    Returns the path to the saved report.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_name = result.source_name.replace("/", "_").replace(" ", "_")
    report_path = out_dir / f"{safe_name}_{timestamp}.md"

    sections: list[str] = [
        f"# Drift Analysis Report — {result.source_name}",
        f"",
        f"> Generated by Taproot-RCA on {result.timestamp}",
        f">",
        f"> Pipeline: {len(result.stages)} stage(s) | "
        f"Total time: {result.total_duration:.1f}s | "
        f"Total tokens: {result.total_tokens}",
        f"",
        f"---",
        f"",
        f"## Drift Summary",
        f"",
        diff.to_diff_text(),
        f"",
    ]

    stage_titles = {
        PromptRole.DETECT: "Detection Analysis",
        PromptRole.DIAGNOSE: "Root Cause Diagnosis",
        PromptRole.REMEDIATE: "Proposed Remediation",
        PromptRole.VALIDATE: "Safety Validation",
    }

    for stage in result.stages:
        title = stage_titles.get(stage.role, stage.role.value.title())
        time_str = f"{stage.duration_seconds:.1f}s" if stage.duration_seconds else "?"
        sections.extend([
            f"---",
            f"",
            f"## {title}",
            f"",
            f"*Model: {stage.model} | Time: {time_str} | "
            f"Tokens: {stage.eval_count or '?'}*",
            f"",
            stage.content,
            f"",
        ])

    report_path.write_text("\n".join(sections))
    return report_path