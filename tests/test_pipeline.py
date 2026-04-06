"""Tests for pipeline orchestrator."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from taproot_rca.config import TaprootConfig, PromptRole
from taproot_rca.pipeline import Pipeline, PipelineResult, StageResult, save_pipeline_report
from taproot_rca.demo import get_demo_before, get_demo_after
from taproot_rca.schema_diff import diff_snapshots


FULL_CONFIG_DICT = {
    "version": "1",
    "model": {"name": "test-model"},
    "prompts": [
        {
            "role": "detect",
            "system": "Detect drift.",
            "user_template": "Source: {source_name}\nDiff: {diff}",
        },
        {
            "role": "diagnose",
            "system": "Diagnose root cause.",
            "user_template": "Source: {source_name}\nDiff: {diff}\nContext: {context}",
        },
        {
            "role": "remediate",
            "system": "Generate migration.",
            "user_template": "Source: {source_name}\nDiff: {diff}\nDB: {context}",
        },
        {
            "role": "validate",
            "system": "Validate migration.",
            "user_template": "Source: {source_name}\nMigration: {context}",
        },
    ],
    "sources": [
        {
            "name": "demo-ecommerce",
            "type": "postgres",
            "connection_string": "postgresql://localhost/db",
        },
    ],
}


def _mock_client(responses: list[str]) -> MagicMock:
    """Create a mock OllamaClient that returns predefined responses."""
    client = MagicMock()
    mock_responses = []
    for text in responses:
        resp = MagicMock()
        resp.content = text
        resp.model = "test-model"
        resp.duration_seconds = 1.0
        resp.eval_count = 50
        resp.total_duration_ns = 1_000_000_000
        mock_responses.append(resp)

    client.chat.side_effect = mock_responses
    return client


class TestPipeline:
    def test_runs_all_four_stages(self):
        cfg = TaprootConfig(**FULL_CONFIG_DICT)
        client = _mock_client([
            "Drift detected: column added",
            "Root cause: intentional migration",
            "ALTER TABLE ADD COLUMN...",
            "SAFE: migration is reversible",
        ])

        pipeline = Pipeline(config=cfg, client=client, stream=False)

        before = get_demo_before()
        after = get_demo_after()
        diff = diff_snapshots(before, after)

        result = pipeline.run(diff=diff, before=before, after=after)

        assert len(result.stages) == 4
        assert result.stages[0].role == PromptRole.DETECT
        assert result.stages[1].role == PromptRole.DIAGNOSE
        assert result.stages[2].role == PromptRole.REMEDIATE
        assert result.stages[3].role == PromptRole.VALIDATE
        assert client.chat.call_count == 4

    def test_skips_undefined_stages(self):
        # Config with only detect and diagnose
        partial_config = {
            **FULL_CONFIG_DICT,
            "prompts": FULL_CONFIG_DICT["prompts"][:2],
        }
        cfg = TaprootConfig(**partial_config)
        client = _mock_client([
            "Drift detected",
            "Root cause analysis",
        ])

        pipeline = Pipeline(config=cfg, client=client, stream=False)

        before = get_demo_before()
        after = get_demo_after()
        diff = diff_snapshots(before, after)

        result = pipeline.run(diff=diff, before=before, after=after)

        assert len(result.stages) == 2
        assert client.chat.call_count == 2

    def test_context_chains_between_stages(self):
        cfg = TaprootConfig(**FULL_CONFIG_DICT)
        client = _mock_client([
            "DETECT: found column rename",
            "DIAGNOSE: intentional rename",
            "REMEDIATE: ALTER TABLE...",
            "VALIDATE: SAFE",
        ])

        pipeline = Pipeline(config=cfg, client=client, stream=False)

        before = get_demo_before()
        after = get_demo_after()
        diff = diff_snapshots(before, after)

        result = pipeline.run(diff=diff, before=before, after=after)

        # Diagnose should receive detect output as context
        diagnose_call = client.chat.call_args_list[1]
        assert "DETECT: found column rename" in diagnose_call.kwargs.get("user", "") or \
               "DETECT: found column rename" in str(diagnose_call)

        # Validate should receive remediation output as context
        validate_call = client.chat.call_args_list[3]
        assert "ALTER TABLE" in str(validate_call)


class TestPipelineResult:
    def test_total_duration(self):
        result = PipelineResult(
            source_name="test",
            stages=[
                StageResult(role=PromptRole.DETECT, content="x", duration_seconds=2.0),
                StageResult(role=PromptRole.DIAGNOSE, content="y", duration_seconds=3.0),
            ],
        )
        assert result.total_duration == 5.0

    def test_total_tokens(self):
        result = PipelineResult(
            source_name="test",
            stages=[
                StageResult(role=PromptRole.DETECT, content="x", eval_count=100),
                StageResult(role=PromptRole.DIAGNOSE, content="y", eval_count=200),
            ],
        )
        assert result.total_tokens == 300

    def test_get_stage(self):
        result = PipelineResult(
            source_name="test",
            stages=[
                StageResult(role=PromptRole.DETECT, content="detected"),
                StageResult(role=PromptRole.REMEDIATE, content="ALTER TABLE..."),
            ],
        )
        assert result.get_stage(PromptRole.DETECT).content == "detected"
        assert result.get_stage(PromptRole.DIAGNOSE) is None
        assert result.remediation_content == "ALTER TABLE..."


class TestSavePipelineReport:
    def test_saves_report_file(self, tmp_path):
        result = PipelineResult(
            source_name="test-db",
            stages=[
                StageResult(role=PromptRole.DETECT, content="Drift found", duration_seconds=1.0, eval_count=50, model="test"),
                StageResult(role=PromptRole.REMEDIATE, content="ALTER TABLE...", duration_seconds=2.0, eval_count=100, model="test"),
            ],
        )
        before = get_demo_before()
        after = get_demo_after()
        diff = diff_snapshots(before, after)

        path = save_pipeline_report(result, diff, output_dir=str(tmp_path))

        assert path.exists()
        content = path.read_text()
        assert "Drift Analysis Report" in content
        assert "Detection Analysis" in content
        assert "Proposed Remediation" in content
        assert "Drift found" in content
        assert "ALTER TABLE" in content