"""
Taproot-RCA CLI — the main entry point.

Commands
--------
  init      Scaffold a new taproot.yaml config file
  validate  Validate an existing config file
  models    Check / pull Ollama models defined in the config
  scan      Detect schema drift and analyze with the LLM
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="taproot",
    help="AI-powered schema drift detection & self-healing for data engineers.",
    add_completion=False,
)
console = Console()

DEFAULT_CONFIG = "taproot.yaml"


# ──────────────────────────────────────────────────────────────────────
# init
# ──────────────────────────────────────────────────────────────────────

@app.command()
def init(
    output: str = typer.Option(DEFAULT_CONFIG, "--output", "-o", help="Output file path"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite if file exists"),
):
    """Scaffold a starter taproot.yaml configuration file."""
    from taproot_rca.scaffold import write_starter_config

    dest = Path(output)
    if dest.exists() and not force:
        console.print(
            f"[yellow]⚠[/yellow]  Config already exists at [bold]{dest}[/bold]. "
            "Use --force to overwrite."
        )
        raise typer.Exit(code=1)

    write_starter_config(dest)
    console.print(f"[green]✓[/green] Created starter config at [bold]{dest}[/bold]")
    console.print("  Edit the file to add your data sources and tweak prompts, then run:")
    console.print("  [dim]taproot validate[/dim]")


# ──────────────────────────────────────────────────────────────────────
# validate
# ──────────────────────────────────────────────────────────────────────

@app.command()
def validate(
    config: str = typer.Option(DEFAULT_CONFIG, "--config", "-c", help="Config file path"),
):
    """Validate a taproot.yaml configuration file."""
    from taproot_rca.config import load_config

    try:
        cfg = load_config(config)
    except FileNotFoundError:
        console.print(f"[red]✗[/red] File not found: [bold]{config}[/bold]")
        console.print("  Run [bold]taproot init[/bold] to create one.")
        raise typer.Exit(code=1)
    except Exception as exc:
        console.print(f"[red]✗[/red] Validation failed:\n  {exc}")
        raise typer.Exit(code=1)

    console.print(f"[green]✓[/green] Config is valid  ([dim]{config}[/dim])")

    # Summary table
    table = Table(title="Configuration Summary", show_lines=False)
    table.add_column("Setting", style="cyan")
    table.add_column("Value")
    table.add_row("Model", cfg.model.name)
    table.add_row("Fallback", cfg.model.fallback or "—")
    table.add_row("Prompts", ", ".join(p.role.value for p in cfg.prompts))
    table.add_row("Sources", ", ".join(s.name for s in cfg.sources))
    table.add_row("Git target", cfg.git.repo_url if cfg.git else "—")
    console.print(table)


# ──────────────────────────────────────────────────────────────────────
# models
# ──────────────────────────────────────────────────────────────────────

@app.command()
def models(
    config: str = typer.Option(DEFAULT_CONFIG, "--config", "-c", help="Config file path"),
    pull: bool = typer.Option(False, "--pull", "-p", help="Pull missing models automatically"),
):
    """Check availability of Ollama models and optionally pull missing ones."""
    from taproot_rca.config import load_config
    from taproot_rca.ollama_manager import OllamaManager

    try:
        cfg = load_config(config)
    except Exception as exc:
        console.print(f"[red]✗[/red] Config error: {exc}")
        raise typer.Exit(code=1)

    manager = OllamaManager(host=cfg.model.host)

    # 1. Server health
    if not manager.is_server_running():
        console.print(
            "[red]✗[/red] Ollama server is not reachable at "
            f"[bold]{cfg.model.host}[/bold]\n"
            "  Start it with: [bold]ollama serve[/bold]"
        )
        raise typer.Exit(code=1)

    console.print(f"[green]✓[/green] Ollama server is running at {cfg.model.host}\n")

    # 2. Check required models
    required: list[str] = [cfg.model.name]
    if cfg.model.fallback:
        required.append(cfg.model.fallback)

    missing: list[str] = []

    for model_tag in required:
        available = manager.is_model_available(model_tag)
        icon = "[green]✓[/green]" if available else "[yellow]✗[/yellow]"
        label = "available" if available else "not found locally"
        console.print(f"  {icon}  {model_tag}  ({label})")
        if not available:
            missing.append(model_tag)

    # 3. Pull missing models if requested
    if missing and pull:
        console.print()
        for tag in missing:
            ok = manager.pull_model(tag)
            if not ok:
                console.print(f"\n[red]✗[/red] Could not pull [bold]{tag}[/bold]. Aborting.")
                raise typer.Exit(code=1)
    elif missing and not pull:
        console.print(
            f"\n[yellow]⚠[/yellow]  {len(missing)} model(s) missing. "
            "Run with [bold]--pull[/bold] to download them:\n"
            f"  [dim]taproot models --pull[/dim]"
        )

    # 4. Show all local models
    console.print("\n[bold]All local Ollama models:[/bold]")
    table = Table(show_lines=False)
    table.add_column("Model", style="cyan")
    table.add_column("Size")
    table.add_column("Modified")

    for m in manager.list_local_models():
        size_str = _format_bytes(m.size) if m.size else "—"
        table.add_row(m.name, size_str, m.modified_at or "—")

    console.print(table)


# ──────────────────────────────────────────────────────────────────────
# scan
# ──────────────────────────────────────────────────────────────────────

@app.command()
def scan(
    config: str = typer.Option(DEFAULT_CONFIG, "--config", "-c", help="Config file path"),
    source: Optional[str] = typer.Option(None, "--source", "-s", help="Scan a specific source by name"),
    demo: bool = typer.Option(False, "--demo", help="Run with built-in demo data (no database needed)"),
    stream: bool = typer.Option(True, "--stream/--no-stream", help="Stream LLM output in real-time"),
):
    """Detect schema drift and analyze it with the configured LLM."""
    from taproot_rca.config import load_config, PromptRole
    from taproot_rca.ollama_client import OllamaClient
    from taproot_rca.ollama_manager import OllamaManager
    from taproot_rca.prompt_engine import PromptEngine, PromptContext
    from taproot_rca.schema_diff import diff_snapshots
    from taproot_rca.snapshot_store import SnapshotStore

    # 1. Load config
    try:
        cfg = load_config(config)
    except Exception as exc:
        console.print(f"[red]✗[/red] Config error: {exc}")
        raise typer.Exit(code=1)

    # 2. Get snapshots (demo or live)
    store = SnapshotStore(cfg.snapshot_dir)

    if demo:
        from taproot_rca.demo import get_demo_before, get_demo_after

        console.print("[bold cyan]Running in demo mode[/bold cyan] — using built-in sample schema\n")
        before = get_demo_before()
        after = get_demo_after()

        # Save and diff
        snap_path = store.save(after)
        console.print(f"[dim]Snapshot saved: {snap_path}[/dim]\n")

        diff = diff_snapshots(before, after)

    else:
        # Live mode — introspect from database
        from taproot_rca.connectors.postgres import PostgresIntrospector
        from taproot_rca.env_resolver import resolve_env_vars

        # Filter sources if --source was provided
        sources_to_scan = cfg.sources
        if source:
            sources_to_scan = [s for s in cfg.sources if s.name == source]
            if not sources_to_scan:
                available = ", ".join(s.name for s in cfg.sources)
                console.print(
                    f"[red]✗[/red] Source '{source}' not found in config. "
                    f"Available: {available}"
                )
                raise typer.Exit(code=1)

        # For now, scan the first matching source
        # (multi-source scanning in a future step)
        src_cfg = sources_to_scan[0]
        console.print(f"[bold cyan]Scanning source:[/bold cyan] {src_cfg.name} ({src_cfg.type.value})\n")

        # Resolve env vars in connection string
        try:
            conn_string = resolve_env_vars(src_cfg.connection_string)
        except EnvironmentError as exc:
            console.print(f"[red]✗[/red] {exc}")
            raise typer.Exit(code=1)

        # Introspect current schema
        try:
            introspector = PostgresIntrospector(conn_string)
            after = introspector.snapshot(
                schemas=src_cfg.schemas,
                source_name=src_cfg.name,
            )
        except ImportError:
            console.print(
                "[red]✗[/red] psycopg2 is required for Postgres scanning.\n"
                "  Install it with: [bold]pip install taproot-rca[postgres][/bold]\n"
                "  Or directly: [bold]pip install psycopg2-binary[/bold]"
            )
            raise typer.Exit(code=1)
        except Exception as exc:
            console.print(f"[red]✗[/red] Failed to connect to {src_cfg.name}: {exc}")
            raise typer.Exit(code=1)

        console.print(
            f"[green]✓[/green] Introspected {len(after.tables)} table(s) "
            f"from {', '.join(src_cfg.schemas)}\n"
        )

        # Save snapshot
        snap_path = store.save(after)
        console.print(f"[dim]Snapshot saved: {snap_path}[/dim]\n")

        # Load previous snapshot for diffing
        snapshots = store.list_snapshots(src_cfg.name)
        if len(snapshots) < 2:
            console.print(
                "[yellow]⚠[/yellow]  First scan — no previous snapshot to diff against.\n"
                "  This snapshot is now your baseline. Run [bold]taproot scan[/bold] again\n"
                "  after schema changes to detect drift."
            )
            # Show what was captured
            console.print(f"\n[bold]Captured schema:[/bold]\n")
            for t in after.tables:
                cols = ", ".join(c.name for c in t.columns)
                console.print(f"  [cyan]{t.full_name}[/cyan] ({cols})")
            return

        # We have at least 2 snapshots — diff the latest two
        before = store._load_snapshot(snapshots[1])  # second-newest
        diff = diff_snapshots(before, after)

    # 3. Evaluate drift
    if not diff.has_drift:
        console.print("[green]✓[/green] No schema drift detected.")
        return

    console.print(f"[yellow]⚠[/yellow]  {diff.summary}\n")
    console.print("[dim]" + diff.to_diff_text() + "[/dim]\n")

    # 4. Check Ollama is available
    manager = OllamaManager(host=cfg.model.host)
    if not manager.is_server_running():
        console.print(
            "[red]✗[/red] Ollama server not reachable. "
            "Start it with: [bold]ollama serve[/bold]"
        )
        raise typer.Exit(code=1)

    # Determine which model to use
    model_name = cfg.model.name
    if not manager.is_model_available(model_name):
        if cfg.model.fallback and manager.is_model_available(cfg.model.fallback):
            console.print(
                f"[yellow]⚠[/yellow]  Primary model '{model_name}' not found, "
                f"using fallback: {cfg.model.fallback}"
            )
            model_name = cfg.model.fallback
        else:
            console.print(
                f"[red]✗[/red] Model '{model_name}' not available. "
                "Run [bold]taproot models --pull[/bold] first."
            )
            raise typer.Exit(code=1)

    # 5. Build the prompt and send to LLM
    engine = PromptEngine(cfg)
    ctx = PromptContext(
        source_name=after.source_name,
        schema_before=before.to_ddl(),
        schema_after=after.to_ddl(),
        diff=diff.to_diff_text(),
    )

    prompt = engine.hydrate(PromptRole.DETECT, ctx)

    console.print(f"[bold cyan]Analyzing drift with {model_name}...[/bold cyan]\n")

    client = OllamaClient(
        host=cfg.model.host,
        model=model_name,
        temperature=cfg.model.temperature,
        context_length=cfg.model.context_length,
    )

    response = client.chat(
        system=prompt.system,
        user=prompt.user,
        stream=stream,
    )

    if not stream:
        # If not streaming, print the full response now
        console.print(response.content)

    # 6. Print stats
    if response.duration_seconds:
        console.print(
            f"\n[dim]Model: {response.model} | "
            f"Time: {response.duration_seconds:.1f}s | "
            f"Tokens: {response.eval_count or '?'}[/dim]"
        )


# ──────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────

def _format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ──────────────────────────────────────────────────────────────────────
# entry point
# ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app()