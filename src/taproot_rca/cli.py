"""
Taproot-RCA CLI — the main entry point.

Commands
--------
  init      Scaffold a new taproot.yaml config file
  validate  Validate an existing config file
  models    Check / pull Ollama models defined in the config
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
