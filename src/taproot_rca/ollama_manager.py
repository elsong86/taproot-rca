"""
Ollama model manager.

Handles:
  - Checking if a model is available locally
  - Pulling (downloading) models that are missing
  - Listing all locally available models
  - Health-checking the Ollama server
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import httpx
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

console = Console()


@dataclass
class ModelInfo:
    name: str
    size: Optional[int] = None  # bytes
    modified_at: Optional[str] = None
    digest: Optional[str] = None


class OllamaManager:
    """Manages interactions with the local Ollama instance."""

    def __init__(self, host: str = "http://localhost:11434", timeout: float = 30.0):
        self.host = host.rstrip("/")
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def is_server_running(self) -> bool:
        """Check if the Ollama server is reachable."""
        try:
            resp = httpx.get(f"{self.host}/", timeout=5.0)
            return resp.status_code == 200
        except httpx.ConnectError:
            return False
        except Exception:
            return False

    # ------------------------------------------------------------------
    # List local models
    # ------------------------------------------------------------------

    def list_local_models(self) -> list[ModelInfo]:
        """Return all models currently available on the local Ollama instance."""
        resp = httpx.get(f"{self.host}/api/tags", timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        models: list[ModelInfo] = []
        for m in data.get("models", []):
            models.append(
                ModelInfo(
                    name=m.get("name", ""),
                    size=m.get("size"),
                    modified_at=m.get("modified_at"),
                    digest=m.get("digest", ""),
                )
            )
        return models

    def is_model_available(self, model_name: str) -> bool:
        """Check whether a specific model tag exists locally."""
        try:
            local = self.list_local_models()
        except Exception:
            return False

        # Ollama tags can include variants — match with and without ':latest'
        normalized = self._normalize_tag(model_name)
        return any(self._normalize_tag(m.name) == normalized for m in local)

    # ------------------------------------------------------------------
    # Pull (download) a model
    # ------------------------------------------------------------------

    def pull_model(self, model_name: str, stream: bool = True) -> bool:
        """
        Pull a model from the Ollama registry.

        When *stream* is True, displays a Rich progress bar as layers download.
        Returns True on success, False on failure.
        """
        console.print(f"\n[bold cyan]Pulling model:[/bold cyan] {model_name}")

        if not stream:
            return self._pull_blocking(model_name)

        return self._pull_streaming(model_name)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _pull_blocking(self, model_name: str) -> bool:
        try:
            resp = httpx.post(
                f"{self.host}/api/pull",
                json={"name": model_name, "stream": False},
                timeout=None,  # pulls can take a very long time
            )
            resp.raise_for_status()
            console.print(f"[green]✓[/green] Model '{model_name}' pulled successfully.")
            return True
        except Exception as exc:
            console.print(f"[red]✗[/red] Failed to pull model: {exc}")
            return False

    def _pull_streaming(self, model_name: str) -> bool:
        """Stream the pull and show per-layer progress."""
        try:
            with httpx.stream(
                "POST",
                f"{self.host}/api/pull",
                json={"name": model_name, "stream": True},
                timeout=None,
            ) as resp:
                resp.raise_for_status()

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    console=console,
                ) as progress:
                    task = progress.add_task("Downloading…", total=None)

                    for line in resp.iter_lines():
                        if not line:
                            continue

                        import json as _json

                        try:
                            chunk = _json.loads(line)
                        except _json.JSONDecodeError:
                            continue

                        status = chunk.get("status", "")
                        progress.update(task, description=status[:60])

                        # Ollama sends total/completed for layer downloads
                        total = chunk.get("total")
                        completed = chunk.get("completed")
                        if total:
                            progress.update(task, total=total, completed=completed or 0)

                        # Check for error
                        if "error" in chunk:
                            console.print(f"[red]✗[/red] Ollama error: {chunk['error']}")
                            return False

            console.print(f"[green]✓[/green] Model '{model_name}' is ready.")
            return True

        except httpx.ConnectError:
            console.print(
                "[red]✗[/red] Cannot connect to Ollama. "
                "Is the server running? Start it with: [bold]ollama serve[/bold]"
            )
            return False
        except Exception as exc:
            console.print(f"[red]✗[/red] Failed to pull model: {exc}")
            return False

    @staticmethod
    def _normalize_tag(tag: str) -> str:
        """Normalize a model tag for comparison (e.g. 'llama3' → 'llama3:latest')."""
        if ":" not in tag:
            return f"{tag}:latest"
        return tag
