"""
Ollama chat client.

Sends prompt templates to the local Ollama instance and returns
structured responses. Supports streaming and non-streaming modes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import httpx
from rich.console import Console

console = Console()


@dataclass
class ChatResponse:
    """Structured response from an Ollama chat completion."""
    content: str
    model: str
    total_duration_ns: Optional[int] = None
    prompt_eval_count: Optional[int] = None
    eval_count: Optional[int] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.total_duration_ns:
            return self.total_duration_ns / 1e9
        return None


@dataclass
class OllamaClient:
    """
    Chat client for the Ollama API.

    Usage:
        client = OllamaClient(host="http://localhost:11434", model="llama3:8b")
        response = client.chat(system="You are a DBA.", user="Analyze this diff...")
    """

    host: str = "http://localhost:11434"
    model: str = "llama3:8b"
    temperature: float = 0.1
    context_length: int = 4096

    def chat(
        self,
        system: str,
        user: str,
        stream: bool = False,
    ) -> ChatResponse:
        """
        Send a chat completion request to Ollama.

        Args:
            system: System-level instruction.
            user: User message (the actual query/prompt).
            stream: If True, stream tokens to console as they arrive.

        Returns:
            ChatResponse with the full model output.
        """
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": stream,
            "options": {
                "temperature": self.temperature,
                "num_ctx": self.context_length,
            },
        }

        if stream:
            return self._chat_streaming(payload)
        return self._chat_blocking(payload)

    def _chat_blocking(self, payload: dict) -> ChatResponse:
        """Non-streaming chat — waits for the full response."""
        resp = httpx.post(
            f"{self.host}/api/chat",
            json=payload,
            timeout=None,  # LLM responses can be slow
        )
        resp.raise_for_status()
        data = resp.json()

        return ChatResponse(
            content=data.get("message", {}).get("content", ""),
            model=data.get("model", self.model),
            total_duration_ns=data.get("total_duration"),
            prompt_eval_count=data.get("prompt_eval_count"),
            eval_count=data.get("eval_count"),
        )

    def _chat_streaming(self, payload: dict) -> ChatResponse:
        """Streaming chat — prints tokens as they arrive."""
        import json as _json

        chunks: list[str] = []
        final_data: dict = {}

        with httpx.stream(
            "POST",
            f"{self.host}/api/chat",
            json=payload,
            timeout=None,
        ) as resp:
            resp.raise_for_status()

            for line in resp.iter_lines():
                if not line:
                    continue

                try:
                    chunk = _json.loads(line)
                except _json.JSONDecodeError:
                    continue

                # Extract token content
                token = chunk.get("message", {}).get("content", "")
                if token:
                    chunks.append(token)
                    console.print(token, end="", highlight=False)

                # The last chunk has done=True and includes timing stats
                if chunk.get("done"):
                    final_data = chunk
                    console.print()  # newline after streaming

        full_content = "".join(chunks)

        return ChatResponse(
            content=full_content,
            model=final_data.get("model", self.model),
            total_duration_ns=final_data.get("total_duration"),
            prompt_eval_count=final_data.get("prompt_eval_count"),
            eval_count=final_data.get("eval_count"),
        )