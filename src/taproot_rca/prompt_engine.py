"""
Prompt template engine.

Takes the PromptTemplate definitions from config and hydrates
{placeholders} with runtime schema data for each pipeline stage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from taproot_rca.config import PromptRole, PromptTemplate, TaprootConfig


@dataclass
class HydratedPrompt:
    """A prompt ready to send to the LLM."""
    role: PromptRole
    system: str
    user: str
    max_tokens: int


@dataclass
class PromptContext:
    """
    Runtime context used to fill prompt template placeholders.

    Populate whichever fields are relevant to the pipeline stage:
      - detect:    source_name, schema_before, schema_after, diff
      - diagnose:  source_name, diff, context
      - remediate: source_name, diff, context (db type)
      - validate:  source_name, context (proposed migration)
    """
    source_name: str = ""
    schema_before: str = ""
    schema_after: str = ""
    diff: str = ""
    context: str = ""


class PromptEngine:
    """
    Hydrates prompt templates from config with runtime context.

    Usage:
        engine = PromptEngine(config)
        prompt = engine.hydrate(PromptRole.DETECT, context)
    """

    def __init__(self, config: TaprootConfig):
        self._templates: dict[PromptRole, PromptTemplate] = {
            p.role: p for p in config.prompts
        }

    @property
    def available_roles(self) -> list[PromptRole]:
        return list(self._templates.keys())

    def has_role(self, role: PromptRole) -> bool:
        return role in self._templates

    def hydrate(self, role: PromptRole, ctx: PromptContext) -> HydratedPrompt:
        """
        Fill a prompt template with runtime context.

        Raises KeyError if the role isn't defined in config.
        """
        template = self._templates.get(role)
        if template is None:
            available = ", ".join(r.value for r in self._templates)
            raise KeyError(
                f"No prompt template for role '{role.value}'. "
                f"Available: {available}"
            )

        placeholders = {
            "source_name": ctx.source_name,
            "schema_before": ctx.schema_before,
            "schema_after": ctx.schema_after,
            "diff": ctx.diff,
            "context": ctx.context,
        }

        user_message = template.user_template.format(**placeholders)

        return HydratedPrompt(
            role=role,
            system=template.system,
            user=user_message,
            max_tokens=template.max_tokens,
        )