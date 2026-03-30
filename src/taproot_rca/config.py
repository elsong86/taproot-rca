"""
Taproot-RCA configuration schema.

Users define their entire pipeline in a single YAML file:
  - Which Ollama models to use (and fallbacks)
  - Prompt templates for drift detection and healing
  - Data source connections (SQL databases, SaaS, cloud storage)
  - Git repositories for pushing self-healing changes
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SourceType(str, Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    # Future phases
    # SALESFORCE = "salesforce"
    # S3 = "s3"
    # BIGQUERY = "bigquery"


class PromptRole(str, Enum):
    """Built-in prompt roles that map to pipeline stages."""
    DETECT = "detect"          # Identify schema drift
    DIAGNOSE = "diagnose"      # Root-cause analysis
    REMEDIATE = "remediate"    # Propose a fix (migration / DDL)
    VALIDATE = "validate"      # Confirm fix is safe


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class OllamaModelConfig(BaseModel):
    """Defines which local LLM to use and connection details."""
    name: str = Field(
        ...,
        description="Ollama model tag, e.g. 'llama3:8b', 'codellama:13b', 'mistral'",
    )
    host: str = Field(
        default="http://localhost:11434",
        description="Ollama API base URL",
    )
    fallback: Optional[str] = Field(
        default=None,
        description="Fallback model tag if primary is unavailable",
    )
    temperature: float = Field(default=0.1, ge=0.0, le=2.0)
    context_length: int = Field(default=4096, gt=0)


class PromptTemplate(BaseModel):
    """A reusable prompt template tied to a pipeline stage."""
    role: PromptRole
    system: str = Field(
        ...,
        description="System-level instruction for the LLM",
    )
    user_template: str = Field(
        ...,
        description=(
            "User message template. Use {placeholders} for runtime values. "
            "Available: {source_name}, {schema_before}, {schema_after}, "
            "{diff}, {context}"
        ),
    )
    max_tokens: int = Field(default=2048, gt=0)


class DataSourceConfig(BaseModel):
    """Connection details for a single data source."""
    name: str = Field(..., description="Human-readable identifier for this source")
    type: SourceType
    connection_string: str = Field(
        ...,
        description=(
            "Connection URI. Supports env-var interpolation via ${VAR_NAME}. "
            "Example: postgresql://${PG_USER}:${PG_PASS}@localhost:5432/mydb"
        ),
    )
    schemas: list[str] = Field(
        default=["public"],
        description="Database schemas to monitor",
    )
    poll_interval_seconds: int = Field(
        default=3600,
        ge=60,
        description="How often to snapshot the schema (minimum 60s)",
    )


class GitTargetConfig(BaseModel):
    """Where to push self-healing changes."""
    repo_url: str = Field(..., description="Git remote URL (SSH or HTTPS)")
    branch: str = Field(
        default="taproot/auto-heal",
        description="Branch to push remediation commits to",
    )
    base_branch: str = Field(
        default="main",
        description="Branch to diff against / create PRs into",
    )
    commit_prefix: str = Field(
        default="[taproot-rca]",
        description="Prefix for auto-generated commit messages",
    )
    auto_pr: bool = Field(
        default=False,
        description="Automatically open a pull request after pushing",
    )


# ---------------------------------------------------------------------------
# Root config
# ---------------------------------------------------------------------------

class TaprootConfig(BaseModel):
    """Root configuration — maps 1:1 with taproot.yaml."""
    version: str = Field(default="1", description="Config schema version")
    model: OllamaModelConfig
    prompts: list[PromptTemplate] = Field(
        ...,
        min_length=1,
        description="At least one prompt template is required",
    )
    sources: list[DataSourceConfig] = Field(
        ...,
        min_length=1,
        description="At least one data source is required",
    )
    git: Optional[GitTargetConfig] = Field(
        default=None,
        description="Git target for self-healing pushes (optional in early phases)",
    )
    snapshot_dir: str = Field(
        default=".taproot/snapshots",
        description="Local directory to store schema snapshots",
    )

    @field_validator("prompts")
    @classmethod
    def unique_prompt_roles(cls, v: list[PromptTemplate]) -> list[PromptTemplate]:
        roles = [p.role for p in v]
        if len(roles) != len(set(roles)):
            raise ValueError("Each prompt role (detect, diagnose, remediate, validate) may only appear once")
        return v


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_config(path: str | Path) -> TaprootConfig:
    """Load and validate a taproot.yaml configuration file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if raw is None:
        raise ValueError(f"Config file is empty: {path}")

    return TaprootConfig(**raw)
