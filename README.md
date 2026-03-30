# 🌱 Taproot-RCA

**AI-powered schema drift detection and self-healing for data engineers.**

Taproot-RCA uses local LLMs (via [Ollama](https://ollama.ai)) to continuously monitor your data sources for schema drift, diagnose root causes, generate safe remediation migrations, and push fixes to your Git repositories — all configured through a single YAML file.

---

## Why Taproot-RCA?

Data engineers deal with schema drift constantly: a column gets renamed in Salesforce, a type changes in Postgres, a new field appears in an S3 Parquet file. These silent changes break pipelines, corrupt reports, and erode trust in data.

Taproot-RCA brings **local AI** into the loop:

- **Detect** — Snapshot schemas on a schedule and surface meaningful drift
- **Diagnose** — Root-cause analysis: was this intentional or accidental?
- **Remediate** — Generate safe, reversible migration DDL
- **Validate** — LLM-powered review before anything gets applied
- **Self-Heal** — Push fixes to Git with auto-PR support

All powered by models running on *your* machine. No data leaves your network.

---

## Quickstart

### Prerequisites

- Python 3.10+
- [Ollama](https://ollama.ai) installed and running (`ollama serve`)

### Install

```bash
pip install taproot-rca

# With database connectors:
pip install taproot-rca[postgres]
pip install taproot-rca[all-connectors]
```

### Initialize

```bash
# Generate a starter config
taproot init

# Edit taproot.yaml with your sources and preferences
# ...

# Validate your config
taproot validate
```

### Model Management

```bash
# Check if your configured models are available locally
taproot models

# Auto-download missing models from Ollama
taproot models --pull
```

---

## Configuration

Taproot-RCA is driven by a single `taproot.yaml` file. Run `taproot init` to generate a fully commented starter config.

### Structure

```yaml
version: "1"

model:
  name: "llama3:8b"             # Ollama model tag
  host: "http://localhost:11434" # Ollama server URL
  fallback: "mistral"           # Fallback if primary unavailable
  temperature: 0.1
  context_length: 4096

prompts:
  - role: detect                 # detect | diagnose | remediate | validate
    system: "..."                # System prompt
    user_template: "..."         # User prompt with {placeholders}

sources:
  - name: "my-postgres"
    type: postgres               # postgres | mysql | snowflake
    connection_string: "postgresql://${PG_USER}:${PG_PASS}@localhost/db"
    schemas: [public, analytics]
    poll_interval_seconds: 3600

git:                             # Optional — for self-healing
  repo_url: "git@github.com:org/migrations.git"
  branch: "taproot/auto-heal"
  base_branch: "main"
  auto_pr: false
```

### Environment Variables

Connection strings support `${VAR_NAME}` interpolation so you never commit secrets.

### Prompt Placeholders

| Placeholder       | Description                          |
|--------------------|--------------------------------------|
| `{source_name}`   | Human-readable source identifier     |
| `{schema_before}` | Previous schema snapshot             |
| `{schema_after}`  | Current schema snapshot              |
| `{diff}`          | Computed diff between snapshots      |
| `{context}`       | Additional context (DB type, etc.)   |

---

## Roadmap

- [x] Project scaffolding & config schema
- [x] Ollama model management (check / pull)
- [ ] SQL source connectors (Postgres, MySQL, Snowflake)
- [ ] Schema snapshot & diff engine
- [ ] AI-driven drift analysis pipeline
- [ ] Self-healing: migration generation + Git push
- [ ] SaaS connectors (Salesforce, HubSpot)
- [ ] Cloud storage connectors (S3 Parquet/CSV)
- [ ] Scheduled monitoring daemon
- [ ] Web dashboard

---

## Development

```bash
git clone https://github.com/your-org/taproot-rca.git
cd taproot-rca
pip install -e ".[dev,all-connectors]"
pytest
```

---

## License

MIT
