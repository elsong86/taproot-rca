# 🌱 Taproot-RCA

**AI-powered schema drift detection, documentation, and self-healing for data engineers.**

Taproot-RCA uses local LLMs (via [Ollama](https://ollama.ai)) to continuously monitor your data sources for schema drift, diagnose root causes, generate safe remediation migrations, document the changes, and push fixes to your Git repositories as reviewable pull requests — all configured through a single YAML file and powered by models running on your own machine.

---

## Why Taproot-RCA?

Data engineers deal with schema drift constantly: a column gets renamed in Postgres, a type changes in Snowflake, a Salesforce admin adds a custom field through the UI. These silent changes break pipelines, corrupt reports, and erode trust in data — and they often happen outside the Git workflow that's supposed to track them.

Taproot-RCA closes that gap by bringing **local AI** into the loop:

- **Detect** — Snapshot schemas on demand and surface meaningful drift
- **Diagnose** — Root-cause analysis: was this intentional or accidental?
- **Remediate** — Generate safe, reversible migration DDL
- **Validate** — LLM-powered review before anything gets applied
- **Document** — Auto-generate data dictionaries, lineage narratives, and changelogs
- **Self-Heal** — Push migration files to a Git branch and open a pull request automatically

All powered by models running on *your* machine. No data leaves your network unless you choose to use a cloud-hosted model.

---

## Quickstart

### Prerequisites

- Python 3.10+
- [Ollama](https://ollama.ai) installed and running (`ollama serve`)
- Docker (optional, for the local Postgres dev environment)

### Install

```bash
git clone https://github.com/elsong86/taproot-rca.git
cd taproot-rca
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,postgres]"
```

### Initialize and validate your config

```bash
taproot init                    # generates a starter taproot.yaml
taproot validate                # checks the config is well-formed
taproot models                  # checks your Ollama models are available
taproot models --pull           # downloads any missing models
```

### Try it out with the demo

```bash
taproot scan --demo             # runs the full pipeline against a built-in mock schema
```

---

## The Full Pipeline

When `taproot scan` detects drift, it runs four LLM-powered analysis stages in sequence:

1. **Detect** — Identifies what changed and rates the severity
2. **Diagnose** — Determines whether the change was intentional or accidental
3. **Remediate** — Generates forward migration DDL, rollback DDL, pre-checks, and validation queries
4. **Validate** — Reviews the proposed migration for safety and assigns a SAFE/UNSAFE/NEEDS_REVIEW verdict

After the pipeline runs, the output is saved as a complete markdown report in `.taproot/reports/`, a changelog entry is appended to `.taproot/docs/{source}/CHANGELOG.md`, and (if Git is configured) the migration is pushed to a new branch with an auto-opened pull request.

---

## Commands

| Command | Description |
|---------|-------------|
| `taproot init` | Scaffold a starter `taproot.yaml` config file |
| `taproot validate` | Validate an existing config file |
| `taproot models` | Check Ollama model availability |
| `taproot models --pull` | Download missing models |
| `taproot scan` | Detect drift, run the analysis pipeline, document, and self-heal |
| `taproot scan --demo` | Run against a built-in mock e-commerce schema |
| `taproot scan --source NAME` | Scan a specific source from your config |
| `taproot docs` | Generate AI-powered data dictionary and lineage docs |

---

## Configuration

Taproot-RCA is driven by a single `taproot.yaml` file. Run `taproot init` to generate a fully commented starter config.

### Structure

```yaml
version: "1"

model:
  name: "qwen2.5-coder:3b"          # Primary Ollama model
  host: "http://localhost:11434"
  fallback: "qwen2.5-coder:1.5b"    # Used if primary is unavailable
  temperature: 0.1
  context_length: 4096

prompts:
  - role: detect                     # detect | diagnose | remediate | validate
    system: "..."                    # System prompt
    user_template: "..."             # User prompt with {placeholders}

sources:
  - name: "my-postgres"
    type: postgres                   # postgres | mysql | snowflake
    connection_string: "postgresql://${PG_USER}:${PG_PASS}@localhost/db"
    schemas: [public, analytics]
    poll_interval_seconds: 3600

git:                                 # Optional — enables self-healing PRs
  repo_url: "https://github.com/your-org/migrations.git"
  branch: "taproot/auto-heal"
  base_branch: "main"
  commit_prefix: "[taproot-rca]"
  auto_pr: true

snapshot_dir: ".taproot/snapshots"
```

### Environment Variables

Connection strings and secrets support `${VAR_NAME}` interpolation so credentials never get committed. Taproot auto-loads variables from a `.env` file in your project root:

```
PG_USER=taproot
PG_PASS=taproot
NEON_CONN=postgresql://user:pass@host/db
GITHUB_TOKEN=ghp_your_token_here
```

### Prompt Placeholders

| Placeholder       | Description                          |
|--------------------|--------------------------------------|
| `{source_name}`   | Human-readable source identifier     |
| `{schema_before}` | Previous schema snapshot             |
| `{schema_after}`  | Current schema snapshot              |
| `{diff}`          | Computed diff between snapshots      |
| `{context}`       | Stage-specific context (DB type, prior stage output, etc.) |

---

## Local Development with Docker Postgres

A complete local dev environment is included:

```bash
docker compose up -d                                                          # spin up Postgres on port 5434
docker exec -i taproot-postgres psql -U taproot -d ecommerce < dev/seed.sql   # seed sample data
taproot scan -c taproot.dev.yaml                                              # baseline scan
docker exec -i taproot-postgres psql -U taproot -d ecommerce < dev/drift.sql  # introduce drift
taproot scan -c taproot.dev.yaml                                              # detect, analyze, document, push
```

---

## Self-Healing Workflow

When Git is configured in your `taproot.yaml` and `GITHUB_TOKEN` is set, Taproot-RCA will automatically:

1. Extract forward and rollback SQL from the LLM remediation output
2. Write proper migration files to `migrations/{source}/V{timestamp}__{source}_drift.sql`
3. Create a new branch from your base branch
4. Commit the migration files with a descriptive message
5. Push the branch to GitHub
6. Open a pull request via the GitHub API with the full drift analysis as the description

The PR includes a review checklist and the complete LLM analysis in a collapsible section, giving your team everything they need to evaluate and merge.

---

## Architecture

Taproot-RCA is designed to evolve from a local CLI into a CI/CD-integrated platform without changing the core code.

**Phase 1 — Local development:** A data engineer runs Taproot-RCA on their laptop, tunes the prompts, and configures sources. The same `taproot.yaml` they iterate on locally becomes the deployment artifact.

**Phase 2 — CI/CD integration:** A GitHub Actions workflow runs `taproot scan` on a schedule against production sources. When drift is detected, the same analysis pipeline runs, the same migration files get generated, and a PR is opened automatically. The only thing that changes between local and CI is where the tool runs and which credentials it uses.

The model layer is pluggable. Ollama is the local default, but the connection layer can be extended to point at any chat-compatible API endpoint, allowing the same prompts to run against cloud-hosted models in CI environments where local inference isn't practical.

---

## Roadmap

- [x] **Step 1** — Project scaffolding, config schema, CLI, and Ollama model management
- [x] **Step 2** — Ollama chat client, prompt engine, schema diff, and demo mode
- [x] **Step 3** — Live Postgres introspection, snapshot history, and AI documentation
- [x] **Step 4** — Full pipeline chain (detect → diagnose → remediate → validate)
- [x] **Step 5** — Self-healing Git workflow with auto-PR creation
- [ ] **Step 6** — Salesforce connector via Developer Edition org
- [ ] **Step 7** — Snowflake and S3 connectors
- [ ] **Step 8** — Cloud LLM provider abstraction (Anthropic, OpenAI)
- [ ] **Step 9** — Scheduled monitoring daemon and GitHub Actions integration
- [ ] **Step 10** — Web dashboard for multi-source observability

---

## Project Structure

```
taproot-rca/
├── pyproject.toml              # Package config and dependencies
├── docker-compose.yml          # Local Postgres dev environment
├── dev/
│   ├── seed.sql                # Baseline e-commerce schema
│   └── drift.sql               # Schema changes for testing drift detection
├── src/taproot_rca/
│   ├── cli.py                  # Typer CLI entry point
│   ├── config.py               # Pydantic config schema
│   ├── ollama_client.py        # Chat client for the LLM
│   ├── ollama_manager.py       # Model availability and pulling
│   ├── prompt_engine.py        # Template hydration
│   ├── pipeline.py             # Multi-stage analysis orchestrator
│   ├── schema_diff.py          # Snapshot comparison logic
│   ├── snapshot_store.py       # Local snapshot persistence
│   ├── docs_generator.py       # AI documentation and changelog
│   ├── sql_extractor.py        # Pulls migration SQL from LLM output
│   ├── env_resolver.py         # Environment variable interpolation
│   ├── connectors/
│   │   └── postgres.py         # Postgres schema introspection
│   └── git_ops/
│       └── healer.py           # Branch creation, commit, push, and PR
└── tests/
    ├── test_config.py
    ├── test_scan_pipeline.py
    ├── test_pipeline.py
    ├── test_docs.py
    └── test_self_heal.py
```

---

## License

MIT