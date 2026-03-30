"""Tests for configuration loading and validation."""

import pytest
import yaml
from pathlib import Path
from taproot_rca.config import load_config, TaprootConfig


MINIMAL_VALID = {
    "version": "1",
    "model": {
        "name": "llama3:8b",
    },
    "prompts": [
        {
            "role": "detect",
            "system": "You are a schema analyst.",
            "user_template": "Diff: {diff}",
        }
    ],
    "sources": [
        {
            "name": "test-pg",
            "type": "postgres",
            "connection_string": "postgresql://user:pass@localhost/db",
        }
    ],
}


def _write_yaml(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "taproot.yaml"
    p.write_text(yaml.dump(data))
    return p


class TestConfigLoading:
    def test_minimal_valid_config(self, tmp_path):
        path = _write_yaml(tmp_path, MINIMAL_VALID)
        cfg = load_config(path)
        assert cfg.model.name == "llama3:8b"
        assert cfg.model.host == "http://localhost:11434"
        assert len(cfg.prompts) == 1
        assert len(cfg.sources) == 1

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yaml")

    def test_empty_file_raises(self, tmp_path):
        p = tmp_path / "taproot.yaml"
        p.write_text("")
        with pytest.raises(ValueError, match="empty"):
            load_config(p)

    def test_missing_model_raises(self, tmp_path):
        bad = {**MINIMAL_VALID}
        del bad["model"]
        path = _write_yaml(tmp_path, bad)
        with pytest.raises(Exception):
            load_config(path)

    def test_missing_sources_raises(self, tmp_path):
        bad = {**MINIMAL_VALID}
        del bad["sources"]
        path = _write_yaml(tmp_path, bad)
        with pytest.raises(Exception):
            load_config(path)

    def test_duplicate_prompt_roles_rejected(self, tmp_path):
        bad = {**MINIMAL_VALID}
        bad["prompts"] = [
            {"role": "detect", "system": "a", "user_template": "b"},
            {"role": "detect", "system": "c", "user_template": "d"},
        ]
        path = _write_yaml(tmp_path, bad)
        with pytest.raises(Exception, match="once"):
            load_config(path)

    def test_invalid_source_type_rejected(self, tmp_path):
        bad = {**MINIMAL_VALID}
        bad["sources"] = [
            {"name": "x", "type": "oracle", "connection_string": "oracle://..."}
        ]
        path = _write_yaml(tmp_path, bad)
        with pytest.raises(Exception):
            load_config(path)

    def test_full_config_with_git(self, tmp_path):
        full = {
            **MINIMAL_VALID,
            "git": {
                "repo_url": "git@github.com:org/repo.git",
                "branch": "taproot/fix",
                "base_branch": "main",
                "commit_prefix": "[taproot]",
                "auto_pr": True,
            },
        }
        path = _write_yaml(tmp_path, full)
        cfg = load_config(path)
        assert cfg.git is not None
        assert cfg.git.auto_pr is True

    def test_defaults_applied(self, tmp_path):
        path = _write_yaml(tmp_path, MINIMAL_VALID)
        cfg = load_config(path)
        assert cfg.model.temperature == 0.1
        assert cfg.model.context_length == 4096
        assert cfg.sources[0].schemas == ["public"]
        assert cfg.sources[0].poll_interval_seconds == 3600
        assert cfg.snapshot_dir == ".taproot/snapshots"
