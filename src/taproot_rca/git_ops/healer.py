"""
Git operations for Taproot-RCA self-healing.

Handles:
  - Cloning or using a local Git repo
  - Creating a new branch
  - Committing migration files
  - Pushing to the remote
  - Opening a PR via the GitHub API

Designed to work with both:
  - The same repo as Taproot-RCA itself (for testing)
  - A separate dedicated migrations repo (for production use)
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

from taproot_rca.config import GitTargetConfig


@dataclass
class GitPushResult:
    """Result of a successful Git push and PR creation."""
    branch_name: str
    commit_sha: str
    files_committed: list[str]
    pr_url: Optional[str] = None
    pr_number: Optional[int] = None


class GitHealer:
    """
    Manages the Git workflow for pushing AI-generated remediation
    migrations to a repository.
    """

    def __init__(self, config: GitTargetConfig, working_dir: Optional[str] = None):
        self.config = config
        # If no working dir is specified, use the current directory
        # (assumes Taproot-RCA is being run from inside the target repo)
        self.working_dir = Path(working_dir) if working_dir else Path.cwd()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push_migrations(
        self,
        migration_files: list[Path],
        source_name: str,
        diff_summary: str,
        analysis_report: Optional[str] = None,
    ) -> GitPushResult:
        """
        Commit migration files to a new branch and push to the remote.
        Optionally opens a PR via the GitHub API.

        Args:
            migration_files: Paths to the SQL files to commit (must be inside working_dir)
            source_name: Name of the data source that drifted
            diff_summary: Short summary of what changed
            analysis_report: Optional full markdown report for the PR body

        Returns:
            GitPushResult with branch name, commit SHA, and PR URL if created.
        """
        self._ensure_git_repo()

        # 1. Generate a unique branch name
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        safe_source = _safe_branch_name(source_name)
        branch_name = f"{self.config.branch}/{safe_source}-{timestamp}"

        # 2. Create branch
        self._run_git("checkout", "-b", branch_name)

        try:
            # 3. Stage the migration files
            file_paths_str = []
            for f in migration_files:
                # Convert to relative path from working_dir
                try:
                    rel = f.resolve().relative_to(self.working_dir.resolve())
                except ValueError:
                    # File is outside the working dir — copy it in
                    rel = Path("migrations") / f.name
                    target = self.working_dir / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy(f, target)
                file_paths_str.append(str(rel))
                self._run_git("add", str(rel))

            # 4. Commit
            commit_message = self._build_commit_message(source_name, diff_summary)
            self._run_git("commit", "-m", commit_message)
            commit_sha = self._run_git("rev-parse", "HEAD").strip()

            # 5. Push the branch
            self._run_git("push", "-u", "origin", branch_name)

            result = GitPushResult(
                branch_name=branch_name,
                commit_sha=commit_sha,
                files_committed=file_paths_str,
            )

            # 6. Open a PR if configured and possible
            if self.config.auto_pr:
                pr_info = self._open_github_pr(
                    branch_name=branch_name,
                    title=f"{self.config.commit_prefix} Schema drift in {source_name}",
                    body=self._build_pr_body(source_name, diff_summary, analysis_report),
                )
                if pr_info:
                    result.pr_url = pr_info["url"]
                    result.pr_number = pr_info["number"]

            return result

        finally:
            # Always switch back to the base branch when we're done
            try:
                self._run_git("checkout", self.config.base_branch)
            except subprocess.CalledProcessError:
                pass

    # ------------------------------------------------------------------
    # Git plumbing
    # ------------------------------------------------------------------

    def _ensure_git_repo(self) -> None:
        """Verify the working directory is a Git repository."""
        try:
            self._run_git("rev-parse", "--is-inside-work-tree")
        except subprocess.CalledProcessError:
            raise RuntimeError(
                f"Not a Git repository: {self.working_dir}\n"
                "  Run [bold]git init[/bold] in this directory or specify a different working dir."
            )

    def _run_git(self, *args: str) -> str:
        """Run a git command in the working directory and return stdout."""
        result = subprocess.run(
            ["git", *args],
            cwd=self.working_dir,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout

    def _build_commit_message(self, source_name: str, diff_summary: str) -> str:
        return (
            f"{self.config.commit_prefix} schema drift remediation: {source_name}\n"
            f"\n"
            f"{diff_summary}\n"
            f"\n"
            f"Auto-generated by Taproot-RCA. Review carefully before applying."
        )

    def _build_pr_body(
        self,
        source_name: str,
        diff_summary: str,
        analysis_report: Optional[str],
    ) -> str:
        body = (
            f"## Schema drift detected in `{source_name}`\n\n"
            f"This PR was opened automatically by **Taproot-RCA** after detecting "
            f"schema drift. The migration files in this branch contain forward and "
            f"rollback DDL generated by an AI analysis pipeline.\n\n"
            f"### Summary\n\n"
            f"{diff_summary}\n\n"
            f"### Review checklist\n\n"
            f"- [ ] Forward migration matches the intended change\n"
            f"- [ ] Rollback migration is complete and reversible\n"
            f"- [ ] Pre-migration safety checks have been run\n"
            f"- [ ] Post-migration validation queries are correct\n"
            f"- [ ] No data loss risk\n\n"
        )
        if analysis_report:
            body += f"### Full analysis\n\n<details>\n<summary>Click to expand</summary>\n\n{analysis_report}\n\n</details>\n"
        return body

    # ------------------------------------------------------------------
    # GitHub API for PR creation
    # ------------------------------------------------------------------

    def _open_github_pr(
        self,
        branch_name: str,
        title: str,
        body: str,
    ) -> Optional[dict]:
        """
        Open a pull request via the GitHub API.

        Requires GITHUB_TOKEN environment variable to be set.
        Returns dict with 'url' and 'number' on success, None on failure.
        """
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            return {
                "_warning": "GITHUB_TOKEN not set — branch was pushed but PR was not opened",
                "url": None,
                "number": None,
            }

        # Parse owner/repo from the repo URL
        repo_info = _parse_github_url(self.config.repo_url)
        if not repo_info:
            return {
                "_warning": f"Could not parse GitHub repo from {self.config.repo_url}",
                "url": None,
                "number": None,
            }

        owner, repo = repo_info
        api_url = f"https://api.github.com/repos/{owner}/{repo}/pulls"

        try:
            resp = httpx.post(
                api_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                json={
                    "title": title,
                    "body": body,
                    "head": branch_name,
                    "base": self.config.base_branch,
                },
                timeout=30.0,
            )

            if resp.status_code in (200, 201):
                data = resp.json()
                return {
                    "url": data.get("html_url"),
                    "number": data.get("number"),
                }
            else:
                return {
                    "_warning": f"GitHub API returned {resp.status_code}: {resp.text[:200]}",
                    "url": None,
                    "number": None,
                }
        except Exception as exc:
            return {
                "_warning": f"PR creation failed: {exc}",
                "url": None,
                "number": None,
            }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GITHUB_URL_PATTERNS = [
    re.compile(r"git@github\.com:([^/]+)/([^/.]+)"),
    re.compile(r"https?://github\.com/([^/]+)/([^/.]+)"),
]


def _parse_github_url(url: str) -> Optional[tuple[str, str]]:
    """Parse a GitHub URL into (owner, repo)."""
    for pattern in _GITHUB_URL_PATTERNS:
        match = pattern.search(url)
        if match:
            return match.group(1), match.group(2)
    return None


def _safe_branch_name(name: str) -> str:
    """Sanitize a string for use in a Git branch name."""
    return re.sub(r"[^a-zA-Z0-9_-]", "-", name)