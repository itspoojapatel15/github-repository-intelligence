"""Historical backfill pipeline with checkpoint/resume support."""

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import structlog

from .github_rest_client import GitHubRESTClient

logger = structlog.get_logger(__name__)
CHECKPOINT_FILE = ".backfill_checkpoint.json"


class BackfillPipeline:
    """Backfill 2+ years of historical data with checkpoint support."""

    def __init__(self):
        self.client = GitHubRESTClient()
        self.checkpoint = self._load_checkpoint()

    def _load_checkpoint(self) -> dict:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE) as f:
                return json.load(f)
        return {"completed_repos": [], "last_run": None}

    def _save_checkpoint(self) -> None:
        self.checkpoint["last_run"] = datetime.now(timezone.utc).isoformat()
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(self.checkpoint, f, indent=2)

    async def backfill_repo(
        self, owner: str, repo: str, years: int = 2
    ) -> dict[str, list[dict]]:
        """Backfill commits and issues for a repo going back N years."""
        repo_key = f"{owner}/{repo}"
        if repo_key in self.checkpoint.get("completed_repos", []):
            logger.info("repo_already_backfilled", repo=repo_key)
            return {"commits": [], "issues": []}

        since = (datetime.now(timezone.utc) - timedelta(days=365 * years)).isoformat()
        logger.info("starting_backfill", repo=repo_key, since=since)

        commits = await self.client.get_commits(owner, repo, since=since, max_pages=20)
        issues = await self.client.get_issues(owner, repo, state="all", max_pages=20)

        self.checkpoint.setdefault("completed_repos", []).append(repo_key)
        self._save_checkpoint()

        logger.info("backfill_complete", repo=repo_key, commits=len(commits), issues=len(issues))
        return {"commits": commits, "issues": issues}

    async def backfill_repos(
        self, repos: list[dict], years: int = 2
    ) -> dict[str, dict]:
        """Backfill multiple repos."""
        results = {}
        for repo in repos:
            owner = repo.get("owner") or repo["full_name"].split("/")[0]
            name = repo.get("name") or repo["full_name"].split("/")[1]
            results[f"{owner}/{name}"] = await self.backfill_repo(owner, name, years)
        return results
