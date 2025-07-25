"""GitHub REST API client with rate limiting and exponential backoff."""

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import get_settings

logger = structlog.get_logger(__name__)
API_BASE = "https://api.github.com"


class GitHubRESTClient:
    """Async GitHub REST API client with rate-limit awareness."""

    def __init__(self):
        settings = get_settings()
        self._token = settings.github.token
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=1, max=60),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectTimeout)),
    )
    async def _request(self, client: httpx.AsyncClient, endpoint: str, params: dict | None = None) -> httpx.Response:
        """Make rate-limit-aware request."""
        resp = await client.get(f"{API_BASE}{endpoint}", params=params, headers=self._headers, timeout=30)

        remaining = int(resp.headers.get("X-RateLimit-Remaining", 100))
        if remaining < 10:
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))
            wait_time = max(reset_ts - int(datetime.now(timezone.utc).timestamp()), 1)
            logger.warning("rate_limit_low", remaining=remaining, wait=wait_time)
            await asyncio.sleep(min(wait_time, 60))

        if resp.status_code == 403 and remaining == 0:
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))
            wait_time = max(reset_ts - int(datetime.now(timezone.utc).timestamp()), 5)
            logger.warning("rate_limited", wait=wait_time)
            await asyncio.sleep(wait_time)
            raise httpx.HTTPStatusError("Rate limited", request=resp.request, response=resp)

        resp.raise_for_status()
        return resp

    async def get_repos(self, query: str, sort: str = "stars", per_page: int = 100, pages: int = 10) -> list[dict]:
        """Search repositories with pagination."""
        repos = []
        async with httpx.AsyncClient() as client:
            for page in range(1, pages + 1):
                resp = await self._request(client, "/search/repositories", {
                    "q": query, "sort": sort, "per_page": per_page, "page": page
                })
                items = resp.json().get("items", [])
                if not items:
                    break
                for r in items:
                    repos.append({
                        "repo_id": r["id"], "full_name": r["full_name"], "name": r["name"],
                        "owner": r["owner"]["login"], "description": (r.get("description") or "")[:500],
                        "language": r.get("language"), "stars": r["stargazers_count"],
                        "forks": r["forks_count"], "watchers": r["watchers_count"],
                        "open_issues": r["open_issues_count"], "size_kb": r["size"],
                        "created_at": r["created_at"], "updated_at": r["updated_at"],
                        "pushed_at": r.get("pushed_at"), "default_branch": r["default_branch"],
                        "license": r.get("license", {}).get("spdx_id") if r.get("license") else None,
                        "topics": r.get("topics", []), "is_fork": r["fork"],
                        "archived": r["archived"], "extracted_at": self.extracted_at,
                    })
                await asyncio.sleep(0.5)
        logger.info("repos_extracted", count=len(repos))
        return repos

    async def get_commits(self, owner: str, repo: str, since: str | None = None, per_page: int = 100, max_pages: int = 5) -> list[dict]:
        """Get commits for a repository with pagination."""
        commits = []
        async with httpx.AsyncClient() as client:
            for page in range(1, max_pages + 1):
                params: dict[str, Any] = {"per_page": per_page, "page": page}
                if since:
                    params["since"] = since
                resp = await self._request(client, f"/repos/{owner}/{repo}/commits", params)
                items = resp.json()
                if not items:
                    break
                for c in items:
                    commit = c.get("commit", {})
                    author = commit.get("author", {})
                    commits.append({
                        "sha": c["sha"], "repo_full_name": f"{owner}/{repo}",
                        "message": commit.get("message", "")[:500],
                        "author_name": author.get("name"), "author_email": author.get("email"),
                        "author_date": author.get("date"),
                        "additions": c.get("stats", {}).get("additions", 0),
                        "deletions": c.get("stats", {}).get("deletions", 0),
                        "extracted_at": self.extracted_at,
                    })
                await asyncio.sleep(0.3)
        return commits

    async def get_issues(self, owner: str, repo: str, state: str = "all", per_page: int = 100, max_pages: int = 5) -> list[dict]:
        """Get issues for a repository."""
        issues = []
        async with httpx.AsyncClient() as client:
            for page in range(1, max_pages + 1):
                resp = await self._request(client, f"/repos/{owner}/{repo}/issues", {
                    "state": state, "per_page": per_page, "page": page, "sort": "updated"
                })
                items = resp.json()
                if not items:
                    break
                for i in items:
                    if i.get("pull_request"):
                        continue  # Skip PRs
                    issues.append({
                        "issue_id": i["id"], "number": i["number"],
                        "repo_full_name": f"{owner}/{repo}",
                        "title": i["title"][:500], "state": i["state"],
                        "author": i["user"]["login"] if i.get("user") else None,
                        "labels": [l["name"] for l in i.get("labels", [])],
                        "comments": i["comments"],
                        "created_at": i["created_at"], "updated_at": i["updated_at"],
                        "closed_at": i.get("closed_at"),
                        "extracted_at": self.extracted_at,
                    })
                await asyncio.sleep(0.3)
        return issues
