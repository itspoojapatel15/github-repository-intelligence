"""GitHub GraphQL API client for batch data retrieval."""

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import get_settings

logger = structlog.get_logger(__name__)
GRAPHQL_URL = "https://api.github.com/graphql"

REPO_QUERY = """
query($owner: String!, $name: String!, $commitCursor: String, $issueCursor: String) {
  repository(owner: $owner, name: $name) {
    nameWithOwner
    stargazerCount
    forkCount
    primaryLanguage { name }
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: 100, after: $commitCursor) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              oid
              message
              committedDate
              author { name email }
              additions
              deletions
            }
          }
        }
      }
    }
    issues(first: 100, after: $issueCursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
      totalCount
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        state
        createdAt
        updatedAt
        closedAt
        author { login }
        labels(first: 5) { nodes { name } }
      }
    }
  }
}
"""


class GitHubGraphQLClient:
    """GraphQL client for efficient batch data retrieval."""

    def __init__(self):
        settings = get_settings()
        self._token = settings.github.token
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=30))
    async def _query(self, client: httpx.AsyncClient, query: str, variables: dict) -> dict:
        """Execute GraphQL query."""
        resp = await client.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=self._headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            logger.error("graphql_errors", errors=data["errors"][:3])
            raise RuntimeError(f"GraphQL errors: {data['errors'][0]['message']}")

        return data["data"]

    async def get_repo_data(self, owner: str, name: str) -> dict[str, Any]:
        """Get comprehensive repo data via GraphQL."""
        all_commits = []
        all_issues = []
        commit_cursor = None
        issue_cursor = None

        async with httpx.AsyncClient() as client:
            for _ in range(5):  # Max 5 pages
                data = await self._query(client, REPO_QUERY, {
                    "owner": owner, "name": name,
                    "commitCursor": commit_cursor, "issueCursor": issue_cursor,
                })

                repo = data["repository"]

                # Process commits
                history = repo["defaultBranchRef"]["target"]["history"]
                for node in history["nodes"]:
                    all_commits.append({
                        "sha": node["oid"],
                        "message": node["message"][:500],
                        "committed_date": node["committedDate"],
                        "author_name": node["author"]["name"] if node["author"] else None,
                        "author_email": node["author"]["email"] if node["author"] else None,
                        "additions": node.get("additions", 0),
                        "deletions": node.get("deletions", 0),
                    })

                if history["pageInfo"]["hasNextPage"]:
                    commit_cursor = history["pageInfo"]["endCursor"]
                else:
                    commit_cursor = None

                # Process issues
                issues = repo["issues"]
                for node in issues["nodes"]:
                    all_issues.append({
                        "number": node["number"],
                        "title": node["title"][:500],
                        "state": node["state"],
                        "created_at": node["createdAt"],
                        "updated_at": node["updatedAt"],
                        "closed_at": node.get("closedAt"),
                        "author": node["author"]["login"] if node.get("author") else None,
                        "labels": [l["name"] for l in node.get("labels", {}).get("nodes", [])],
                    })

                if issues["pageInfo"]["hasNextPage"]:
                    issue_cursor = issues["pageInfo"]["endCursor"]
                else:
                    issue_cursor = None

                if not commit_cursor and not issue_cursor:
                    break

                await asyncio.sleep(0.5)

        return {
            "repo": f"{owner}/{name}",
            "stars": repo["stargazerCount"],
            "forks": repo["forkCount"],
            "language": repo["primaryLanguage"]["name"] if repo.get("primaryLanguage") else None,
            "commits": all_commits,
            "issues": all_issues,
            "extracted_at": self.extracted_at,
        }
