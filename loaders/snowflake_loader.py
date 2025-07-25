"""Snowflake loader for GitHub data."""

import json
from datetime import datetime, timezone

import snowflake.connector
import structlog

from config import get_settings

logger = structlog.get_logger(__name__)


class SnowflakeLoader:
    def __init__(self):
        s = get_settings()
        self.conn_params = {
            "account": s.snowflake.account, "user": s.snowflake.user,
            "password": s.snowflake.password, "warehouse": s.snowflake.warehouse,
            "database": s.snowflake.database, "role": s.snowflake.role,
        }

    def load_records(self, records: list[dict], schema: str, table: str) -> int:
        if not records:
            return 0
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    raw_data VARIANT,
                    loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            batch_id = f"{table}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            for r in records:
                cur.execute(
                    f"INSERT INTO {schema}.{table} (raw_data) SELECT PARSE_JSON(%s)",
                    (json.dumps(r, default=str),)
                )
            conn.commit()
            logger.info("snowflake_loaded", schema=schema, table=table, rows=len(records))
            return len(records)
        finally:
            conn.close()

    def load_repos(self, records): return self.load_records(records, "RAW_GITHUB", "REPOSITORIES")
    def load_commits(self, records): return self.load_records(records, "RAW_GITHUB", "COMMITS")
    def load_issues(self, records): return self.load_records(records, "RAW_GITHUB", "ISSUES")
