"""S3 Parquet loader using PyArrow."""

import io
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from config import get_settings

logger = structlog.get_logger(__name__)


class S3ParquetLoader:
    """Write DataFrames as Parquet to S3."""

    def __init__(self):
        settings = get_settings()
        self.s3 = boto3.client("s3")
        self.bucket = settings.s3.bucket

    def upload(self, records: list[dict], prefix: str, partition_cols: list[str] | None = None) -> str:
        """Upload records as Parquet to S3."""
        if not records:
            return ""

        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        now = datetime.now(timezone.utc)
        key = f"{prefix}/{now.strftime('%Y/%m/%d')}/data_{now.strftime('%H%M%S')}.parquet"

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
        uri = f"s3://{self.bucket}/{key}"
        logger.info("uploaded_parquet", uri=uri, rows=len(records))
        return uri
