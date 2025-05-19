from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache

class GitHubSettings(BaseSettings):
    token: str = Field(..., alias="GITHUB_TOKEN")
    model_config = {"env_file": ".env", "extra": "ignore"}

class S3Settings(BaseSettings):
    bucket: str = Field("github-intelligence-data", alias="S3_BUCKET")
    model_config = {"env_file": ".env", "extra": "ignore"}

class SnowflakeSettings(BaseSettings):
    account: str = Field(..., alias="SNOWFLAKE_ACCOUNT")
    user: str = Field(..., alias="SNOWFLAKE_USER")
    password: str = Field(..., alias="SNOWFLAKE_PASSWORD")
    warehouse: str = Field("GITHUB_WH", alias="SNOWFLAKE_WAREHOUSE")
    database: str = Field("GITHUB_DB", alias="SNOWFLAKE_DATABASE")
    role: str = Field("GITHUB_ROLE", alias="SNOWFLAKE_ROLE")
    model_config = {"env_file": ".env", "extra": "ignore"}

class Settings(BaseSettings):
    github: GitHubSettings = GitHubSettings()
    s3: S3Settings = S3Settings()
    snowflake: SnowflakeSettings = SnowflakeSettings()
    model_config = {"env_file": ".env", "extra": "ignore"}

@lru_cache()
def get_settings() -> Settings:
    return Settings()
