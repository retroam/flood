from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Quake SQL"
    app_env: Literal["development", "production"] = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    max_result_rows: int = 100
    max_generated_limit: int = 500

    openai_api_key: str | None = None
    openai_base_url: str | None = None
    openai_model: str = "gpt-5.4-mini"
    openai_reasoning_effort: Literal["minimal", "low", "medium", "high", "xhigh"] = (
        "low"
    )
    openai_timeout_seconds: float = 60.0
    openai_input_cost_per_1m: float = 0.25
    openai_cached_input_cost_per_1m: float = 0.025
    openai_output_cost_per_1m: float = 2.0

    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "quake_sql"
    clickhouse_secure: bool = False
    clickhouse_timeout_seconds: float = 30.0

    usgs_feed_url: str = (
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
    )

    @computed_field
    @property
    def clickhouse_http_url(self) -> str:
        scheme = "https" if self.clickhouse_secure else "http"
        return f"{scheme}://{self.clickhouse_host}:{self.clickhouse_port}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
