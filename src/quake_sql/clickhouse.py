from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

import clickhouse_connect
import pandas as pd

from quake_sql.config import Settings
from quake_sql.schema import TABLE_NAME

_client_lock = threading.Lock()
_client_cache: dict[str, Any] = {}


DDL = """
CREATE TABLE IF NOT EXISTS earthquakes (
    event_id String,
    event_time DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    latitude Float64,
    longitude Float64,
    depth_km Float64,
    magnitude Nullable(Float64),
    magnitude_type LowCardinality(String),
    station_count Nullable(UInt32),
    azimuthal_gap Nullable(Float64),
    distance_to_station_deg Nullable(Float64),
    rms_residual Nullable(Float64),
    horizontal_error_km Nullable(Float64),
    depth_error_km Nullable(Float64),
    magnitude_error Nullable(Float64),
    magnitude_station_count Nullable(UInt32),
    event_type LowCardinality(String),
    status LowCardinality(String),
    source_net LowCardinality(String),
    place String,
    region LowCardinality(String),
    location_source LowCardinality(String),
    magnitude_source LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (event_time, event_id)
"""


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    row_count: int


def get_client(settings: Settings):
    """Return a shared ClickHouse client (thread-safe singleton per database)."""
    cache_key = f"{settings.clickhouse_host}:{settings.clickhouse_port}/{settings.clickhouse_database}"
    cached = _client_cache.get(cache_key)
    if cached is not None:
        return cached
    with _client_lock:
        cached = _client_cache.get(cache_key)
        if cached is not None:
            return cached
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            secure=settings.clickhouse_secure,
            connect_timeout=settings.clickhouse_timeout_seconds,
            send_receive_timeout=settings.clickhouse_timeout_seconds,
        )
        _client_cache[cache_key] = client
        return client


def get_bootstrap_client(settings: Settings):
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database="default",
        secure=settings.clickhouse_secure,
        connect_timeout=settings.clickhouse_timeout_seconds,
        send_receive_timeout=settings.clickhouse_timeout_seconds,
    )


def ensure_database(client, settings: Settings) -> None:
    client.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")
    client.command(f"USE {settings.clickhouse_database}")

def recreate_table(client) -> None:
    client.command(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    client.command(DDL)


def insert_dataframe(client, dataframe: pd.DataFrame) -> int:
    client.insert_df(TABLE_NAME, dataframe)
    return int(dataframe.shape[0])


def bootstrap_table(settings: Settings, dataframe: pd.DataFrame) -> int:
    boot_client = get_bootstrap_client(settings)
    ensure_database(boot_client, settings)
    client = get_client(settings)
    recreate_table(client)
    return insert_dataframe(client, dataframe)


def execute_query(client, sql: str, max_rows: int = 100) -> QueryResult:
    result = client.query(sql, settings={"max_result_rows": max_rows, "result_overflow_mode": "break"})
    return QueryResult(
        columns=list(result.column_names),
        rows=[list(row) for row in result.result_rows],
        row_count=result.row_count,
    )
