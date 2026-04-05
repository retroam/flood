from __future__ import annotations

from quake_sql.clickhouse import bootstrap_table
from quake_sql.config import get_settings
from quake_sql.data import load_and_transform
from quake_sql.schema import TABLE_NAME


def main() -> None:
    settings = get_settings()
    dataframe = load_and_transform(settings)
    inserted_rows = bootstrap_table(settings, dataframe)
    print(
        f"Loaded {inserted_rows} rows into {settings.clickhouse_database}.{TABLE_NAME} from {settings.usgs_feed_url}"
    )


if __name__ == "__main__":
    main()
