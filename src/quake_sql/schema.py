from __future__ import annotations

from dataclasses import dataclass


TABLE_NAME = "earthquakes"

NUMERIC_COLUMNS = (
    "magnitude",
    "depth_km",
    "latitude",
    "longitude",
    "station_count",
    "azimuthal_gap",
    "distance_to_station_deg",
    "rms_residual",
    "horizontal_error_km",
    "depth_error_km",
    "magnitude_error",
    "magnitude_station_count",
)

STRING_COLUMNS = (
    "event_id",
    "magnitude_type",
    "event_type",
    "status",
    "source_net",
    "place",
    "region",
    "location_source",
    "magnitude_source",
)

TIME_COLUMNS = ("event_time", "updated_at")

ALL_COLUMNS = TIME_COLUMNS + NUMERIC_COLUMNS + STRING_COLUMNS

GROUPABLE_COLUMNS = (
    "region",
    "event_type",
    "status",
    "source_net",
    "magnitude_type",
    "location_source",
    "magnitude_source",
)


@dataclass(frozen=True)
class ColumnDoc:
    name: str
    type_name: str
    description: str


COLUMN_DOCS = (
    ColumnDoc("event_id", "String", "USGS event identifier."),
    ColumnDoc("event_time", "DateTime64(3, 'UTC')", "When the earthquake occurred."),
    ColumnDoc("updated_at", "DateTime64(3, 'UTC')", "Most recent USGS update time."),
    ColumnDoc("latitude", "Float64", "Latitude in decimal degrees."),
    ColumnDoc("longitude", "Float64", "Longitude in decimal degrees."),
    ColumnDoc("depth_km", "Float64", "Depth in kilometers."),
    ColumnDoc("magnitude", "Nullable(Float64)", "Reported earthquake magnitude."),
    ColumnDoc("magnitude_type", "LowCardinality(String)", "Magnitude type such as `ml` or `mb`."),
    ColumnDoc("station_count", "Nullable(UInt32)", "Number of seismic stations used."),
    ColumnDoc("azimuthal_gap", "Nullable(Float64)", "Azimuthal gap in degrees."),
    ColumnDoc("distance_to_station_deg", "Nullable(Float64)", "Distance to nearest station in degrees."),
    ColumnDoc("rms_residual", "Nullable(Float64)", "Root mean square travel-time residual."),
    ColumnDoc("horizontal_error_km", "Nullable(Float64)", "Horizontal location uncertainty in kilometers."),
    ColumnDoc("depth_error_km", "Nullable(Float64)", "Depth uncertainty in kilometers."),
    ColumnDoc("magnitude_error", "Nullable(Float64)", "Magnitude uncertainty."),
    ColumnDoc("magnitude_station_count", "Nullable(UInt32)", "Number of stations used for the magnitude estimate."),
    ColumnDoc("event_type", "LowCardinality(String)", "USGS event type such as `earthquake`."),
    ColumnDoc("status", "LowCardinality(String)", "USGS processing status."),
    ColumnDoc("source_net", "LowCardinality(String)", "Source seismic network."),
    ColumnDoc("place", "String", "Full human-readable location string."),
    ColumnDoc("region", "LowCardinality(String)", "Derived place bucket, usually the area after `of` or the trailing region."),
    ColumnDoc("location_source", "LowCardinality(String)", "Source for the location solution."),
    ColumnDoc("magnitude_source", "LowCardinality(String)", "Source for the magnitude solution."),
)


TRANSFORMATION_NOTES = (
    "Downloaded from the USGS `all_month.csv` feed at bootstrap time.",
    "Renamed API columns to ClickHouse-friendly snake_case names.",
    "Derived `region` from the `place` text to support grouping and filtering.",
    "Parsed `time` and `updated` into UTC `DateTime64(3)` values.",
)


def schema_markdown() -> str:
    rows = ["| Column | Type | Description |", "| --- | --- | --- |"]
    rows.extend(
        f"| `{column.name}` | `{column.type_name}` | {column.description} |"
        for column in COLUMN_DOCS
    )
    return "\n".join(rows)


def schema_html() -> str:
    rows = []
    for col in COLUMN_DOCS:
        rows.append(
            f"<tr><td><code>{col.name}</code></td>"
            f"<td><code>{col.type_name}</code></td>"
            f"<td>{col.description}</td></tr>"
        )
    return (
        '<table class="result-table schema-rendered-table">'
        "<thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def schema_prompt() -> str:
    lines = [
        f"Table: {TABLE_NAME}",
        "Allowed columns:",
    ]
    for column in COLUMN_DOCS:
        lines.append(f"- {column.name} ({column.type_name}): {column.description}")
    lines.extend(
        [
            "",
            "SQL rules:",
            f"- Only query `{TABLE_NAME}`.",
            "- Generate exactly one ClickHouse `SELECT` statement or the literal `UNSUPPORTED`.",
            "- Never use columns or tables outside the allowed schema.",
            "- Use ClickHouse functions such as `count`, `avg`, `sum`, `min`, `max`, `toDate`, `toStartOfHour`, `toStartOfDay`, `toStartOfWeek`, and `toStartOfMonth` when helpful.",
            "- Use `event_time` for time filtering unless the request explicitly asks about updates.",
            "- For recent windows, prefer `event_time >= now() - INTERVAL <n> <UNIT>`.",
            "- For `yesterday`, prefer `toDate(event_time) = today() - 1`. Do not append a DAY unit.",
            "- This table is already the earthquake dataset. Do not add `event_type = 'earthquake'` for generic earthquake phrasing.",
            "- Only filter on `event_type` when the user explicitly asks about event types or names a non-default event type.",
            "- For geography text such as `in Alaska`, prefer `region ILIKE '%Alaska%'` over exact equality unless the user explicitly asks for an exact region value.",
            "- Only use `LIMIT` when the user explicitly asks for a bounded list, top-N ranking, or raw row lookup.",
            "- When `LIMIT` is needed, use plain `LIMIT <n>` only. Never use `OFFSET` or `LIMIT <offset>, <count>`.",
            "- For raw row lookups, keep `LIMIT` no larger than 100.",
            "- For grouped aggregate summaries that are not time series, order by the computed aggregate rather than alphabetically unless the user requests a different sort.",
            "- Avoid unnecessary aliases, filters, and clauses.",
            "- If the request is unsafe, ambiguous, requests schema that does not exist, or is not answerable from this dataset, output `UNSUPPORTED`.",
        ]
    )
    return "\n".join(lines)
