from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from quake_sql.config import Settings


RAW_CSV_PATH = Path("data/raw_earthquakes_month.csv")
TRANSFORMED_CSV_PATH = Path("data/earthquakes_transformed.csv")


def derive_region(place: str | float | None) -> str:
    if not place or not isinstance(place, str):
        return "Unknown"
    if " of " in place:
        return place.split(" of ", maxsplit=1)[1].strip()
    if "," in place:
        return place.rsplit(",", maxsplit=1)[1].strip()
    compact = re.sub(r"\s+", " ", place).strip()
    return compact or "Unknown"


def fetch_raw_dataset(settings: Settings) -> pd.DataFrame:
    RAW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    dataframe = pd.read_csv(settings.usgs_feed_url)
    dataframe.to_csv(RAW_CSV_PATH, index=False)
    return dataframe


def transform_dataset(dataframe: pd.DataFrame) -> pd.DataFrame:
    transformed = dataframe.rename(
        columns={
            "id": "event_id",
            "time": "event_time",
            "updated": "updated_at",
            "depth": "depth_km",
            "mag": "magnitude",
            "magType": "magnitude_type",
            "type": "event_type",
            "net": "source_net",
            "nst": "station_count",
            "gap": "azimuthal_gap",
            "dmin": "distance_to_station_deg",
            "rms": "rms_residual",
            "horizontalError": "horizontal_error_km",
            "depthError": "depth_error_km",
            "magError": "magnitude_error",
            "magNst": "magnitude_station_count",
            "locationSource": "location_source",
            "magSource": "magnitude_source",
        }
    ).copy()

    transformed["event_time"] = pd.to_datetime(
        transformed["event_time"], utc=True, errors="coerce"
    )
    transformed["updated_at"] = pd.to_datetime(
        transformed["updated_at"], utc=True, errors="coerce"
    )
    for column in (
        "magnitude_type",
        "event_type",
        "status",
        "source_net",
        "place",
        "location_source",
        "magnitude_source",
    ):
        transformed[column] = transformed[column].fillna("unknown").astype(str)
    transformed["region"] = transformed["place"].map(derive_region)
    for column in (
        "latitude",
        "longitude",
        "depth_km",
        "magnitude",
        "azimuthal_gap",
        "distance_to_station_deg",
        "rms_residual",
        "horizontal_error_km",
        "depth_error_km",
        "magnitude_error",
    ):
        transformed[column] = pd.to_numeric(transformed[column], errors="coerce")
    for column in ("station_count", "magnitude_station_count"):
        transformed[column] = pd.to_numeric(transformed[column], errors="coerce").astype("Int64")

    selected = transformed[
        [
            "event_id",
            "event_time",
            "updated_at",
            "latitude",
            "longitude",
            "depth_km",
            "magnitude",
            "magnitude_type",
            "station_count",
            "azimuthal_gap",
            "distance_to_station_deg",
            "rms_residual",
            "horizontal_error_km",
            "depth_error_km",
            "magnitude_error",
            "magnitude_station_count",
            "event_type",
            "status",
            "source_net",
            "place",
            "region",
            "location_source",
            "magnitude_source",
        ]
    ].dropna(subset=["event_id", "event_time"])

    TRANSFORMED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(TRANSFORMED_CSV_PATH, index=False)
    return selected


def load_and_transform(settings: Settings) -> pd.DataFrame:
    return transform_dataset(fetch_raw_dataset(settings))
