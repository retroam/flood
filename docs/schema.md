# Earthquake Table Schema

Dataset source: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv`

Transformations:
- Renamed the USGS CSV columns into snake_case ClickHouse fields.
- Parsed `time` and `updated` into UTC `DateTime64(3)` timestamps.
- Derived `region` from the trailing location phrase in `place`.
- Kept the data at event grain: one row per USGS event.

| Column | Type | Description |
| --- | --- | --- |
| `event_id` | `String` | USGS event identifier. |
| `event_time` | `DateTime64(3, 'UTC')` | When the earthquake occurred. |
| `updated_at` | `DateTime64(3, 'UTC')` | Most recent USGS update time. |
| `latitude` | `Float64` | Latitude in decimal degrees. |
| `longitude` | `Float64` | Longitude in decimal degrees. |
| `depth_km` | `Float64` | Depth in kilometers. |
| `magnitude` | `Nullable(Float64)` | Reported earthquake magnitude. |
| `magnitude_type` | `LowCardinality(String)` | Magnitude type such as `ml` or `mb`. |
| `station_count` | `Nullable(UInt32)` | Number of seismic stations used. |
| `azimuthal_gap` | `Nullable(Float64)` | Azimuthal gap in degrees. |
| `distance_to_station_deg` | `Nullable(Float64)` | Distance to the nearest station in degrees. |
| `rms_residual` | `Nullable(Float64)` | Root mean square travel-time residual. |
| `horizontal_error_km` | `Nullable(Float64)` | Horizontal uncertainty in kilometers. |
| `depth_error_km` | `Nullable(Float64)` | Depth uncertainty in kilometers. |
| `magnitude_error` | `Nullable(Float64)` | Magnitude uncertainty. |
| `magnitude_station_count` | `Nullable(UInt32)` | Station count for the magnitude estimate. |
| `event_type` | `LowCardinality(String)` | USGS event type such as `earthquake`. |
| `status` | `LowCardinality(String)` | USGS processing status. |
| `source_net` | `LowCardinality(String)` | Source seismic network. |
| `place` | `String` | Full human-readable location string. |
| `region` | `LowCardinality(String)` | Derived place bucket used for grouping and filtering. |
| `location_source` | `LowCardinality(String)` | Source for the location solution. |
| `magnitude_source` | `LowCardinality(String)` | Source for the magnitude solution. |
