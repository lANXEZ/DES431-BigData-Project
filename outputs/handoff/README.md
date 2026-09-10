# Handoff aggregates

Derived tables produced by `scripts/02_preprocess.py` alongside the curated fact
table. They are byproducts of preprocessing, published here so downstream stages do
not have to re-run the pipeline to get them.

| File | Rows | Owner of the work it feeds |
|---|---:|---|
| `station_dim.parquet` / `.csv` | 1,895 | Workstream 1 — resolves the corrupt `station_id` problem |
| `station_hourly_flow.parquet` | 2,353,242 | **Prepared for Stage 3 (Analytics)** |

## `station_dim`

The station dimension is genuinely part of preprocessing. Divvy re-numbered its
stations mid-period and recycled old IDs, so `station_id` fragments 82.8% of
stations and merges 105 unrelated pairs (see §5.2 and §9.1 of the write-up). This
table collapses 3,435 unreliable IDs onto 1,859 stable, name-derived keys.

| Column | Meaning |
|---|---|
| `station_key` | surrogate key, e.g. `CLARK_ST_ARMITAGE_AVE` — **join on this** |
| `station_name` | display name, whitespace-normalised |
| `lat`, `lng` | trip-weighted median coordinate |
| `n_trip_endpoints` | times the station appears as a start or end |
| `n_legacy_ids`, `legacy_station_ids` | the original IDs merged into this key |

## `station_hourly_flow`

> **Note on scope.** Station-imbalance analysis belongs to **Stage 3**, not to
> Workstream 1. This table is a convenience aggregate, not an analysis — it counts
> departures and arrivals and does nothing else. How imbalance is defined,
> thresholded, or acted on is Stage 3's call.

`station_key` × `trip_date` × `hour`, restricted to trips with a docked endpoint:

| Column | Meaning |
|---|---|
| `departures` | trips starting at this station in this hour |
| `arrivals` | trips ending at this station in this hour |
| `net_flow` | `arrivals − departures`; positive means bikes accumulate here |

Dockless trips are excluded by construction, since they have no station. Stage 3 can
reach the other 30.69% of trips through `start_geohash7` / `end_geohash7` on the
curated fact table.
