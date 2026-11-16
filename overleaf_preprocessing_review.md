# Overleaf review: preprocessing vs. repo

**Scope:** `main.tex` in the DES431-Group6 Overleaf project, checked against this repo at commit `99ee5be`:
- Section III (Data Description)
- Section IV-A (Data Preprocessing Steps)
- The preprocessing figures quoted in the Abstract and Introduction

**Sources of truth in the repo:**
- `outputs/stats/*.csv`, `outputs/stats/00_summary.json`
- `outputs/handoff/station_dim.csv` and `outputs/handoff/station_hourly_flow.parquet`
- `scripts/02_preprocess.py` (and its PySpark twin `scripts/02_preprocess_spark.py`, which produces the same output)

**Result:** about 40 numbers and claims match the repo exactly. **4 problems** need action, listed below.

---

## Problems

### 1. The 17 station merges are not in the repo

- **Where:** Section IV-A, paragraph "*Station identity and flow*" (around lines 316-317 and 326-330 of `main.tex`)
- **Paper says:**
  - "after the 17 merges described below, 1,878 keys remain"
  - "17 key pairs were merged where a shared non-numeric ID, near-identical coordinates and consecutive months of activity agreed (for example, Streeter Dr/Grand Ave and Navy Pier, 86,750 + 51,393 = 138,143 endpoints)"
- **Repo does:**
  - No merge step exists. `scripts/02_preprocess.py` builds `station_key` from the station name only.
  - `outputs/handoff/station_dim.csv` has **1,895** keys, not 1,878.
  - `STREETER_DR_GRAND_AVE` (86,750 endpoints) and `NAVY_PIER` (51,393 endpoints) are still separate rows at the same coordinates.
  - In `station_hourly_flow`, Streeter Dr & Grand Ave drops to 0 departures from August 2025. The station was renamed "Navy Pier" on 2025-07-24 and kept the same ID, `CHI01747`.
- **Action:**
  - If the merge was done downstream (Athena / `features.py`), say so in the paper and point the reader there. The preprocessing code that the paper describes does not do it.
  - Otherwise, the merge step needs to be added to `02_preprocess.py` / `02_preprocess_spark.py`.
  - Either way, the list of the 17 pairs should be kept somewhere reproducible.

### 2. "1,627,147 valid trips that listwise deletion would discard" should be 1,656,158

- **Where:** Introduction, last paragraph, contribution (i)
- **Paper says:** "retaining 1,627,147 valid trips that listwise deletion would discard (removing 2.57% instead of 31.9%)"
- **Repo shows:**
  - Listwise deletion would drop the 1,769,497 trips that miss a station (31.94%). The **31.9% is correct**.
  - Of those trips, the pipeline keeps **1,656,158**: `outputs/stats/13_validation_checks.csv`, "dockless-endpoint trips kept".
  - The sum of `trips_dockless_start` + `trips_dockless_end` + `trips_fully_dockless` in `14_reconciliation_card.csv` gives the same figure.
  - Section IV-A of the paper already uses 1,656,158 correctly.
- **Cause:** 1,627,147 = 1,769,497 − 142,350. That subtracts *all* rejected trips, but most rejected trips have both stations, so they were never part of the listwise-deletion set.
- **Action:** replace 1,627,147 with **1,656,158**.

### 3. Station flow does not exclude trips with only one dockless end

- **Where:** Section IV-A, end of the "*Station identity and flow*" paragraph (around lines 336-339)
- **Paper says:** "Trips without a station at one or both ends (1,656,158, 30.69%) are excluded from station-level analysis"
- **Repo does:** `station_hourly_flow` drops only the trips with **no** station at either end (458,163 `fully_dockless` trips).
  - A trip that starts at a station and ends dockless still counts as a **departure**. A trip that starts dockless and ends at a station still counts as an **arrival**.
  - departures total **4,349,584** = 3,741,013 docked + 608,571 dockless_end
  - arrivals total **4,330,437** = 3,741,013 docked + 589,424 dockless_start
  - Code: `scripts/02_preprocess.py`, the `dep` / `arr` groupbys (around lines 189-194). They filter only on the station they count.
- **Action:** check whether the station features / K-Means input (Athena) filter to docked trips only.
  - **If they do:** the sentence is right for the modeling, but the paper should say the filter is applied there, not in preprocessing.
  - **If they don't:** reword it to: "Trips with no station at either end (458,163) are excluded from station-level flow; trips with one station count toward that station's departures or arrivals."

### 4. Arrivals are bucketed by the trip's start hour, not its arrival hour

- **Where:** Section IV-A, net-flow definition, Eq. (eq:netflow), around lines 330-336
- **Paper says:** "For station *s*, date *t* and hour *h*, with departures p_{s,t,h} and arrivals a_{s,t,h}". This reads as arrivals that happen in hour *h*.
- **Repo does:** arrivals are grouped by `trip_date` and `hour`, which are derived from **`started_at`**. A trip that leaves at 08:50 and docks at 09:10 counts as an arrival in the 08:00 hour.
- **Impact:** small, because the median trip is about 9.7 min, but it is systematic: arrivals shift slightly earlier.
- **Action:** either add a clause such as "arrivals are indexed by the trip's start hour", or the repo is changed to bucket arrivals by `ended_at` and the flow-based results are regenerated.

---

## Verified correct (no action needed)

| Paper claim | Repo source |
|---|---|
| 12 CSV, 5,539,521 rows, 13 columns, 1,119 MB | raw files total 1,119,105,245 bytes; `00_summary.json` |
| Missing start station 1,129,029 (20.38%), end 1,175,835 (21.23%), end coords 5,442 (0.10%), other 7 columns 0 | `02_missing_by_column.csv` |
| 0 duplicate `ride_id` | `00_summary.json` |
| 1,920 station names / 3,435 IDs; 1,589 names → several IDs; 105 IDs → several names; float-format IDs | `06_station_identity_integrity.csv`, `06b_*` (e.g. `20.0`) |
| 35 rows dated 30 Sep 2024, 31 survive | `10_trips_by_month_biketype.csv` (13+22); `outputs/clean/year=2024/month=9` has 31 rows |
| Jan 2025 138,689 vs Aug 2025 790,177 (5.7-fold) | `01_volume_by_month.csv` |
| 32.61% of e-bikes lack a start station, 0% classic; 5,552 classic lack an end station | `03_missing_by_biketype.csv` |
| 1,763,945 of 3,462,468 e-bike trips (50.9%) miss a station | `00_summary.json` (535,367 + 1,234,130 − 5,552) |
| Duration −56.0 to 1,574.9 min; 99.9th percentile 1,439.8 | `04_duration_stats.csv` |
| 24 rows outside the bounding box; box 41.60–42.15 / −88.00 to −87.45 | `00_summary.json`, `scripts/common.py` |
| Table II: all matched / removed counts, totals 165,478 / 142,350 | `12_quality_register.csv` |
| R3 = 0 ≤ duration < 60 s, R4 = duration > 24 h | `scripts/02_preprocess.py` lines 91-92 |
| R4: 5,325 of 5,533 already caught by R1 | 5,533 − 208; R2/R3 cannot overlap with > 24 h |
| R6: most of the 17,803 already caught by R3 | 17,784 are R3 rejects (rejects table) |
| 20 columns added → 33 columns; weekday rush hour 07–09 / 16–18 | `scripts/02_preprocess.py`, `scripts/common.py` |
| R = 6371.0088 km; v = d / (t/60) | `scripts/common.py` `haversine_km` |
| Geohash-7 cell ≈ 153 m × 114 m at Chicago | 360/2^18 and 180/2^17 degrees at 41.9° N |
| 1,895 keys, 1,859 with departures | `station_dim.csv` rows; `14_reconciliation_card.csv` |
| 13 validation checks, all pass; 17-metric reconciliation card | `13_validation_checks.csv`, `14_reconciliation_card.csv` |
| 5,397,171 clean, 142,350 removed (2.57%) | `11_cleaning_audit.csv`, `12_quality_register.csv` |
| 1,656,158 trips (30.69%) with a dockless end | `13_validation_checks.csv` |
| PySpark run reproduced pandas apart from last-digit latitude rounding | `scripts/02_preprocess_spark.py`, README "Preprocessing with PySpark" |
