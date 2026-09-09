# DES431 — Divvy Bike-Share Big Data Pipeline

**Data Understanding & Preprocessing workstream** · Big Data Analytics and Machine
Learning · Sirindhorn International Institute of Technology

A reproducible pipeline that turns 12 months of raw Chicago bike-share trip records
into a validated, partitioned, feature-engineered Parquet dataset ready for
Spark/Athena analytics on AWS.

| | |
|---|---|
| **Input** | 5,539,521 trips · 13 columns · 1,119 MB CSV |
| **Output** | 5,397,171 trips · 33 columns · 362 MB Snappy Parquet, Hive-partitioned |
| **Rows removed** | 142,350 (2.57%) — each attributable to one named rule |
| **Validation** | 13/13 automated checks pass |
| **Runtime** | ≈11 min on a laptop, streaming one month at a time |

📄 **[Full methodology and analysis →](DATA_UNDERSTANDING_AND_PREPROCESSING.md)**

---

## The headline finding

20.4% of trips have no start-station name. The reflex is to drop or impute them.
Both are wrong — splitting the missingness by bike type explains **100%** of it:

| Bike type | Trips | Missing start station | Rate |
|---|---:|---:|---:|
| `classic_bike` | 2,077,053 | 0 | **0.00%** |
| `electric_bike` | 3,462,468 | 1,129,029 | **32.61%** |

![Missingness by bike type](outputs/figures/F2_missing_by_biketype.png)

Divvy e-bikes are **dockless** — a rider can lock one to any public rack, so no
station gets recorded. Classic bikes must be returned to a dock, so they *never*
have a missing station. The nulls encode a real operational mode, which makes this
**MNAR** (Missing Not At Random) rather than damage.

Treating it correctly instead of calling `dropna()` is worth **1,627,147 trips**:

| Approach | Rows kept | Lost | Population studied |
|---|---:|---:|---|
| `dropna()` on all columns | 3,770,024 | 31.9% | Docked trips only — biased |
| **Rule-based (this pipeline)** | **5,397,171** | **2.57%** | Full system |

Two further data-quality findings — a corrupt `station_id` column that fragments
82.8% of stations if you join on it, and a 24-hour auto-close artefact masquerading
as day-long rides — are documented in
[the write-up](DATA_UNDERSTANDING_AND_PREPROCESSING.md).

---

## Quickstart

```bash
pip install -r requirements.txt

# 1. download the raw CSVs into archive/CSV_Files/  (see archive/README.md)

python scripts/01_profile.py          # profiling  → outputs/stats, figures F1–F7
python scripts/02_preprocess.py       # clean+engineer → outputs/clean (Parquet)
python scripts/03_validate.py         # 13 checks + reconciliation card, F8–F10
python scripts/04_data_dictionary.py  # schemas + Athena DDL
```

Every script streams **one month at a time** and never holds all 5.5 M rows in
memory, so the pipeline runs comfortably in 8 GB of RAM.

## Repository layout

```
├── DATA_UNDERSTANDING_AND_PREPROCESSING.md   full methodology + analysis
├── requirements.txt
├── archive/CSV_Files/          raw CSVs — NOT in git, see archive/README.md
├── scripts/
│   ├── common.py               shared config, thresholds, geohash, haversine
│   ├── 01_profile.py           profiling only, mutates nothing
│   ├── 02_preprocess.py        cleaning, transformation, feature engineering
│   ├── 03_validate.py          assertions + reconciliation card
│   ├── 04_data_dictionary.py   schema docs + Athena DDL generator
│   └── spark_preprocess.py     PySpark port for AWS Glue
└── outputs/
    ├── stats/                  19 profiling tables (tracked)
    ├── figures/                10 report figures (tracked)
    ├── handoff/                station dimension, hourly flows, Athena DDL (tracked)
    ├── clean/                  curated Parquet — gitignored, regenerable
    └── rejects/                dropped rows + reason — gitignored, regenerable
```

## Pipeline

```mermaid
flowchart LR
    A["12 raw CSVs<br/>1,119 MB<br/>5.54 M rows"] --> B["01_profile.py<br/>Data Understanding"]
    A --> C["02_preprocess.py<br/>Clean · Transform · Engineer"]
    B --> S["outputs/stats/<br/>19 profiling tables"]
    B --> F["outputs/figures/<br/>10 report figures"]
    C --> D["outputs/clean/<br/>Parquet, year/month<br/>5.40 M rows · 33 cols"]
    C --> R["outputs/rejects/<br/>142,350 rows<br/>+ reject_reason"]
    C --> H["outputs/handoff/<br/>station_dim · flows · DDL"]
    D --> V["03_validate.py<br/>13 checks + reconciliation card"]
    D --> N["→ Stage 2: S3 / Glue / Athena"]
    H --> N
    D --> M["→ Stage 3: Analytics"]

    classDef input   fill:#e8eef7,stroke:#2a78d6,stroke-width:1.5px,color:#0b0b0b
    classDef proc    fill:#ffffff,stroke:#52514e,stroke-width:1.5px,color:#0b0b0b
    classDef output  fill:#f0efec,stroke:#8a8880,stroke-width:1.5px,color:#0b0b0b
    classDef curated fill:#e0f2ea,stroke:#1baf7a,stroke-width:1.5px,color:#0b0b0b
    classDef next    fill:#fdf0e8,stroke:#eb6834,stroke-width:1.5px,color:#0b0b0b

    class A input
    class B,C,V proc
    class S,F,R,H output
    class D curated
    class N,M next
```

## Cleaning rules

Six rules. Each dropped row is tagged with **exactly one** reason and written to
`outputs/rejects/`, so any figure in the report traces back to individual records.

| Rule | Condition | Rationale | Removed |
|---|---|---|---:|
| R1 | `end_lat`/`end_lng` NULL | distance not computable | 5,442 |
| R2 | `ended_at < started_at` | physically impossible | 43 |
| R3 | duration < 60 s | failed checkout / immediate re-dock | 136,390 |
| R4 | duration > 24 h | system auto-close, not a ride | 208 |
| R5 | coords outside Chicago bbox | GPS error | 24 |
| R6 | avg speed > 60 km/h | impossible on a bicycle | 243 |

A missing **station name** is deliberately *not* a rule — see the headline finding.

## Feature engineering

20 engineered columns on top of the 13 source columns. Two are load-bearing:

- **`station_key`** — a surrogate key derived from the normalised station *name*.
  Divvy re-numbered stations mid-period (`13084` → `CHI00307`) *and* recycled old
  IDs for unrelated stations, so `GROUP BY station_id` fragments 82.8% of stations
  and merges 105 unrelated pairs. This collapses 3,435 unreliable IDs onto 1,859
  stable keys.
- **`geohash7`** — a ~153 m spatial cell computed from coordinates, which are
  present on *every* row. Without it, 30.69% of the cleaned dataset would be
  invisible to spatial analysis. Geohashes are hierarchical, so truncating the
  string zooms out with no re-processing.

## AWS handoff

`scripts/spark_preprocess.py` is a line-for-line PySpark port, including a pure
Spark SQL geohash encoder, runnable as a Glue job:

```bash
spark-submit scripts/spark_preprocess.py \
    --input   s3://<bucket>/raw/divvy/ \
    --output  s3://<bucket>/curated/divvy_trips/ \
    --rejects s3://<bucket>/rejects/divvy/
```

`outputs/handoff/athena_create_table.sql` is generated from the real Parquet schema,
so the table definition cannot drift from the data.

`outputs/stats/14_reconciliation_card.csv` is the acceptance test — 17 metrics the
Glue run must reproduce exactly. If any differs, the pandas and Spark
implementations have diverged and the cause must be found before analysis starts.

> ⚠️ The Spark port is syntax-checked but has **not** been executed (no local Spark
> install). Validate it on a single month against the reconciliation card before
> running it on all twelve.

## Data source

Divvy trip data is published monthly by Lyft Bikes and Scooters, LLC under a
[public licence](https://divvybikes.com/data-license-agreement). It contains no
personally identifying information. This repository stores no raw data — see
[`archive/README.md`](archive/README.md) to fetch it.

## Tooling

Python 3.14.5 · pandas 3.0.3 · numpy 2.4.6 · pyarrow 25.0.1 · matplotlib 3.11.0

No step samples and no step depends on random state, so the pipeline is
deterministic: re-running it reproduces the reconciliation card exactly.
