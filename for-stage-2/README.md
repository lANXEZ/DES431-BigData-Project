# Optional scaffolding for Stage 2 (AWS & Big Data)

> **This is not part of Workstream 1's deliverable.**
>
> Workstream 1 covers *Data Understanding* and *Data Preprocessing*. Stage 2 — S3,
> Glue, Spark, Parquet, Athena, pipeline — belongs to a different team member.
>
> The files here were written as a **starting point offered to the Stage 2 owner**,
> not as work claimed under Workstream 1. Use them, rewrite them, or ignore them
> entirely — the design decisions for the AWS pipeline are Stage 2's to make.

## What is here

| File | What it is | Status |
|---|---|---|
| `spark_preprocess.py` | PySpark translation of `scripts/02_preprocess.py`, shaped as a Glue job | **Never executed** — syntax-checked only |
| `athena_create_table.sql` | `CREATE EXTERNAL TABLE` DDL generated from the real Parquet schema | **Never run against Athena** |

Nothing in this repository has ever touched an AWS service. There is no `boto3`, no
SDK, no credentials, and no cloud call anywhere in the codebase. The only `s3://`
strings are placeholders. `spark_preprocess.py` has never been run, locally or on
Glue.

`scripts/02_preprocess_spark.py` is a separate PySpark implementation that **has**
been run locally and checked against the pandas output (see the main README). It
is the better starting point for a Glue job. This older file has known gaps: for
example, it clips with `greatest()`/`least()`, which turn a missing coordinate
into a distance of 0, and it uses Spark's HALF_UP `round()` where pandas rounds
half to even.

## Why it exists

The commonest failure mode when a pipeline is split across people is that the
cleaning logic gets written twice — once in pandas, once in Spark — and the two
quietly disagree. Nobody notices until the analysis numbers don't reconcile.

`spark_preprocess.py` is a line-for-line port of the pandas rules so that the two
implementations start from the same place. Two portability details are handled:

- Spark's `dayofweek` is 1 = Sunday; it is shifted to match the pandas 0 = Monday
  convention used throughout the curated dataset.
- The rule thresholds are **duplicated** at the top of the file, because a Glue job
  cannot import `scripts/common.py`. If one changes, **both must change.**

```bash
spark-submit for-stage-2/spark_preprocess.py \
    --input   s3://<bucket>/raw/divvy/ \
    --output  s3://<bucket>/curated/divvy_trips/ \
    --rejects s3://<bucket>/rejects/divvy/
```

```sql
-- after creating the table, load the Hive partitions:
MSCK REPAIR TABLE divvy.trips_clean;
```

## The one thing Stage 2 genuinely needs from Stage 1

Whether or not you use the scripts above, the **interface contract** in
[`DATA_UNDERSTANDING_AND_PREPROCESSING.md` §12](../DATA_UNDERSTANDING_AND_PREPROCESSING.md)
is the real handoff — the output schema and
[`outputs/stats/14_reconciliation_card.csv`](../outputs/stats/14_reconciliation_card.csv),
17 metrics that any reimplementation must reproduce exactly. If your Spark run
produces different numbers, the two implementations have diverged and the cause
should be found before analytics begins.

## Open decisions that belong to Stage 2

These were deliberately **not** decided by Workstream 1:

- S3 bucket layout, storage classes, and lifecycle rules
- Whether Glue crawlers or an explicit DDL define the catalog
- Athena workgroup, result location, and cost controls
- Whether to re-partition (the local output uses `year` / `month`, which suits a
  12-month window; a longer horizon may want `year` / `month` / `day`)
- Whether to run the cleaning in Glue at all, or simply ingest the Parquet that
  `scripts/02_preprocess.py` already produces

> ⚠️ This repository is public. Keep bucket names, account IDs and ARNs as
> placeholders and pass real values as job arguments — never hardcode them here.
