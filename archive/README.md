# Raw data — not stored in this repository

The 12 monthly Divvy trip CSVs total **1,119 MB**, and nine of them individually
exceed GitHub's 100 MB per-file hard limit. They are therefore gitignored. Nothing
is lost: the entire curated dataset is reproducible from them with one command.

## How to get the data

Download from the official source — **<https://divvybikes.com/system-data>** — and
unzip into `archive/CSV_Files/` so the tree looks like this:

```
archive/CSV_Files/
├── 202410-divvy-tripdata.csv     126.7 MB
├── 202411-divvy-tripdata.csv      69.1 MB
├── 202412-divvy-tripdata.csv      36.8 MB
├── 202501-divvy-tripdata.csv      28.6 MB
├── 202502-divvy-tripdata.csv      31.3 MB
├── 202503-divvy-tripdata.csv      60.9 MB
├── 202504-divvy-tripdata.csv      75.6 MB
├── 202505-divvy-tripdata.csv     101.6 MB
├── 202506-divvy-tripdata.csv     136.0 MB
├── 202507-divvy-tripdata.csv     152.3 MB
├── 202508-divvy-tripdata.csv     157.5 MB
└── 202509-divvy-tripdata.csv     142.7 MB
```

The same 12 files are also mirrored on Kaggle if the official S3 bucket is slow.

## Then rebuild everything

```bash
pip install -r ../requirements.txt
python ../scripts/02_preprocess.py    # → outputs/clean/ and outputs/rejects/
python ../scripts/03_validate.py      # confirms 13/13 checks pass
```

Run from the repository root, not from this directory.

## Expected schema

All 12 files share an identical 13-column header — there is no schema drift:

```
ride_id, rideable_type, started_at, ended_at,
start_station_name, start_station_id, end_station_name, end_station_id,
start_lat, start_lng, end_lat, end_lng, member_casual
```

`scripts/01_profile.py` will fail loudly if a file deviates from this.

## Licence

Published by Lyft Bikes and Scooters, LLC under the
[Divvy Data License Agreement](https://divvybikes.com/data-license-agreement).
Trip records are anonymised and contain no personally identifying information.
