"""
DES431 Big Data Project - Divvy Bike-Share
Shared configuration, data-quality rules, and feature helpers.

Owner: Task 1 (Data Understanding) + Task 2 (Data Preprocessing)

Every cleaning rule lives here as a named, documented constant so that the
PySpark port (spark_preprocess.py) and the pandas reference implementation
(02_preprocess.py) cannot drift apart.
"""
from __future__ import annotations
import os
import glob
import numpy as np
import pandas as pd

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR   = os.path.join(ROOT, "archive", "CSV_Files")
OUT       = os.path.join(ROOT, "outputs")
FIG_DIR   = os.path.join(OUT, "figures")
STAT_DIR  = os.path.join(OUT, "stats")
CLEAN_DIR = os.path.join(OUT, "clean")
REJ_DIR   = os.path.join(OUT, "rejects")
HAND_DIR  = os.path.join(OUT, "handoff")

# --------------------------------------------------------------------------
# Raw schema (13 columns, identical across all 12 monthly files)
# --------------------------------------------------------------------------
RAW_DTYPES = {
    "ride_id":            "string",
    "rideable_type":      "string",
    "start_station_name": "string",
    "start_station_id":   "string",
    "end_station_name":   "string",
    "end_station_id":     "string",
    "start_lat":          "float64",
    "start_lng":          "float64",
    "end_lat":            "float64",
    "end_lng":            "float64",
    "member_casual":      "string",
}
DATE_COLS   = ["started_at", "ended_at"]
RAW_COLUMNS = ["ride_id", "rideable_type", "started_at", "ended_at",
               "start_station_name", "start_station_id",
               "end_station_name", "end_station_id",
               "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"]

# --------------------------------------------------------------------------
# DATA QUALITY RULE THRESHOLDS  (single source of truth)
# --------------------------------------------------------------------------
MIN_DURATION_SEC = 60          # R3: below this = failed checkout, not a trip
MAX_DURATION_SEC = 24 * 3600   # R4: above this = system auto-close, not a trip
MAX_SPEED_KMH    = 60.0        # R6: physically implausible for a bicycle

# Chicago service-area bounding box (R5). Divvy operates in Chicago + near suburbs.
CHI_LAT_MIN, CHI_LAT_MAX = 41.60, 42.15
CHI_LNG_MIN, CHI_LNG_MAX = -88.00, -87.45

# Rule labels, applied in priority order when tagging a rejected row.
RULES = [
    ("R1_missing_end_coords",     "End coordinates absent - trip distance is not computable"),
    ("R2_negative_duration",      "ended_at earlier than started_at - impossible"),
    ("R3_duration_under_60s",     "Under 60 seconds - failed checkout / immediate re-dock"),
    ("R4_duration_over_24h",      "Over 24 hours - system auto-close, not a real ride"),
    ("R5_coords_outside_chicago", "Coordinates outside the Chicago service area - GPS error"),
    ("R6_implausible_speed",      "Average speed over 60 km/h - impossible on a bicycle"),
]

# NOTE: a missing STATION NAME is deliberately NOT a rejection rule.
# It is a structural property of dockless e-bikes (see the analysis write-up),
# not a defect. Those rows are kept and flagged via the `trip_type` column.

SEASONS = {12: "Winter", 1: "Winter", 2: "Winter",
           3: "Spring", 4: "Spring", 5: "Spring",
           6: "Summer", 7: "Summer", 8: "Summer",
           9: "Fall", 10: "Fall", 11: "Fall"}
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
RUSH_AM = (7, 9)     # inclusive
RUSH_PM = (16, 18)   # inclusive

# --------------------------------------------------------------------------
# Chart styling - light surface, recessive axes
# --------------------------------------------------------------------------
SURFACE  = "#fcfcfb"
INK      = "#0b0b0b"
INK_2    = "#52514e"
GRID     = "#e6e5e1"
SERIES   = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]   # blue, orange, aqua, yellow
SEQ_BLUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]

# --------------------------------------------------------------------------
# Geospatial helpers
# --------------------------------------------------------------------------
_B32 = b"0123456789bcdefghjkmnpqrstuvwxyz"


def geohash7(lat, lon, precision: int = 7) -> np.ndarray:
    """Vectorised geohash encoder.

    A geohash maps (lat, lon) onto a hierarchical grid and encodes the cell as a
    short base-32 string. Precision 7 gives roughly 153 m x 153 m cells - fine
    enough to group free-floating (dockless) e-bike trips into meaningful
    locations even though those rows carry no station name.

    Returns an array of fixed-width strings; non-finite inputs give "".
    """
    lat = np.asarray(lat, dtype="float64")
    lon = np.asarray(lon, dtype="float64")
    n = lat.shape[0]
    bad = ~(np.isfinite(lat) & np.isfinite(lon))
    lat = np.where(bad, 0.0, lat)
    lon = np.where(bad, 0.0, lon)

    nbits = precision * 5        # 35 bits at precision 7
    nlon  = (nbits + 1) // 2     # geohash alternates lon,lat starting with lon
    nlat  = nbits // 2

    lon_i = np.clip(((lon + 180.0) / 360.0 * (1 << nlon)).astype(np.int64),
                    0, (1 << nlon) - 1).astype(np.uint64)
    lat_i = np.clip(((lat + 90.0) / 180.0 * (1 << nlat)).astype(np.int64),
                    0, (1 << nlat) - 1).astype(np.uint64)

    code = np.zeros(n, dtype=np.uint64)
    one = np.uint64(1)
    for k in range(nbits):
        if k % 2 == 0:
            bit = (lon_i >> np.uint64(nlon - 1 - (k // 2))) & one
        else:
            bit = (lat_i >> np.uint64(nlat - 1 - (k // 2))) & one
        code = (code << one) | bit

    lut   = np.frombuffer(_B32, dtype="S1")
    chars = np.empty((n, precision), dtype="S1")
    for j in range(precision):
        shift = np.uint64(nbits - 5 * (j + 1))
        chars[:, j] = lut[((code >> shift) & np.uint64(31)).astype(np.int64)]

    out = np.ascontiguousarray(chars).view("S%d" % precision).ravel().astype("U%d" % precision)
    out[bad] = ""
    return out


def haversine_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    """Great-circle distance in km between two coordinate arrays.

    This is straight-line ("as the crow flies") distance, NOT the distance the
    rider actually cycled - the real path is always longer. That makes derived
    speed a conservative LOWER bound, which is exactly what we want when using
    speed to detect impossible rides.
    """
    R = 6371.0088
    p1 = np.radians(np.asarray(lat1, dtype="float64"))
    p2 = np.radians(np.asarray(lat2, dtype="float64"))
    dphi = p2 - p1
    dlmb = np.radians(np.asarray(lon2, dtype="float64") - np.asarray(lon1, dtype="float64"))
    a = np.sin(dphi / 2.0) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlmb / 2.0) ** 2
    return 2.0 * R * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def station_key(name: pd.Series) -> pd.Series:
    """Surrogate key for a station, derived from its NAME - never its ID.

    Divvy re-numbered its stations partway through the study window (numeric
    IDs such as '13084' were replaced by 'CHI00307') and, worse, recycled old
    IDs for unrelated stations. Joining on station_id therefore splits one
    station into several and merges unrelated ones. The cleaned name is the
    only stable identifier in this dataset.
    """
    return (name.str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.upper()
                .str.replace(r"[^A-Z0-9]+", "_", regex=True)
                .str.strip("_"))


def clean_name(name: pd.Series) -> pd.Series:
    """Human-readable station name with whitespace defects removed."""
    return name.str.strip().str.replace(r"\s+", " ", regex=True)


def monthly_files() -> list:
    return sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))


def read_raw(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=RAW_DTYPES, parse_dates=DATE_COLS)


def ensure_dirs() -> None:
    for d in (FIG_DIR, STAT_DIR, CLEAN_DIR, REJ_DIR, HAND_DIR):
        os.makedirs(d, exist_ok=True)
