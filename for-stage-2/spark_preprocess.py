"""
PYSPARK PORT OF THE PREPROCESSING PIPELINE  -  for the AWS / Glue owner
======================================================================
This is a line-for-line translation of 02_preprocess.py into Spark SQL so the
same cleaning rules can run at scale in AWS Glue. It produces a byte-compatible
schema and must reproduce outputs/stats/14_reconciliation_card.csv exactly.

Run as a Glue 4.0 / 5.0 job (Spark 3.3+), or locally:
    spark-submit scripts/spark_preprocess.py \
        --input  s3://<bucket>/raw/divvy/ \
        --output s3://<bucket>/curated/divvy_trips/

WHY EACH RULE EXISTS is documented in 02_preprocess.py and the write-up; the
thresholds below are duplicated from common.py deliberately, because a Glue job
cannot import from the local repo. If you change one, change it in BOTH files.
"""
import argparse

from pyspark.sql import SparkSession, functions as F, types as T

# ---- thresholds: keep in sync with scripts/common.py ---------------------
MIN_DURATION_SEC = 60
MAX_DURATION_SEC = 24 * 3600
MAX_SPEED_KMH    = 60.0
CHI_LAT_MIN, CHI_LAT_MAX = 41.60, 42.15
CHI_LNG_MIN, CHI_LNG_MAX = -88.00, -87.45

RAW_SCHEMA = T.StructType([
    T.StructField("ride_id",            T.StringType()),
    T.StructField("rideable_type",      T.StringType()),
    T.StructField("started_at",         T.TimestampType()),
    T.StructField("ended_at",           T.TimestampType()),
    T.StructField("start_station_name", T.StringType()),
    T.StructField("start_station_id",   T.StringType()),
    T.StructField("end_station_name",   T.StringType()),
    T.StructField("end_station_id",     T.StringType()),
    T.StructField("start_lat",          T.DoubleType()),
    T.StructField("start_lng",          T.DoubleType()),
    T.StructField("end_lat",            T.DoubleType()),
    T.StructField("end_lng",            T.DoubleType()),
    T.StructField("member_casual",      T.StringType()),
])


def clean_name(col):
    """Trim and collapse internal whitespace (13 station names are affected)."""
    return F.regexp_replace(F.trim(col), r"\s+", " ")


def station_key(col):
    """Name-derived surrogate key. Divvy re-numbered and RECYCLED station_id
    values mid-period, so station_id cannot be used as a join key."""
    return F.regexp_replace(
        F.regexp_replace(F.upper(clean_name(col)), r"[^A-Z0-9]+", "_"),
        r"^_+|_+$", "")


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km. Straight-line, so derived speed is a
    conservative lower bound - good for outlier detection."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = p2 - p1
    dlmb = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dphi / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(dlmb / 2), 2)
    return F.lit(2 * 6371.0088) * F.asin(F.sqrt(F.least(F.greatest(a, F.lit(0.0)), F.lit(1.0))))


def geohash7(lat, lon):
    """Base-32 geohash at precision 7 (~153 m cells), built with Spark SQL only.

    Mirrors common.geohash7: 35 bits, alternating lon/lat starting with lon,
    then 7 base-32 characters. Every trip gets a spatial bin, including the 20%
    of dockless trips that carry no station name.
    """
    nbits, nlon, nlat = 35, 18, 17
    lon_i = F.floor((lon + F.lit(180.0)) / F.lit(360.0) * F.lit(1 << nlon)).cast("long")
    lat_i = F.floor((lat + F.lit(90.0)) / F.lit(180.0) * F.lit(1 << nlat)).cast("long")
    lon_i = F.least(F.greatest(lon_i, F.lit(0)), F.lit((1 << nlon) - 1))
    lat_i = F.least(F.greatest(lat_i, F.lit(0)), F.lit((1 << nlat) - 1))

    code = F.lit(0).cast("long")
    for k in range(nbits):
        if k % 2 == 0:
            bit = F.shiftright(lon_i, nlon - 1 - (k // 2)).bitwiseAND(F.lit(1))
        else:
            bit = F.shiftright(lat_i, nlat - 1 - (k // 2)).bitwiseAND(F.lit(1))
        code = F.shiftleft(code, 1).bitwiseOR(bit)

    b32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    parts = [F.element_at(F.split(F.lit(b32), ""),
                          (F.shiftright(code, nbits - 5 * (j + 1)).bitwiseAND(F.lit(31)) + 1).cast("int"))
             for j in range(7)]
    return F.when(lat.isNull() | lon.isNull(), F.lit(None).cast("string")).otherwise(F.concat(*parts))


def build(df):
    # ---- 1. structural cleaning -----------------------------------------
    df = df.dropDuplicates(["ride_id"])
    df = (df.withColumn("start_station_name", clean_name(F.col("start_station_name")))
            .withColumn("end_station_name",   clean_name(F.col("end_station_name"))))

    # ---- 2. derived quantities ------------------------------------------
    dur = F.col("ended_at").cast("double") - F.col("started_at").cast("double")
    df = (df.withColumn("_dur_sec", dur)
            .withColumn("haversine_km", haversine_km(F.col("start_lat"), F.col("start_lng"),
                                                     F.col("end_lat"), F.col("end_lng")))
            .withColumn("avg_speed_kmh",
                        F.when(F.col("_dur_sec") > 0,
                               F.col("haversine_km") / (F.col("_dur_sec") / 3600.0))))

    # ---- 3. quality rules, tagged in priority order ---------------------
    oob = (~F.col("start_lat").between(CHI_LAT_MIN, CHI_LAT_MAX)
           | ~F.col("start_lng").between(CHI_LNG_MIN, CHI_LNG_MAX)
           | (F.col("end_lat").isNotNull() & ~F.col("end_lat").between(CHI_LAT_MIN, CHI_LAT_MAX))
           | (F.col("end_lng").isNotNull() & ~F.col("end_lng").between(CHI_LNG_MIN, CHI_LNG_MAX)))
    df = df.withColumn("reject_reason", F.coalesce(
        F.when(F.col("end_lat").isNull() | F.col("end_lng").isNull(), F.lit("R1_missing_end_coords")),
        F.when(F.col("_dur_sec") < 0,                                 F.lit("R2_negative_duration")),
        F.when(F.col("_dur_sec") < MIN_DURATION_SEC,                  F.lit("R3_duration_under_60s")),
        F.when(F.col("_dur_sec") > MAX_DURATION_SEC,                  F.lit("R4_duration_over_24h")),
        F.when(oob,                                                   F.lit("R5_coords_outside_chicago")),
        F.when(F.col("avg_speed_kmh") > MAX_SPEED_KMH,                F.lit("R6_implausible_speed")),
    ))
    rejects = df.filter(F.col("reject_reason").isNotNull())
    df = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    # ---- 4. feature engineering -----------------------------------------
    st, sm, em = F.col("started_at"), F.col("start_station_name").isNull(), F.col("end_station_name").isNull()
    df = (df
        .withColumn("duration_min",  F.round(F.col("_dur_sec") / 60.0, 4).cast("float"))
        .withColumn("haversine_km",  F.round("haversine_km", 4).cast("float"))
        .withColumn("avg_speed_kmh", F.round("avg_speed_kmh", 3).cast("float"))
        .withColumn("start_station_key", station_key(F.col("start_station_name")))
        .withColumn("end_station_key",   station_key(F.col("end_station_name")))
        .withColumn("start_geohash7", geohash7(F.col("start_lat"), F.col("start_lng")))
        .withColumn("end_geohash7",   geohash7(F.col("end_lat"),   F.col("end_lng")))
        .withColumn("trip_type",
                    F.when(~sm & ~em, "docked")
                     .when(sm & ~em,  "dockless_start")
                     .when(~sm & em,  "dockless_end")
                     .otherwise("fully_dockless"))
        .withColumn("is_round_trip",
                    F.when(~sm & ~em, F.col("start_station_key") == F.col("end_station_key")))
        .withColumn("trip_date",   F.date_format(st, "yyyy-MM-dd"))
        .withColumn("year",        F.year(st).cast("short"))
        .withColumn("month",       F.month(st).cast("byte"))
        .withColumn("day",         F.dayofmonth(st).cast("byte"))
        .withColumn("hour",        F.hour(st).cast("byte"))
        # Spark dayofweek is 1=Sunday; shift to 0=Monday to match pandas.
        .withColumn("day_of_week", ((F.dayofweek(st) + 5) % 7).cast("byte"))
        .withColumn("day_name",    F.date_format(st, "EEEE"))
        .withColumn("is_weekend",  F.col("day_of_week") >= 5)
        .withColumn("season", F.when(F.col("month").isin(12, 1, 2), "Winter")
                               .when(F.col("month").isin(3, 4, 5),  "Spring")
                               .when(F.col("month").isin(6, 7, 8),  "Summer")
                               .otherwise("Fall"))
        .withColumn("time_of_day", F.when(F.col("hour") <= 5,  "Night")
                                    .when(F.col("hour") <= 11, "Morning")
                                    .when(F.col("hour") <= 17, "Afternoon")
                                    .otherwise("Evening"))
        .withColumn("is_rush_hour", ~F.col("is_weekend")
                    & (F.col("hour").between(7, 9) | F.col("hour").between(16, 18)))
        .drop("_dur_sec"))

    return df.select(
        "ride_id", "rideable_type", "member_casual", "started_at", "ended_at",
        "start_station_name", "start_station_id", "start_station_key",
        "end_station_name", "end_station_id", "end_station_key",
        "start_lat", "start_lng", "end_lat", "end_lng",
        "start_geohash7", "end_geohash7",
        "duration_min", "haversine_km", "avg_speed_kmh",
        "trip_type", "is_round_trip",
        "trip_date", "year", "month", "day", "hour",
        "day_of_week", "day_name", "is_weekend", "season", "time_of_day", "is_rush_hour",
    ), rejects


def reconcile(df):
    """Print the same metrics as outputs/stats/14_reconciliation_card.csv."""
    df.cache()
    tt = {r["trip_type"]: r["count"] for r in df.groupBy("trip_type").count().collect()}
    row = df.select(
        F.count("*").alias("clean_row_count"),
        F.countDistinct("ride_id").alias("distinct_ride_id"),
        F.round(F.mean("duration_min"), 4).alias("mean_duration_min"),
        F.round(F.mean("haversine_km"), 4).alias("mean_haversine_km"),
        F.round(F.mean("avg_speed_kmh"), 4).alias("mean_avg_speed_kmh"),
        F.round(100 * F.avg((F.col("member_casual") == "member").cast("int")), 4).alias("pct_member"),
        F.round(100 * F.avg((F.col("rideable_type") == "electric_bike").cast("int")), 4).alias("pct_electric_bike"),
        F.round(100 * F.avg(F.col("is_weekend").cast("int")), 4).alias("pct_weekend"),
        F.round(100 * F.avg(F.col("is_rush_hour").cast("int")), 4).alias("pct_rush_hour"),
        F.countDistinct("start_geohash7").alias("distinct_start_geohash7"),
        F.countDistinct("start_station_key").alias("distinct_start_station_key"),
    ).collect()[0].asDict()
    row.update({"trips_" + k: v for k, v in tt.items()})
    print("\nRECONCILIATION (must match 14_reconciliation_card.csv)")
    for k in sorted(row):
        print("  %-30s %s" % (k, row[k]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  required=True, help="S3 or local path holding the raw monthly CSVs")
    ap.add_argument("--output", required=True, help="destination for the curated Parquet dataset")
    ap.add_argument("--rejects", default=None, help="optional destination for the reject audit trail")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("divvy-preprocess")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())

    raw = (spark.read.option("header", True).option("mode", "PERMISSIVE")
                .schema(RAW_SCHEMA).csv(a.input))
    clean, rejects = build(raw)

    (clean.repartition("year", "month")
          .write.mode("overwrite").partitionBy("year", "month").parquet(a.output))
    if a.rejects:
        rejects.write.mode("overwrite").parquet(a.rejects)

    reconcile(spark.read.parquet(a.output))
    spark.stop()


if __name__ == "__main__":
    main()
