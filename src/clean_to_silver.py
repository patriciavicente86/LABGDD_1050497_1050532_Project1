# ETL script to read Bronze parquet files, clean & validate NYC taxi trips,
# and write out Silver parquet files partitioned by pickup_date.
import argparse
import yaml
from functools import reduce
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, Window


def get_spark(app="NYC Taxi - Clean to Silver"):
    # Create or get a Spark session with a sensible shuffle partition default.
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def parse_args():
    # Command-line parsing: expect --config pointing to a YAML config file.
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()


def read_cfg(path):
    # Read YAML config file and return a dict.
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run(cfg):
    # Main logic: read bronze data, clean it, deduplicate and write silver data.
    spark = get_spark()

    # Root directories for Bronze and Silver layers (can be absolute or relative).
    bronze_root = Path(cfg["bronze_path"])   # e.g. ./lake/bronze
    silver_root = Path(cfg["silver_path"])   # e.g. ./lake/silver
    years = cfg.get("years", [])             # list of years to load
    services = cfg.get("services", [])       # taxi services like 'yellow', 'green'
    dq = cfg.get("dq", {})                   # data quality thresholds

    # Data quality thresholds with defaults
    max_trip_hours = float(dq.get("max_trip_hours", 6))     # max trip duration in hours
    min_distance_km = float(dq.get("min_distance_km", 0.1)) # minimum trip distance in km
    min_total_amount = float(dq.get("min_total_amount", 0.0))

    # NYC trip_distance is recorded in miles; convert km threshold -> miles
    min_distance_miles = min_distance_km * 0.621371

    # Process each service independently (results written to silver/<service>/...)
    for svc in services:
        # ---- Read per (service, year) with a common basePath, then union ----
        dfs = []
        for y in years:
            # Expect layout: <bronze_root>/service=<svc>/year=<YYYY>/month=*
            path = bronze_root / f"service={svc}" / f"year={y}"
            if path.exists():
                # Use basePath so partition columns are inferred consistently
                df_part = (
                    spark.read.format("parquet")
                    .option("basePath", str(bronze_root))
                    .load(str(path))  # includes month=* subdirs
                )
                # If the partition column 'service' is missing in file metadata, add it
                if "service" not in df_part.columns:
                    df_part = df_part.withColumn("service", F.lit(svc))
                dfs.append(df_part)

        # If no data found for this service/year range, raise a helpful error
        if not dfs:
            raise FileNotFoundError(
                f"No Bronze inputs under {bronze_root} for service='{svc}', years={years}. "
                f"Expected like {bronze_root}/service={svc}/year=<YYYY>/month=*"
            )

        # Union all yearly parts together, tolerating missing columns across files
        df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

        # ---- Standardize datetime column names across taxi variants ----
        # Yellow taxis use tpep_*, green use lpep_*; normalize to pickup/dropoff names
        if "tpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        if "tpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        if "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        if "lpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

        # ---- Compute duration_min robustly (seconds -> minutes) ----
        # Prefer precomputed timestamp columns if present (pickup_ts, dropoff_ts),
        # otherwise fallback to parsed datetime columns.
        if "pickup_ts" in df.columns and "dropoff_ts" in df.columns:
            # timestampdiff returns seconds; divide by 60 to get minutes
            df = df.withColumn(
                "duration_min",
                F.expr("timestampdiff(SECOND, pickup_ts, dropoff_ts)") / F.lit(60.0),
            )
        else:
            df = df.withColumn(
                "duration_min",
                F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)") / F.lit(60.0),
            )

        # ---- Core cleaning & validation rules ----
        # - Fill missing passenger_count with 1
        # - Cast numeric columns to appropriate types
        # - Filter out unrealistic distances, amounts, durations, and passenger counts
        df = (
            df.withColumn(
                "passenger_count",
                F.when(F.col("passenger_count").isNull(), F.lit(1))
                 .otherwise(F.col("passenger_count"))
                 .cast("int"),
            )
            .withColumn("trip_distance", F.col("trip_distance").cast("double"))
            .withColumn("total_amount", F.col("total_amount").cast("double"))
            # trip distance must be above min threshold and below a sane upper bound
            .filter((F.col("trip_distance") > min_distance_miles) & (F.col("trip_distance") < 200))
            # enforce minimum charge threshold
            .filter(F.col("total_amount") > min_total_amount)
            # duration must be positive and below max_trip_hours
            .filter((F.col("duration_min") > 0) & (F.col("duration_min") < max_trip_hours * 60.0))
            # passenger count sanity
            .filter((F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6))
            # derive a date column for partitioning
            .withColumn("pickup_date", F.to_date("pickup_datetime"))
            # enforce service column value for consistency across files
            .withColumn("service", F.lit(svc))
        )

        # ---- De-duplication by composite key (be tolerant to column case) ----
        # Some datasets use different capitalization for keys; create lowercase aliases.
        key_candidates = {
            "vendorid": ["vendorid", "VendorID"],
            "pulocationid": ["pulocationid", "PULocationID"],
            "dolocationid": ["dolocationid", "DOLocationID"],
        }
        # Create lowercase aliases if the canonical lowercase name is missing
        for alias, options in key_candidates.items():
            if alias not in df.columns:
                for opt in options:
                    if opt in df.columns:
                        df = df.withColumn(alias, F.col(opt))
                        break

        # Ensure the deduplication key columns exist; if missing create null-typed placeholders.
        required_key_cols = ["vendorid", "pickup_datetime", "dropoff_datetime", "pulocationid", "dolocationid", "total_amount"]
        for c in required_key_cols:
            if c not in df.columns:
                # Use string cast for missing keys so comparisons succeed in Window partition
                df = df.withColumn(c, F.lit(None).cast("string"))

        # Define a Window to assign row numbers within each duplicate group and keep first row
        w = Window.partitionBy("vendorid", "pickup_datetime", "dropoff_datetime",
                               "pulocationid", "dolocationid", "total_amount") \
                  .orderBy(F.col("pickup_datetime").asc())
        df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

        # ---- Write Silver (partitioned by pickup_date) ----
        # Repartition to a fixed number of files per partition to control small files,
        # then overwrite the output directory for this service.
        out = silver_root / svc
        (
            df.repartition(64, "pickup_date")
            .write.mode("overwrite")
            .partitionBy("pickup_date")
            .format("parquet")
            .save(str(out))
        )

    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    run(cfg)
