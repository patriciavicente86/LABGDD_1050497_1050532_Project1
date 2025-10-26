# Read "silver" parquet feature datasets (per service), compute derived features,
# and write "gold" tables: trip-level features, zone-hour aggregates, OD-hour aggregates.

import argparse
import yaml
from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession, functions as F


def get_spark(app="NYC Taxi - Features to Gold"):
    # Create (or get) a SparkSession configured for this job.
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def parse_args():
    # Parse CLI args. Expect --config pointing to a YAML config file.
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()


def read_cfg(path):
    # Read YAML config from path and return as dict.
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run(cfg):
    # Main ETL function. Expects cfg keys: silver_path, gold_path, services (list).
    spark = get_spark()

    # Root directories for silver (input) and gold (output)
    silver_root = Path(cfg["silver_path"])   # e.g., ./lake/silver
    gold_root   = Path(cfg["gold_path"])     # e.g., ./lake/gold
    services    = cfg.get("services", [])    # list of service subfolders to read

    # --- Read per-service parquet inputs using a common basePath, then union them ---
    # We set basePath to silver_root so partition discovery (pickup_date=*) works when loading each service.
    dfs = []
    for svc in services:
        svc_path = silver_root / svc  # e.g., lake/silver/yellow
        if svc_path.exists():
            df = (
                spark.read.format("parquet")
                .option("basePath", str(silver_root))  # basePath allows consistent partition columns
                .load(str(svc_path))
            )
            # If the dataset doesn't include a 'service' column, inject it so downstream grouping includes service.
            if "service" not in df.columns:
                df = df.withColumn("service", F.lit(svc))
            dfs.append(df)

    # If no service folders were found, fail fast with a clear message.
    if not dfs:
        raise FileNotFoundError(f"No Silver inputs under {silver_root} for services={services}")

    # Union all service dataframes, allowing missing columns between services.
    sdf = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

    # --- Normalize ID column names across datasets ---
    # Some sources name location ids with mixed case (PULocationID/DOLocationID), others use lowercase.
    # Normalize to pulocationid/dolocationid to make downstream code consistent.
    if "pulocationid" not in sdf.columns and "PULocationID" in sdf.columns:
        sdf = sdf.withColumn("pulocationid", F.col("PULocationID"))
    if "dolocationid" not in sdf.columns and "DOLocationID" in sdf.columns:
        sdf = sdf.withColumn("dolocationid", F.col("DOLocationID"))

    # Ensure expected columns exist so writes/aggregations don't fail due to missing fields.
    required_cols = [
        "pickup_datetime", "pickup_date", "duration_min",
        "trip_distance", "total_amount", "service", "pulocationid", "dolocationid"
    ]
    for c in required_cols:
        if c not in sdf.columns:
            # Create the missing column with nulls (type inference will set it to NullType).
            sdf = sdf.withColumn(c, F.lit(None))

    # --- Compute feature columns on trip-level data ---
    # Avoid using timestamp pattern columns when computing features to keep schema stable.
    # dayofweek(): 1=Sunday ... 7=Saturday
    features = (
        sdf
        .withColumn("hour", F.hour("pickup_datetime"))                 # pickup hour of day
        .withColumn("dow", F.dayofweek("pickup_datetime"))            # day of week
        .withColumn("is_weekend", F.col("dow").isin(1, 7))            # weekend flag (Sun or Sat)
        .withColumn(
            "speed_mph",
            # speed = distance (miles) / hours; duration_min is minutes so divide by 60
            F.when(F.col("duration_min") > 0, F.col("trip_distance") / (F.col("duration_min") / 60.0))
        )
    )

    # --- Write trip-level feature table to gold/trip_features ---
    # Repartition by service and pickup_date for better downstream reads/partition pruning.
    (
        features
        .repartition(64, "service", "pickup_date")
        .write.mode("overwrite")
        .format("parquet")
        .save(str(gold_root / "trip_features"))
    )

    # --- Aggregation: demand & simple stats per pickup zone / hour ---
    # Compute trips, average distance and average total fare grouped by service/pickup zone/date/hour.
    agg_zone_hour = (
        features
        .groupBy("service", "pulocationid", "pickup_date", "hour")
        .agg(
            F.count("*").alias("trips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
        )
    )
    (
        agg_zone_hour
        .repartition(64, "service", "pickup_date")
        .write.mode("overwrite")
        .format("parquet")
        .save(str(gold_root / "agg_zone_hour"))
    )

    # --- Aggregation: OD (origin-destination) avg duration & speed per hour ---
    # Useful for routing and expected trip-times between zones by hour of day.
    agg_od_hour = (
        features
        .groupBy("service", "pulocationid", "dolocationid", "pickup_date", "hour")
        .agg(
            F.count("*").alias("trips"),
            F.avg("duration_min").alias("avg_duration_min"),
            F.avg("speed_mph").alias("avg_speed_mph"),
        )
    )
    (
        agg_od_hour
        .repartition(64, "service", "pickup_date")
        .write.mode("overwrite")
        .format("parquet")
        .save(str(gold_root / "agg_od_hour"))
    )

    # Stop Spark session when done.
    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    run(cfg)
