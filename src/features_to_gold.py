# src/features_to_gold.py
import argparse
import yaml
from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession, functions as F


def get_spark(app="NYC Taxi - Features to Gold"):
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()


def read_cfg(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run(cfg):
    spark = get_spark()

    silver_root = Path(cfg["silver_path"])   # ./lake/silver
    gold_root   = Path(cfg["gold_path"])     # ./lake/gold
    services    = cfg.get("services", [])

    # --- Read per service with a common basePath, then union ---
    dfs = []
    for svc in services:
        svc_path = silver_root / svc  # e.g., lake/silver/yellow
        if svc_path.exists():
            df = (
                spark.read.format("parquet")
                .option("basePath", str(silver_root))  # pickup_date=* under each service
                .load(str(svc_path))
            )
            if "service" not in df.columns:
                df = df.withColumn("service", F.lit(svc))
            dfs.append(df)

    if not dfs:
        raise FileNotFoundError(f"No Silver inputs under {silver_root} for services={services}")

    sdf = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

    # --- Normalize ID columns (handle PULocationID/DOLocationID vs lowercase) ---
    if "pulocationid" not in sdf.columns and "PULocationID" in sdf.columns:
        sdf = sdf.withColumn("pulocationid", F.col("PULocationID"))
    if "dolocationid" not in sdf.columns and "DOLocationID" in sdf.columns:
        sdf = sdf.withColumn("dolocationid", F.col("DOLocationID"))

    # Ensure expected columns exist (avoid write-time schema surprises)
    required_cols = [
        "pickup_datetime", "pickup_date", "duration_min",
        "trip_distance", "total_amount", "service", "pulocationid", "dolocationid"
    ]
    for c in required_cols:
        if c not in sdf.columns:
            sdf = sdf.withColumn(c, F.lit(None))

    # --- Feature columns (avoid datetime patterns) ---
    # dayofweek(): 1=Sunday ... 7=Saturday
    features = (
        sdf
        .withColumn("hour", F.hour("pickup_datetime"))
        .withColumn("dow", F.dayofweek("pickup_datetime"))
        .withColumn("is_weekend", F.col("dow").isin(1, 7))  # Sun or Sat
        .withColumn(
            "speed_mph",
            F.when(F.col("duration_min") > 0, F.col("trip_distance") / (F.col("duration_min") / 60.0))
        )
    )

    # --- Write trip_features ---
    (
        features
        .repartition(64, "service", "pickup_date")
        .write.mode("overwrite")
        .format("parquet")
        .save(str(gold_root / "trip_features"))
    )

    # --- Aggregation: demand per pickup zone/hour ---
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

    # --- Aggregation: OD avg duration/speed per hour ---
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

    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    run(cfg)
