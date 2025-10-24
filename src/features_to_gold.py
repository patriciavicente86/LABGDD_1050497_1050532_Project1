# src/features_to_gold.py
import argparse, yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

def get_spark(app="NYC Taxi - Features to Gold"):
    return (SparkSession.builder
            .appName(app)
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate())

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

def read_cfg(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def run(cfg):
    spark = get_spark()
    silver = cfg["silver_path"]
    gold = cfg["gold_path"]
    years = cfg.get("years", [])
    services = cfg.get("services", [])

    # read all services
    inputs = [str(Path(silver) / svc) for svc in services]
    sdf = spark.read.format("parquet").load(inputs)

    features = (sdf
        .withColumn("hour", F.hour("pickup_datetime"))
        .withColumn("dow", F.date_format("pickup_datetime","u").cast("int"))  # 1=Mon..7=Sun
        .withColumn("is_weekend", F.col("dow").isin(6,7).cast("boolean"))
        .withColumn("speed_mph", F.when(F.col("duration_min") > 0, F.col("trip_distance") / (F.col("duration_min")/60.0)))
    )

    (features
        .write.mode("overwrite").format("parquet")
        .save(str(Path(gold) / "trip_features"))
    )

    # demand by pickup zone/hour
    agg_zone_hour = (features
        .groupBy("service","pulocationid","pickup_date","hour")
        .agg(
            F.count("*").alias("trips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
        )
    )
    (agg_zone_hour
        .write.mode("overwrite").format("parquet")
        .save(str(Path(gold) / "agg_zone_hour"))
    )

    # OD duration/speed by hour
    agg_od_hour = (features
        .groupBy("service","pulocationid","dolocationid","pickup_date","hour")
        .agg(
            F.count("*").alias("trips"),
            F.avg("duration_min").alias("avg_duration_min"),
            F.avg("speed_mph").alias("avg_speed_mph"),
        )
    )
    (agg_od_hour
        .write.mode("overwrite").format("parquet")
        .save(str(Path(gold) / "agg_od_hour"))
    )

    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    run(cfg)
