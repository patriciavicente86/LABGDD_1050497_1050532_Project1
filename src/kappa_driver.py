# src/kappa_driver.py
import argparse, yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

def get_spark(app="NYC Taxi - Kappa Streaming"):
    return (SparkSession.builder
            .appName(app)
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate())

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

def read_cfg(path):
    with open(path,"r") as f:
        return yaml.safe_load(f)

def run(cfg):
    spark = get_spark()
    gold = cfg["gold_path"]
    services = cfg.get("services", [])
    bronze_stream = cfg.get("bronze_stream_path", cfg["bronze_path"] + "_stream")
    checkpoint = cfg.get("checkpoint_path", "checkpoints/kappa_zone_hour")

    # watch all services folders under bronze_stream
    inputs = [str(Path(bronze_stream) / svc) for svc in services]
    sdf = (spark.readStream.format("parquet").load(inputs))

    # rename datetimes if present
    if "tpep_pickup_datetime" in sdf.columns:
        sdf = sdf.withColumnRenamed("tpep_pickup_datetime","pickup_datetime")
    if "tpep_dropoff_datetime" in sdf.columns:
        sdf = sdf.withColumnRenamed("tpep_dropoff_datetime","dropoff_datetime")

    sdf = (sdf
        .withColumn("passenger_count",
                    F.when(F.col("passenger_count").isNull(), F.lit(1)).otherwise(F.col("passenger_count")).cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("duration_min", (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))/60.0)
        .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") < 200))
        .filter((F.col("total_amount") > 0))
        .filter((F.col("duration_min") > 0) & (F.col("duration_min") < 360))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("hour", F.hour("pickup_datetime"))
    )

    # if service column missing, try infer by folder (optional)
    if "service" not in sdf.columns:
        # Structured Streaming can't easily parse parent dir; skip and write without it
        pass

    agg = (sdf.groupBy("pulocationid","pickup_date","hour")
              .agg(F.count("*").alias("trips"),
                   F.avg("trip_distance").alias("avg_distance"),
                   F.avg("total_amount").alias("avg_total_amount")))

    q = (agg.writeStream
        .outputMode("update")
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .option("path", str(Path(gold) / "agg_zone_hour_streaming"))
        .start())

    print("Streaming started. Add files to", inputs, "Press Ctrl+C to stop.")
    q.awaitTermination()

if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    run(cfg)
