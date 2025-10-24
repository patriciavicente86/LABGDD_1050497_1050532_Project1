# src/clean_to_silver.py
import argparse, yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, Window

def get_spark(app="NYC Taxi - Clean to Silver"):
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
    bronze = cfg["bronze_path"]          # e.g., ./lake/bronze
    silver = cfg["silver_path"]          # e.g., ./lake/silver
    years = cfg.get("years", [])
    services = cfg.get("services", [])
    dq = cfg.get("dq", {})

    max_trip_hours = float(dq.get("max_trip_hours", 6))
    min_distance_km = float(dq.get("min_distance_km", 0.1))
    min_total_amount = float(dq.get("min_total_amount", 0.0))

    # NYC trip_distance is in miles; convert km threshold -> miles
    min_distance_miles = min_distance_km * 0.621371

    for svc in services:
        # read all requested years for this service
        inputs = [str(Path(bronze) / svc / str(y)) for y in years]
        df = spark.read.format("parquet").load(inputs)

        # standardize datetime names if needed (yellow/green 2024)
        if "tpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        if "tpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

        df = (df
            .withColumn("passenger_count",
                        F.when(F.col("passenger_count").isNull(), F.lit(1)).otherwise(F.col("passenger_count")).cast("int"))
            .withColumn("trip_distance", F.col("trip_distance").cast("double"))
            .withColumn("total_amount", F.col("total_amount").cast("double"))
            .withColumn("duration_min", (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))/60.0)
            .filter((F.col("trip_distance") > min_distance_miles) & (F.col("trip_distance") < 200))
            .filter((F.col("total_amount") > min_total_amount))
            .filter((F.col("duration_min") > 0) & (F.col("duration_min") < max_trip_hours*60.0))
            .filter((F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6))
            .withColumn("pickup_date", F.to_date("pickup_datetime"))
            .withColumn("service", F.lit(svc))
        )

        # de-dup key
        key_cols = ["vendorid","pickup_datetime","dropoff_datetime","pulocationid","dolocationid","total_amount"]
        for c in key_cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))
        w = Window.partitionBy(*key_cols).orderBy(F.col("pickup_datetime").asc())
        df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1).drop("rn")

        # write silver/service partitioned by pickup_date
        out = str(Path(silver) / svc)
        (df
            .repartition(64, "pickup_date")
            .write.mode("overwrite")
            .partitionBy("pickup_date")
            .format("parquet")
            .save(out)
        )

    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    run(cfg)
