# src/clean_to_silver.py
import argparse
import yaml
from functools import reduce
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, Window


def get_spark(app="NYC Taxi - Clean to Silver"):
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

    bronze_root = Path(cfg["bronze_path"])   # ./lake/bronze
    silver_root = Path(cfg["silver_path"])   # ./lake/silver
    years = cfg.get("years", [])
    services = cfg.get("services", [])
    dq = cfg.get("dq", {})

    max_trip_hours = float(dq.get("max_trip_hours", 6))
    min_distance_km = float(dq.get("min_distance_km", 0.1))
    min_total_amount = float(dq.get("min_total_amount", 0.0))

    # NYC trip_distance is miles; convert km threshold -> miles
    min_distance_miles = min_distance_km * 0.621371

    for svc in services:
        # ---- Read per (service, year) with a common basePath, then union ----
        dfs = []
        for y in years:
            path = bronze_root / f"service={svc}" / f"year={y}"  # your layout
            if path.exists():
                df_part = (
                    spark.read.format("parquet")
                    .option("basePath", str(bronze_root))
                    .load(str(path))  # includes month=* subdirs
                )
                if "service" not in df_part.columns:
                    df_part = df_part.withColumn("service", F.lit(svc))
                dfs.append(df_part)

        if not dfs:
            raise FileNotFoundError(
                f"No Bronze inputs under {bronze_root} for service='{svc}', years={years}. "
                f"Expected like {bronze_root}/service={svc}/year=<YYYY>/month=*"
            )

        df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

        # ---- Standardize datetime columns (yellow: tpep_*, green: lpep_*) ----
        if "tpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        if "tpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        if "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        if "lpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

        # ---- Compute duration_min robustly ----
        if "pickup_ts" in df.columns and "dropoff_ts" in df.columns:
            # They are timestamps in your bronze -> use timestampdiff
            df = df.withColumn(
                "duration_min",
                F.expr("timestampdiff(SECOND, pickup_ts, dropoff_ts)") / F.lit(60.0),
            )
        else:
            # fallback: use pickup/dropoff datetime columns
            df = df.withColumn(
                "duration_min",
                F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)") / F.lit(60.0),
            )

        # ---- Core cleaning rules ----
        df = (
            df.withColumn(
                "passenger_count",
                F.when(F.col("passenger_count").isNull(), F.lit(1))
                 .otherwise(F.col("passenger_count"))
                 .cast("int"),
            )
            .withColumn("trip_distance", F.col("trip_distance").cast("double"))
            .withColumn("total_amount", F.col("total_amount").cast("double"))
            .filter((F.col("trip_distance") > min_distance_miles) & (F.col("trip_distance") < 200))
            .filter(F.col("total_amount") > min_total_amount)
            .filter((F.col("duration_min") > 0) & (F.col("duration_min") < max_trip_hours * 60.0))
            .filter((F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6))
            .withColumn("pickup_date", F.to_date("pickup_datetime"))
            .withColumn("service", F.lit(svc))  # enforce for consistency
        )

        # ---- De-dup by composite key (be tolerant to column case) ----
        key_candidates = {
            "vendorid": ["vendorid", "VendorID"],
            "pulocationid": ["pulocationid", "PULocationID"],
            "dolocationid": ["dolocationid", "DOLocationID"],
        }
        # create lowercase aliases if needed for the key
        for alias, options in key_candidates.items():
            if alias not in df.columns:
                for opt in options:
                    if opt in df.columns:
                        df = df.withColumn(alias, F.col(opt))
                        break
        # ensure key columns exist
        for c in ["vendorid", "pickup_datetime", "dropoff_datetime", "pulocationid", "dolocationid", "total_amount"]:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))

        w = Window.partitionBy("vendorid", "pickup_datetime", "dropoff_datetime",
                               "pulocationid", "dolocationid", "total_amount") \
                  .orderBy(F.col("pickup_datetime").asc())
        df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

        # ---- Write Silver (partitioned by pickup_date) ----
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
