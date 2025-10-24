# src/kappa_driver.py
import argparse, yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

def get_spark(app="NYC Taxi - Kappa Streaming"):
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
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

    bronze_root   = Path(cfg["bronze_path"])                               # batch bronze for schema
    bronze_stream = Path(cfg.get("bronze_stream_path", cfg["bronze_path"] + "_stream"))
    bronze_stream.mkdir(parents=True, exist_ok=True)                        # ensure exists
    gold          = Path(cfg["gold_path"])
    checkpoint    = cfg.get("checkpoint_path", "checkpoints/kappa_zone_hour")
    services      = cfg.get("services", [])
    years         = [int(y) for y in cfg.get("years", [])]
    dq            = cfg.get("dq", {})

    max_trip_hours  = float(dq.get("max_trip_hours", 6))
    min_distance_km = float(dq.get("min_distance_km", 0.1))
    min_total_amt   = float(dq.get("min_total_amount", 0.0))
    min_distance_mi = min_distance_km * 0.621371

    # ----- infer schema from Bronze (static read) -----
    try:
        static_bronze = (
            spark.read.format("parquet")
            .option("basePath", str(bronze_root))
            .load(str(bronze_root))
        )
        if services:
            static_bronze = static_bronze.filter(F.col("service").isin(services))
        if years:
            static_bronze = static_bronze.filter(F.col("year").isin(years))
        inferred_schema = static_bronze.schema
    except Exception as e:
        raise RuntimeError(
            f"Could not infer schema from bronze at {bronze_root}. "
            f"Run your bronze ingest first."
        ) from e

    # ----- streaming read from bronze_stream with inferred schema -----
    sdf = (
        spark.readStream.format("parquet")
        .schema(inferred_schema)
        .option("basePath", str(bronze_stream))
        .load(str(bronze_stream))
    )
    if services:
        sdf = sdf.filter(F.col("service").isin(services))
    if years:
        sdf = sdf.filter(F.col("year").isin(years))

    # ----- standardize datetime columns -----
    for src, dst in [
        ("tpep_pickup_datetime", "pickup_datetime"),
        ("tpep_dropoff_datetime", "dropoff_datetime"),
        ("lpep_pickup_datetime", "pickup_datetime"),
        ("lpep_dropoff_datetime", "dropoff_datetime"),
    ]:
        if src in sdf.columns and dst not in sdf.columns:
            sdf = sdf.withColumnRenamed(src, dst)

    # ----- duration -----
    if "pickup_ts" in sdf.columns and "dropoff_ts" in sdf.columns:
        sdf = sdf.withColumn(
            "duration_min",
            F.expr("timestampdiff(SECOND, pickup_ts, dropoff_ts)") / F.lit(60.0),
        )
    else:
        sdf = sdf.withColumn(
            "duration_min",
            F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)") / F.lit(60.0),
        )

    # ----- cleaning (same as Silver) -----
    sdf = (
        sdf
        .withColumn(
            "passenger_count",
            F.when(F.col("passenger_count").isNull(), F.lit(1))
             .otherwise(F.col("passenger_count")).cast("int"),
        )
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .filter((F.col("trip_distance") > min_distance_mi) & (F.col("trip_distance") < 200))
        .filter(F.col("total_amount") > min_total_amt)
        .filter((F.col("duration_min") > 0) & (F.col("duration_min") < max_trip_hours * 60.0))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("hour", F.hour("pickup_datetime"))
    )

    # normalize IDs if needed
    if "pulocationid" not in sdf.columns and "PULocationID" in sdf.columns:
        sdf = sdf.withColumn("pulocationid", F.col("PULocationID"))
    if "dolocationid" not in sdf.columns and "DOLocationID" in sdf.columns:
        sdf = sdf.withColumn("dolocationid", F.col("DOLocationID"))

    # ----- micro-batch aggregate (same grain as agg_zone_hour) -----
    agg_batch = (
        sdf.groupBy("service", "pulocationid", "pickup_date", "hour")
           .agg(
               F.count("*").alias("trips"),
               F.avg("trip_distance").alias("avg_distance"),
               F.avg("total_amount").alias("avg_total_amount"),
           )
    )

    out_path = str(gold / "agg_zone_hour_streaming")

    def write_batch(batch_df, batch_id: int):
        (
            batch_df
            .repartition(64, "service", "pickup_date")
            .write.mode("overwrite")
            .partitionBy("service", "pickup_date")
            .format("parquet")
            .save(out_path)
        )
        print(f"[kappa] wrote micro-batch {batch_id} -> {out_path} rows={batch_df.count()}")

    q = (
        agg_batch.writeStream
        .outputMode("update")                # <-- crucial fix (avoid append-without-watermark)
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Streaming started. Watching base: {bronze_stream}")
    print(f"Filters -> services: {services or 'ALL'}, years: {years or 'ALL'}")
    print("Press Ctrl+C to stop.")
    q.awaitTermination()

if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    run(cfg)
