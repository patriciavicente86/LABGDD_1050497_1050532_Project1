# Kappa streaming driver for NYC Taxi example:
# - infers schema from a static "bronze" parquet dataset
# - reads a streaming parquet "bronze_stream" using that schema
# - cleans & normalizes records
# - computes micro-batch hourly aggregates (zone x service)
# - writes aggregated results to a "gold" location using foreachBatch
#
# Run: python kappa_driver.py --config path/to/config.yaml
#
# Expected config keys (examples):
#   bronze_path: <path/to/bronze>
#   bronze_stream_path: <path/to/bronze_stream>  # optional
#   gold_path: <path/to/gold>
#   checkpoint_path: <path/to/checkpoint>        # optional
#   services: [ ... ]                            # optional filter
#   years: [ ... ]                               # optional filter
#   dq: { max_trip_hours: .., min_distance_km: .., min_total_amount: .. }  # optional

import argparse
import yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

def get_spark(app="NYC Taxi - Kappa Streaming"):
    """
    Create or get a SparkSession configured for this job.
    The shuffle partitions and partitionOverwriteMode are tuned for the example.
    """
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def parse_args():
    """Parse command line arguments. --config (yaml) is required."""
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

def read_cfg(path):
    """Read YAML config from path and return as dict."""
    with open(path, "r") as f:
        return yaml.safe_load(f)

def run(cfg):
    """
    Main pipeline:
    1. Infer schema by reading a static bronze (parquet).
    2. Use inferred schema to read streaming parquet from bronze_stream.
    3. Standardize column names and types, perform basic cleaning / DQ.
    4. Compute hourly aggregates by zone and service.
    5. Write aggregates using foreachBatch (micro-batch overwrite per partition).
    """
    spark = get_spark()

    # Paths and config extraction
    bronze_root   = Path(cfg["bronze_path"])                               # static bronze for schema inference
    bronze_stream = Path(cfg.get("bronze_stream_path", cfg["bronze_path"] + "_stream"))
    bronze_stream.mkdir(parents=True, exist_ok=True)                        # ensure stream path exists locally (no-op on distributed FS)
    gold          = Path(cfg["gold_path"])
    checkpoint    = cfg.get("checkpoint_path", "checkpoints/kappa_zone_hour")
    services      = cfg.get("services", [])   # filter by service types if provided (e.g., taxi types)
    years         = [int(y) for y in cfg.get("years", [])]  # filter by year if provided
    dq            = cfg.get("dq", {})  # data-quality thresholds

    # Data-quality thresholds (defaults if missing)
    max_trip_hours  = float(dq.get("max_trip_hours", 6))
    min_distance_km = float(dq.get("min_distance_km", 0.1))
    min_total_amt   = float(dq.get("min_total_amount", 0.0))
    min_distance_mi = min_distance_km * 0.621371  # convert km -> miles (source data uses miles)

    # ----- infer schema from Bronze (static read) -----
    # We need a concrete schema for parquet streaming reads. Read the static parquet to get the schema.
    try:
        static_bronze = (
            spark.read.format("parquet")
            .option("basePath", str(bronze_root))
            .load(str(bronze_root))
        )
        # Optionally apply the same filters used for streaming so inferred schema reflects filtered data
        if services:
            static_bronze = static_bronze.filter(F.col("service").isin(services))
        if years:
            static_bronze = static_bronze.filter(F.col("year").isin(years))
        inferred_schema = static_bronze.schema
    except Exception as e:
        # If schema inference fails, surface a readable error (likely the bronze data is missing)
        raise RuntimeError(
            f"Could not infer schema from bronze at {bronze_root}. "
            f"Run your bronze ingest first."
        ) from e

    # ----- streaming read from bronze_stream with inferred schema -----
    # Use the same basePath option so Spark can discover partitions correctly.
    sdf = (
        spark.readStream.format("parquet")
        .schema(inferred_schema)
        .option("basePath", str(bronze_stream))
        .load(str(bronze_stream))
    )
    # Apply filters early in the stream to reduce processing
    if services:
        sdf = sdf.filter(F.col("service").isin(services))
    if years:
        sdf = sdf.filter(F.col("year").isin(years))

    # ----- standardize datetime columns -----
    # Different taxi datasets use different column names (tpep/lpep). Normalize to 'pickup_datetime' and 'dropoff_datetime'
    for src, dst in [
        ("tpep_pickup_datetime", "pickup_datetime"),
        ("tpep_dropoff_datetime", "dropoff_datetime"),
        ("lpep_pickup_datetime", "pickup_datetime"),
        ("lpep_dropoff_datetime", "dropoff_datetime"),
    ]:
        if src in sdf.columns and dst not in sdf.columns:
            sdf = sdf.withColumnRenamed(src, dst)

    # ----- duration -----
    # Compute duration in minutes. Prefer precomputed epoch timestamps if available (pickup_ts/dropoff_ts),
    # otherwise compute from datetime columns.
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
    # - Fill null passenger_count with 1 and cast to int
    # - Cast trip_distance and total_amount to double
    # - Filter out extremely short / long / cheap trips based on DQ thresholds
    # - Extract pickup_date and hour for partitioning and aggregations
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

    # normalize numeric ID column names if needed (older datasets use camelcase)
    if "pulocationid" not in sdf.columns and "PULocationID" in sdf.columns:
        sdf = sdf.withColumn("pulocationid", F.col("PULocationID"))
    if "dolocationid" not in sdf.columns and "DOLocationID" in sdf.columns:
        sdf = sdf.withColumn("dolocationid", F.col("DOLocationID"))

    # ----- micro-batch aggregate (same grain as agg_zone_hour) -----
    # Aggregate at the service x pulocation x day x hour granularity.
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
        """
        Called for each micro-batch. We:
        - repartition to reduce small files and ensure same partitioning keys
        - overwrite the partitions for the batch (dynamic mode enabled in Spark config)
        - write as parquet to out_path
        """
        (
            batch_df
            .repartition(64, "service", "pickup_date")
            .write.mode("overwrite")
            .partitionBy("service", "pickup_date")
            .format("parquet")
            .save(out_path)
        )
        # Logging: show which micro-batch was written and how many rows it contained
        print(f"[kappa] wrote micro-batch {batch_id} -> {out_path} rows={batch_df.count()}")

    # ----- start streaming query -----
    # Use outputMode("update") so we can use foreachBatch safely (avoids append without watermark issues).
    q = (
        agg_batch.writeStream
        .outputMode("update")                # update mode emits changed aggregation results per trigger
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")  # process every 10s
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
