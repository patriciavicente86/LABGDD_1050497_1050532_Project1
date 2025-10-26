import argparse
import yaml
from pyspark.sql import SparkSession, functions as F

# Script to compare aggregated taxi trip counts computed via batch (lambda) vs streaming (kappa) pipelines.
# It loads two parquet datasets with the same schema, joins them on their keys, and reports overlap, exact matches,
# percentage exact, and the mean absolute error (MAE) in trip counts.

def parse_args():
    """
    Parse command line arguments.
    --config : path to a YAML config file containing at least "gold_path".
    """
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

def main(cfg):
    """
    Main routine:
    - start Spark
    - read the batch and streaming aggregated parquet tables
    - select relevant columns and rename the trips column to distinguish sources
    - inner-join on the grouping keys
    - compute counts of overlapping keys, exact matches, percentage exact, and MAE of trip counts
    - print a small summary dict
    """
    # Start a Spark session
    spark = SparkSession.builder.appName("NYC Taxi - Lambda vs Kappa Compare").getOrCreate()

    # Base path for gold (reference) data, provided by config file
    gold = cfg["gold_path"]

    # Read batch (lambda) aggregation results from parquet and keep only needed columns.
    # Rename "trips" to "trips_batch" to avoid collision after join.
    batch = (
        spark.read.parquet(f"{gold}/agg_zone_hour")
        .select("service", "pulocationid", "pickup_date", "hour", "trips")
        .withColumnRenamed("trips", "trips_batch")
    )

    # Read streaming (kappa) aggregation results from parquet and keep only needed columns.
    # Rename "trips" to "trips_stream" to avoid collision after join.
    stream = (
        spark.read.parquet(f"{gold}/agg_zone_hour_streaming")
        .select("service", "pulocationid", "pickup_date", "hour", "trips")
        .withColumnRenamed("trips", "trips_stream")
    )

    # Inner join on the grouping keys to compare matching buckets between the two approaches.
    joined = batch.join(stream, ["service", "pulocationid", "pickup_date", "hour"], "inner")

    # Total number of overlapping keys present in both results
    total = joined.count()

    # Number of keys where trips are exactly equal between batch and stream
    exact = joined.filter(F.col("trips_batch") == F.col("trips_stream")).count()

    # Mean Absolute Error of trips between batch and stream
    mae = (
        joined
        .select(F.abs(F.col("trips_batch") - F.col("trips_stream")).alias("ae"))
        .agg(F.avg("ae"))
        .first()[0]
    )

    # Print a succinct summary. Protect division by zero using max(total,1).
    print({
        "overlap_keys": total,
        "exact_match_keys": exact,
        "exact_match_pct": round(100 * exact / max(total, 1), 2),
        "MAE_trips": mae
    })

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)
    main(cfg)
