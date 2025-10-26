# Ingest parquet files from a raw data layout into a "bronze" parquet dataset.
# - Reads config yaml to find data root, output path, years and services to process
# - Loads parquet files for each service/year combination
# - Normalizes timestamp columns and adds partition columns (service, year, month)
# - Appends results to the bronze parquet dataset partitioned by service/year/month

import argparse
import yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
import glob

def build_spark():
    """
    Build and return a SparkSession for this ingestion job.
    Keep default settings; the devcontainer / environment may inject configs.
    """
    return (SparkSession.builder
            .appName("ingest_bronze")
            .getOrCreate())

def resolve_path(repo_root: Path, p: str) -> str:
    """
    Resolve a potentially relative path `p` against the repository root.
    If `p` is absolute, return it unchanged. Return the resolved path as a string.
    """
    pth = Path(p)
    return str((repo_root / pth).resolve()) if not pth.is_absolute() else str(pth)

def main(config_path: str):
    # repo_root is the project root (two levels up from this file)
    repo_root = Path(__file__).resolve().parents[1]

    # Load YAML configuration (expects keys: data_root, bronze_path, years, services)
    cfg = yaml.safe_load(open(config_path))

    # Resolve configured paths relative to repo_root if needed
    data_root = resolve_path(repo_root, cfg["data_root"])
    out_bronze = resolve_path(repo_root, cfg["bronze_path"])

    # Ensure years are strings and services are normalized to lowercase
    years = [str(y) for y in cfg["years"]]
    services = [s.lower() for s in cfg["services"]]

    # Start Spark
    spark = build_spark()
    total = 0  # running total of ingested rows

    # Iterate all combinations of year and service configured
    for y in years:
        for s in services:
            # Expect input layout: <data_root>/<service>/<year>/*
            path = f"{data_root}/{s}/{y}/*"
            print(f"[INGEST] Loading: {path}")

            # If no files exist for this pattern, skip gracefully
            if not glob.glob(path):
                print(f"[SKIP] No files match: {path}")
                continue

            # Read all parquet files matching the pattern into a DataFrame
            df = spark.read.format("parquet").load(path)

            # Helper to pick the first column name that exists in the dataframe
            def first_existing(df, candidates):
                for c in candidates:
                    if c in df.columns:
                        return F.col(c)
                return None

            # Different datasets use different timestamp column names.
            # Try known candidates for pickup and dropoff timestamps.
            pickup_col  = first_existing(df, ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"])
            dropoff_col = first_existing(df, ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"])

            # If required timestamp columns are missing, skip this dataset
            if pickup_col is None or dropoff_col is None:
                print(f"[SKIP] Could not find pickup/dropoff columns in {path}. Columns seen: {df.columns[:8]} ...")
                continue

            # Normalize/derive columns:
            # - Cast chosen pickup/dropoff columns to timestamp types
            # - Add a "service" column indicating the service name
            # - Add a "year" column (from the loop variable)
            # - Derive "month" from pickup_ts; if pickup_ts is missing, default to 1
            df = (df
                .withColumn("pickup_ts", pickup_col.cast("timestamp"))
                .withColumn("dropoff_ts", dropoff_col.cast("timestamp"))
                .withColumn("service", F.lit(s))
                .withColumn("year", F.lit(int(y)))
                .withColumn(
                    "month",
                    F.when(F.col("pickup_ts").isNotNull(), F.month("pickup_ts"))
                     .otherwise(F.lit(1))
                     .cast("int")
                )
            )

            # Write out appended parquet, partitioned for efficient reads later.
            # Partitioning keys: service, year, month
            (df.write
             .mode("append")
             .partitionBy("service", "year", "month")
             .parquet(out_bronze))

            # Count rows written for logging/monitoring
            cnt = df.count()
            total += cnt
            print(f"[INGEST] Wrote {cnt} rows for {s} {y}. Total so far: {total}")

    # Cleanly stop Spark and report output location
    spark.stop()
    print(f"[DONE] Bronze at: {out_bronze}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)  # path to config yaml
    args = parser.parse_args()
    main(args.config)