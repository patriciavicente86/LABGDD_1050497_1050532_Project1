# src/ingest_bronze.py
import argparse, yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
import glob

def build_spark():
    return (SparkSession.builder
            .appName("ingest_bronze")
            .getOrCreate())

def resolve_path(repo_root: Path, p: str) -> str:
    pth = Path(p)
    return str((repo_root / pth).resolve()) if not pth.is_absolute() else str(pth)

def main(config_path: str):
    repo_root = Path(__file__).resolve().parents[1]
    cfg = yaml.safe_load(open(config_path))

    data_root = resolve_path(repo_root, cfg["data_root"])
    out_bronze = resolve_path(repo_root, cfg["bronze_path"])
    years = [str(y) for y in cfg["years"]]
    services = [s.lower() for s in cfg["services"]]

    spark = build_spark()
    total = 0

    for y in years:
        for s in services:
            path = f"{data_root}/{s}/{y}/*"
            print(f"[INGEST] Loading: {path}")

            if not glob.glob(path):
                print(f"[SKIP] No files match: {path}")
                continue

            df = spark.read.format("parquet").load(path)

            # helper to pick the first existing column
            def first_existing(df, candidates):
                for c in candidates:
                    if c in df.columns:
                        return F.col(c)
                return None

            pickup_col  = first_existing(df, ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"])
            dropoff_col = first_existing(df, ["tpep_dropoff_datetime","lpep_dropoff_datetime","dropoff_datetime"])

            if pickup_col is None or dropoff_col is None:
                print(f"[SKIP] Could not find pickup/dropoff columns in {path}. Columns seen: {df.columns[:8]} ...")
                continue

            df = (df
                .withColumn("pickup_ts", pickup_col.cast("timestamp"))
                .withColumn("dropoff_ts", dropoff_col.cast("timestamp"))
                .withColumn("service", F.lit(s))
                .withColumn("year", F.lit(int(y)))
                # If the file is missing pickup_ts (shouldn't be), default month to 1
                .withColumn("month", F.when(F.col("pickup_ts").isNotNull(), F.month("pickup_ts")).otherwise(F.lit(1)).cast("int"))
            )

            (df.write
            .mode("append")
            .partitionBy("service","year","month")
            .parquet(out_bronze))

            cnt = df.count()
            total += cnt
            print(f"[INGEST] Wrote {cnt} rows for {s} {y}. Total so far: {total}")

    spark.stop()
    print(f"[DONE] Bronze at: {out_bronze}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    main(args.config)