import argparse, yaml
from pyspark.sql import SparkSession, functions as F

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

def main(cfg):
    spark = SparkSession.builder.appName("NYC Taxi - Lambda vs Kappa Compare").getOrCreate()
    gold = cfg["gold_path"]

    batch = spark.read.parquet(f"{gold}/agg_zone_hour") \
        .select("service","pulocationid","pickup_date","hour","trips") \
        .withColumnRenamed("trips","trips_batch")

    stream = spark.read.parquet(f"{gold}/agg_zone_hour_streaming") \
        .select("service","pulocationid","pickup_date","hour","trips") \
        .withColumnRenamed("trips","trips_stream")

    joined = batch.join(stream, ["service","pulocationid","pickup_date","hour"], "inner")
    total = joined.count()
    exact = joined.filter(F.col("trips_batch")==F.col("trips_stream")).count()
    mae = joined.select(F.abs(F.col("trips_batch")-F.col("trips_stream")).alias("ae")).agg(F.avg("ae")).first()[0]

    print({"overlap_keys": total, "exact_match_keys": exact, "exact_match_pct": round(100*exact/max(total,1),2), "MAE_trips": mae})

    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    with open(args.config,"r") as f: cfg = yaml.safe_load(f)
    main(cfg)
