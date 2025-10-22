import argparse, yaml, os
from pyspark.sql import SparkSession

def spark(): return SparkSession.builder.appName("ingest_bronze").getOrCreate()

def main(cfg):
    spark_s = spark()
    data_root = cfg["data_root"]; out = cfg["bronze_path"]
    years = cfg["years"]; services = cfg["services"]
    for y in years:
        for s in services:
            # assume monthly Parquet/CSV already downloaded/mounted under data_root/s/y=YYYY/m=MM.*
            df = spark_s.read.option("header", True).format("parquet").load(f"{data_root}/{s}/{y}/*")
            # minimal standardization: lower snake_case, timestamps to ts
            # (add a small mapping dict per service/year if needed)
            df.write.mode("overwrite").partitionBy("service","year","month").parquet(out)
    spark_s.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--config", required=True)
    cfg = yaml.safe_load(open(p.parse_args().config))
    main(cfg)