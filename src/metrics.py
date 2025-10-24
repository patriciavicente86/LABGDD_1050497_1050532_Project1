# src/metrics.py
import argparse, yaml
from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession, functions as F

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

def read_cfg(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def read_bronze(spark, bronze_root, services, years):
    bronze_root = Path(bronze_root)
    dfs = []
    for svc in services:
        for y in years:
            p = bronze_root / f"service={svc}" / f"year={y}"
            if p.exists():
                df = (
                    spark.read.format("parquet")
                    .option("basePath", str(bronze_root))
                    .load(str(p))
                )
                if "service" not in df.columns:
                    df = df.withColumn("service", F.lit(svc))
                dfs.append(df)
    if not dfs:
        return None
    return reduce(lambda a,b: a.unionByName(b, allowMissingColumns=True), dfs)

def main(cfg):
    spark = SparkSession.builder.appName("NYC Taxi - Metrics").getOrCreate()
    bronze_root, silver_root, gold_root = cfg["bronze_path"], cfg["silver_path"], cfg["gold_path"]
    services, years = cfg.get("services", []), cfg.get("years", [])

    reports_dir = Path("reports"); reports_dir.mkdir(parents=True, exist_ok=True)

    # ----- ROW COUNTS -----
    bdf = read_bronze(spark, bronze_root, services, years)
    bronze_rows = bdf.count() if bdf is not None else None

    counts_silver = {}
    total_silver = 0
    for svc in services:
        p = Path(silver_root) / svc
        if p.exists():
            df = spark.read.parquet(str(p))
            c = df.count()
            counts_silver[svc] = c
            total_silver += c

    def cnt(path):
        try:
            return spark.read.parquet(path).count()
        except Exception:
            return None

    trip_features = cnt(f"{gold_root}/trip_features")
    agg_zone_hour = cnt(f"{gold_root}/agg_zone_hour")
    agg_od_hour   = cnt(f"{gold_root}/agg_od_hour")

    print("== ROW COUNTS ==")
    print({
        "bronze": bronze_rows,
        **{f"silver_{k}": v for k,v in counts_silver.items()},
        "trip_features": trip_features,
        "agg_zone_hour": agg_zone_hour,
        "agg_od_hour": agg_od_hour
    })
    if bronze_rows:
        kept_pct = round(100.0 * total_silver / bronze_rows, 2)
        print(f"\n== RETENTION ==\nBronze -> Silver kept: {total_silver}/{bronze_rows} rows ({kept_pct}%)")

    # ----- GOLD SUMMARIES -----
    # 1) Demand profile by HOUR per service (from agg_zone_hour)
    try:
        az = spark.read.parquet(f"{gold_root}/agg_zone_hour")
        demand_hour = (az.groupBy("service","hour")
                         .agg(F.sum("trips").alias("trips"))
                         .orderBy("service","hour"))
        print("\n== DEMAND BY HOUR (top 10 rows) ==")
        demand_hour.show(10, truncate=False)
        demand_hour.coalesce(1).write.mode("overwrite").option("header","true").csv(str(reports_dir / "gold_demand_by_hour"))
    except Exception as e:
        print("demand_by_hour failed:", e)

    # 2) Top OD pairs by trips (from agg_od_hour)
    try:
        od = spark.read.parquet(f"{gold_root}/agg_od_hour")
        od_top = (od.groupBy("service","pulocationid","dolocationid")
                    .agg(F.sum("trips").alias("trips"))
                    .orderBy(F.desc("trips")))
        print("\n== TOP 10 OD PAIRS BY TRIPS ==")
        od_top.show(10, truncate=False)
        od_top.coalesce(1).write.mode("overwrite").option("header","true").csv(str(reports_dir / "gold_top_od_pairs"))
    except Exception as e:
        print("top_od_pairs failed:", e)

    # 3) Average speed by hour per service (from trip_features)
    try:
        tf = spark.read.parquet(f"{gold_root}/trip_features")
        speed_hour = (tf.groupBy("service","hour")
                        .agg(F.avg("speed_mph").alias("avg_speed_mph")))
        print("\n== AVG SPEED (MPH) BY HOUR (top 10 rows) ==")
        speed_hour.orderBy("service","hour").show(10, truncate=False)
        speed_hour.coalesce(1).write.mode("overwrite").option("header","true").csv(str(reports_dir / "gold_speed_by_hour"))
    except Exception as e:
        print("speed_by_hour failed:", e)

    print(f"\nCSV exports written in: {reports_dir}/")
    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    main(cfg)
