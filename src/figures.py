import argparse, yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

def read_cfg(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def main(cfg):
    spark = SparkSession.builder.appName("NYC Taxi - Figures").getOrCreate()
    gold = cfg["gold_path"]
    reports = Path("reports"); reports.mkdir(parents=True, exist_ok=True)

    # 1) Hourly demand profile (yellow vs green)
    az = spark.read.parquet(f"{gold}/agg_zone_hour")
    demand_hour = (az.groupBy("service","hour")
                     .agg(F.sum("trips").alias("trips"))
                     .orderBy("service","hour"))
    pdf = demand_hour.toPandas()

    # 2) Avg speed by hour (trip_features)
    tf = spark.read.parquet(f"{gold}/trip_features")
    speed_hour = (tf.groupBy("service","hour")
                    .agg(F.avg("speed_mph").alias("avg_speed_mph"))
                    .orderBy("service","hour"))
    speed_pdf = speed_hour.toPandas()

    # 3) Top 10 pickup zones
    top_zones = (az.groupBy("service","pulocationid")
                   .agg(F.sum("trips").alias("trips"))
                   .orderBy(F.desc("trips")))
    top10_pdf = top_zones.limit(10).toPandas()
    top10_pdf.to_csv(reports / "table_top10_pickup_zones.csv", index=False)

    # Matplotlib plots (no explicit colors/styles)
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # Figure 1: demand by hour
    pivot = pdf.pivot_table(index="hour", columns="service", values="trips", aggfunc="sum").sort_index()
    ax = pivot.plot(figsize=(9,4), marker="o")
    ax.set_title("Demand by Hour")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Trips")
    plt.tight_layout()
    plt.savefig(reports / "fig_hourly_demand.png", dpi=150)
    plt.close()

    # Figure 2: avg speed by hour
    pivot2 = speed_pdf.pivot_table(index="hour", columns="service", values="avg_speed_mph", aggfunc="mean").sort_index()
    ax2 = pivot2.plot(figsize=(9,4), marker="o")
    ax2.set_title("Average Speed (mph) by Hour")
    ax2.set_xlabel("Hour of Day")
    ax2.set_ylabel("Avg speed (mph)")
    plt.tight_layout()
    plt.savefig(reports / "fig_speed_by_hour.png", dpi=150)
    plt.close()

    print("Saved:")
    print(" -", reports / "fig_hourly_demand.png")
    print(" -", reports / "fig_speed_by_hour.png")
    print(" -", reports / "table_top10_pickup_zones.csv")

    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    main(cfg)