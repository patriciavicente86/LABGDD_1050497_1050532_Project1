# Generate simple reports and figures (PNG + CSV) from processed NYC taxi data in "gold" parquet tables.
# Usage: python figures.py --config path/to/config.yaml

import argparse
import yaml
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

def parse_args():
    # Parse command line arguments. Expects a --config path to a YAML config.
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

def read_cfg(path):
    # Read YAML configuration file and return as a dict.
    with open(path, "r") as f:
        return yaml.safe_load(f)

def main(cfg):
    # Create a Spark session for reading parquet data.
    spark = SparkSession.builder.appName("NYC Taxi - Figures").getOrCreate()

    # Path to the processed "gold" data (provided via config)
    gold = cfg["gold_path"]

    # Ensure output reports directory exists
    reports = Path("reports")
    reports.mkdir(parents=True, exist_ok=True)

    # 1) Hourly demand profile (yellow vs green)
    # Read aggregated trips by zone and hour (parquet table 'agg_zone_hour')
    az = spark.read.parquet(f"{gold}/agg_zone_hour")

    # Aggregate trips by service (e.g., taxi type) and hour of day
    demand_hour = (
        az.groupBy("service", "hour")
          .agg(F.sum("trips").alias("trips"))
          .orderBy("service", "hour")
    )

    # Convert to pandas for plotting with matplotlib
    pdf = demand_hour.toPandas()

    # 2) Average speed by hour (from trip-level features table)
    tf = spark.read.parquet(f"{gold}/trip_features")

    # Compute average speed (mph) by service and hour
    speed_hour = (
        tf.groupBy("service", "hour")
          .agg(F.avg("speed_mph").alias("avg_speed_mph"))
          .orderBy("service", "hour")
    )

    # Convert to pandas for plotting
    speed_pdf = speed_hour.toPandas()

    # 3) Top 10 pickup zones overall
    # Sum trips by pickup location id and service, then sort descending to get top locations
    top_zones = (
        az.groupBy("service", "pulocationid")
          .agg(F.sum("trips").alias("trips"))
          .orderBy(F.desc("trips"))
    )

    # Keep top 10 and save as CSV in reports directory
    top10_pdf = top_zones.limit(10).toPandas()
    top10_pdf.to_csv(reports / "table_top10_pickup_zones.csv", index=False)

    # Matplotlib setup for non-interactive rendering (suitable for containers/servers)
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # Figure 1: demand by hour
    # Pivot the aggregated pandas DataFrame so each service is a column and index is hour
    pivot = (
        pdf.pivot_table(index="hour", columns="service", values="trips", aggfunc="sum")
           .sort_index()
    )

    # Plot demand curves for each service across hours
    ax = pivot.plot(figsize=(9, 4), marker="o")
    ax.set_title("Demand by Hour")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Trips")
    plt.tight_layout()
    plt.savefig(reports / "fig_hourly_demand.png", dpi=150)
    plt.close()

    # Figure 2: average speed by hour
    # Pivot the speed DataFrame similarly so each service is a column
    pivot2 = (
        speed_pdf.pivot_table(index="hour", columns="service", values="avg_speed_mph", aggfunc="mean")
                  .sort_index()
    )

    # Plot average speed curves for each service across hours
    ax2 = pivot2.plot(figsize=(9, 4), marker="o")
    ax2.set_title("Average Speed (mph) by Hour")
    ax2.set_xlabel("Hour of Day")
    ax2.set_ylabel("Avg speed (mph)")
    plt.tight_layout()
    plt.savefig(reports / "fig_speed_by_hour.png", dpi=150)
    plt.close()

    # Print saved file locations for convenience
    print("Saved:")
    print(" -", reports / "fig_hourly_demand.png")
    print(" -", reports / "fig_speed_by_hour.png")
    print(" -", reports / "table_top10_pickup_zones.csv")

    # Stop the Spark session cleanly
    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)
    main(cfg)