"""
Probe script to inspect aggregated streaming data stored as Parquet files.

This script:
- creates (or reuses) a SparkSession,
- reads a Parquet dataset containing hourly aggregated streaming metrics,
- prints the number of rows,
- prints a per-service sum of the 'trips' column,
- and ensures the Spark session is stopped on completion or error.
"""

from pyspark.sql import SparkSession, functions as F

def main():
    # Create or get an existing SparkSession (uses defaults/config from the environment)
    spark = SparkSession.builder.getOrCreate()

    try:
        # Read the aggregated hourly streaming data from the gold zone (Parquet files)
        # Path: "lake/gold/agg_zone_hour_streaming"
        df = spark.read.parquet("lake/gold/agg_zone_hour_streaming")

        # Count the number of rows in the DataFrame and print a simple probe message
        print("streaming agg rows:", df.count())

        # Group by the 'service' column and compute the total number of trips per service,
        # then display the results to the console.
        df.groupBy("service").agg(F.sum("trips").alias("trips")).show()

    except Exception as e:
        # If anything goes wrong while reading or computing, print a short error message.
        print("probe error:", e)

    finally:
        # Always stop the SparkSession to free resources.
        spark.stop()

if __name__ == "__main__":
    main()