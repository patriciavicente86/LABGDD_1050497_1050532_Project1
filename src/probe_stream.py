from pyspark.sql import SparkSession, functions as F

def main():
    spark = SparkSession.builder.getOrCreate()
    try:
        df = spark.read.parquet("lake/gold/agg_zone_hour_streaming")
        print("streaming agg rows:", df.count())
        df.groupBy("service").agg(F.sum("trips").alias("trips")).show()
    except Exception as e:
        print("probe error:", e)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()