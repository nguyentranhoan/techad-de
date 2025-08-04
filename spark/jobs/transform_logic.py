from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, round as spark_round,
    when, mean, stddev, lit, isnan
)
from pyspark.sql.utils import AnalysisException
import traceback

def main():
    try:
        spark = SparkSession.builder.appName("TransformLogic").getOrCreate()

        input_path = "s3a://sample-bucket/staging/clean_emissions/"
        temp_prefix = "s3a://sample-bucket/temp/"

        print(f"Reading cleaned data from: {input_path}")
        df = spark.read.parquet(input_path)
        df.cache()

        # Daily summary by country and device
        df.groupBy("date", "country", "device").agg(
            count("*").alias("record_count"),
            _sum("totalEmissions").alias("sum_totalEmissions"),
            avg("totalEmissions").alias("avg_totalEmissions")
        ).write.mode("overwrite").parquet(f"{temp_prefix}daily_by_country_device")

        # Daily summary by domain and format
        df.groupBy("date", "domain", "format").agg(
            count("*").alias("record_count"),
            _sum("totalEmissions").alias("sum_totalEmissions"),
            avg("totalEmissions").alias("avg_totalEmissions")
        ).write.mode("overwrite").parquet(f"{temp_prefix}daily_by_domain_format")

        # Top 10 domains
        df.groupBy("domain").agg(
            _sum("totalEmissions").alias("total_emissions")
        ).orderBy(col("total_emissions").desc()).limit(10) \
         .write.mode("overwrite").parquet(f"{temp_prefix}top_10_domains")

        # Emission contribution percentages
        df.select(
            "date", "domain",
            (col("adSelectionEmissions") / col("totalEmissions")).alias("pct_adSelection"),
            (col("creativeDistributionEmissions") / col("totalEmissions")).alias("pct_creativeDist"),
            (col("mediaDistributionEmissions") / col("totalEmissions")).alias("pct_mediaDist")
        ).write.mode("overwrite").parquet(f"{temp_prefix}emission_contributions")

        # Daily emissions trends by country
        df.groupBy("date", "country").agg(
            _sum("totalEmissions").alias("total_emissions")
        ).write.mode("overwrite").parquet(f"{temp_prefix}daily_country_trends")

        # Avg domain coverage per format
        df.withColumn("coverage_binary", when(col("domainCoverage") == "measured", 1).otherwise(0)) \
          .groupBy("format").agg(avg("coverage_binary").alias("avg_measured_coverage")) \
          .write.mode("overwrite").parquet(f"{temp_prefix}avg_domain_coverage")

        # Flag outliers (z-score)
        domain_emissions = df.groupBy("domain").agg(
            _sum("totalEmissions").alias("total_emissions")
        )

        stats = domain_emissions.agg(
            mean("total_emissions").alias("mean_val"),
            stddev("total_emissions").alias("stddev_val")
        ).collect()[0]

        mean_val = stats["mean_val"]
        stddev_val = stats["stddev_val"]

        domain_emissions.withColumn(
            "z_score", (col("total_emissions") - lit(mean_val)) / lit(stddev_val)
        ).withColumn(
            "is_outlier", (col("z_score") > 2) | (col("z_score") < -2)
        ).write.mode("overwrite").parquet(f"{temp_prefix}flagged_domains")

        print("All transformation logic complete.")

    except AnalysisException as e:
        print(f"Spark Analysis Error: {e}")
        raise
    except Exception as e:
        print("Error in transformation logic:")
        traceback.print_exc()
        raise
    finally:
        try:
            spark.stop()
            print("Spark session stopped.")
        except:
            print("Could not stop Spark session cleanly.")

if __name__ == "__main__":
    main()
