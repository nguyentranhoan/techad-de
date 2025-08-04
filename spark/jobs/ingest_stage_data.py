import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round as spark_round
from pyspark.sql.types import DoubleType

# MinIO/S3 connection details (assumes configs set in spark-defaults.conf)
BUCKET = os.environ.get("MINIO_BUCKET", "sample-bucket")
CSV_PATH = f"s3a://{BUCKET}/raw/advertising_emissions.csv"
PARQUET_PATH = f"s3a://{BUCKET}/staging/clean_emissions/"

# Create Spark session
spark = SparkSession.builder \
    .appName("IngestAndStageEmissionsData") \
    .getOrCreate()

print(f"Reading CSV from {CSV_PATH}")
df = spark.read.option("header", "true").csv(CSV_PATH)

print("Initial Schema:")
df.printSchema()

# Transform and clean data
df_cleaned = df \
    .withColumn("date", to_date(col("date"), "MM/dd/yyyy")) \
    .withColumn("adSelectionEmissions", col("adSelectionEmissions").cast(DoubleType())) \
    .withColumn("creativeDistributionEmissions", col("creativeDistributionEmissions").cast(DoubleType())) \
    .withColumn("mediaDistributionEmissions", col("mediaDistributionEmissions").cast(DoubleType())) \
    .withColumn("totalEmissions", col("totalEmissions").cast(DoubleType()))

df_valid = df_cleaned.dropna(subset=[
    "date", "domain", "country",
    "adSelectionEmissions", "creativeDistributionEmissions",
    "mediaDistributionEmissions", "totalEmissions"
])

df_valid = df_valid.withColumn(
    "emission_check",
    spark_round(
        col("adSelectionEmissions") +
        col("creativeDistributionEmissions") +
        col("mediaDistributionEmissions") -
        col("totalEmissions"), 3
    )
).filter((col("emission_check") >= -0.1) & (col("emission_check") <= 0.1))

print("Cleaned Schema:")
df_valid.printSchema()

print("Sample cleaned rows:")
df_valid.show(10)

# Write output
df_valid.drop("emission_check") \
    .write.mode("overwrite") \
    .parquet(PARQUET_PATH)

print(f"✅ Wrote cleaned data to: {PARQUET_PATH}")

# Stop Spark session
spark.stop()
print("🛑 Spark session stopped.")
