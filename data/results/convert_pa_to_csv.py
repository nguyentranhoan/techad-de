from pyspark.sql import SparkSession


# Define the input Parquet file path and output CSV directory
input_parquet_path = "data/results/08-05-2025-01-37-17_files_list/output/avg_domain_coverage/part-00000-c10fdc09-50a0-42c0-8fb2-f73d88768f51-c000.snappy.parquet"
output_csv_dir = "data/results/08-05-2025-01-37-17_files_list/output/avg_domain_coverage/output_csv"

def main():

    spark = SparkSession.builder \
        .appName("ParquetToCSV") \
        .getOrCreate()
    # Read the Parquet file and write it as a CSV
    df = spark.read.parquet(input_parquet_path)
    # Ensure the output directory exists or is created
    df.write.mode("overwrite").option("header", "true").csv(output_csv_dir)
    print(f"Converted {input_parquet_path} to CSV format in {output_csv_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
