from pyspark.sql import SparkSession
import traceback
from pyspark.sql.utils import AnalysisException

def copy_dataset(spark, name):
    temp_path = f"s3a://sample-bucket/temp/{name}/"
    output_path = f"s3a://sample-bucket/output/{name}/"
    try:
        print(f"Copying {name} from temp → output")
        df = spark.read.parquet(temp_path)
        df.write.mode("overwrite").parquet(output_path)
    except Exception as e:
        print(f"Failed to copy {name}: {e}")
        raise

def main():
    try:
        spark = SparkSession.builder.appName("WriteOutputs").getOrCreate()

        datasets = [
            "daily_by_country_device",
            "daily_by_domain_format",
            "top_10_domains",
            "emission_contributions",
            "daily_country_trends",
            "avg_domain_coverage",
            "flagged_domains"
        ]

        for name in datasets:
            copy_dataset(spark, name)

        print("All outputs written to final /output/ directory.")

    except AnalysisException as e:
        print(f"Spark error while writing outputs: {e}")
        raise
    except Exception as e:
        print("Unexpected error in write_outputs:")
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
