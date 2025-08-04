import traceback
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when
from pyspark.sql.utils import AnalysisException

# Map: folder name -> required columns
OUTPUT_PATHS = {
    "daily_by_country_device": ["date", "country", "device", "record_count", "sum_totalEmissions", "avg_totalEmissions"],
    "daily_by_domain_format": ["date", "domain", "format", "record_count", "sum_totalEmissions", "avg_totalEmissions"],
    "top_10_domains": ["domain", "total_emissions"],
    "emission_contributions": ["date", "domain", "pct_adSelection", "pct_creativeDist", "pct_mediaDist"],
    "daily_country_trends": ["date", "country", "total_emissions"],
    "avg_domain_coverage": ["format", "avg_measured_coverage"],
    "flagged_domains": ["domain", "total_emissions", "z_score", "is_outlier"]
}

BASE_PATH = "s3a://sample-bucket/temp/"

def check_output_folder(spark, folder_name, required_cols):
    try:
        path = BASE_PATH + folder_name
        print(f"\n🔍 Checking: {path}")
        df = spark.read.parquet(path)
        df.cache()

        # 1. Row count
        row_count = df.count()
        if row_count == 0:
            raise ValueError(f"❌ No rows in {folder_name}")
        print(f"✅ Row count > 0: {row_count} rows")

        # 2. Column check
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"❌ Missing columns in {folder_name}: {missing}")
        print(f"✅ Required columns present: {required_cols}")

        # 3. Null check
        null_counts = df.select([
            count(when(col(c).isNull() | isnan(c), c)).alias(c)
            for c in required_cols
        ]).collect()[0].asDict()

        nulls = {k: v for k, v in null_counts.items() if v > 0}
        if nulls:
            raise ValueError(f"❌ Nulls found in {folder_name}: {nulls}")
        print("✅ No nulls in required columns")

        print(f"🎉 Passed quality check for: {folder_name}")
        return True

    except AnalysisException as e:
        print(f"🔥 Failed to read parquet in {folder_name}: {e}")
        return False
    except Exception as e:
        print(f"🔥 Data quality check failed for {folder_name}")
        traceback.print_exc()
        return False


def main():
    try:
        spark = SparkSession.builder.appName("OutputDataQualityCheck").getOrCreate()

        all_passed = True
        for folder, cols in OUTPUT_PATHS.items():
            result = check_output_folder(spark, folder, cols)
            if not result:
                all_passed = False

        spark.stop()

        if not all_passed:
            print("❌ One or more output folders failed data quality checks.")
            sys.exit(1)
        else:
            print("✅ All output folders passed data quality checks.")

    except Exception as e:
        print("🔥 Unexpected failure in Spark session:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
