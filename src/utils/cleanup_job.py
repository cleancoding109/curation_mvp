import argparse
from pyspark.sql import SparkSession

from utils.temp_table_cleanup import TempTableCleaner


def main():
    parser = argparse.ArgumentParser(description="Cleanup temp staging tables")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", default="temp")
    parser.add_argument("--older_than_hours", type=int, default=24)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("TempTableCleanup").getOrCreate()
    cleaner = TempTableCleaner(spark, args.catalog)
    dropped = cleaner.cleanup_old_temp_tables(args.schema, args.older_than_hours)
    print(f"Dropped {dropped} tables")


if __name__ == "__main__":
    main()
