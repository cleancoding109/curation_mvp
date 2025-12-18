from datetime import datetime, timedelta
from pyspark.sql import SparkSession


class TempTableCleaner:
    """Utility to drop staging temp tables older than the cutoff."""

    def __init__(self, spark: SparkSession, catalog: str):
        self.spark = spark
        self.catalog = catalog

    def cleanup_old_temp_tables(self, schema: str = "temp", older_than_hours: int = 24) -> int:
        tables = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{schema}").collect()
        cutoff = datetime.now() - timedelta(hours=older_than_hours)
        dropped = 0

        for table in tables:
            if not table.tableName.startswith("staging_"):
                continue
            try:
                desc = self.spark.sql(
                    f"DESCRIBE EXTENDED {self.catalog}.{schema}.{table.tableName}"
                ).collect()
                created = next((row.data_type for row in desc if row.col_name == "Created Time"), None)
                if not created:
                    continue
                created_dt = datetime.strptime(created, "%a %b %d %H:%M:%S %Z %Y")
                if created_dt < cutoff:
                    self.spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{schema}.{table.tableName}")
                    dropped += 1
            except Exception:
                # Skip tables we cannot parse or drop
                continue

        return dropped
