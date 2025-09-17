from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, input_file_name, col, to_timestamp, struct
) 
import os, logging
from utils.spark_session import get_spark
from delta.tables import DeltaTable
from configs.schemas import stage_schema, match_preview_schema, match_data_schema
from utils.io_utils import read_last_crawl_time
from crawler.utils import write_crawl_time
from utils.logging_utils import init_logging
from datetime import datetime, timezone
from pyspark.sql.utils import AnalysisException
from dotenv import load_dotenv

load_dotenv()

HDFS_BRONZE_BASE_PATH = os.getenv("HDFS_BRONZE_BASE_PATH")


def migrate_json_source(
    spark: SparkSession, 
    schema: StructType, 
    depth: int, 
    json_subdir: str, 
    target_json: str, 
    table_name: str,
    primary_keys: list[str],
    last_crawl_time: datetime,
    extract_partitions_from_path: bool = False,
    partition_by: list[str] | None = None
):
    """
    Reads JSON files and writes them to a bronze Delta table.

    - If 'last_crawl_time' is None, performs a full overwrite and registers the table if needed.
    - If 'last_crawl_time' is provided, reads only recently modified JSON files and MERGES them.
    """
    wildcard_path = "/".join(["*"] * depth)
    source_glob_path = f"{HDFS_BRONZE_BASE_PATH}/{json_subdir}/{wildcard_path}/{target_json}"
    db_name = "bronze"
    full_table_name = f"{db_name}.{table_name}"

    warehouse_path = spark.conf.get("spark.sql.warehouse.dir")
    table_path = f"{warehouse_path}/{db_name}.db/{table_name}"

    logging.info(f"--- Starting migration for bronze.{table_name} ---")
    
    try:
        # --- 1. Read the Source Data (Incremental or Full) ---
        reader = spark.read.schema(schema)

        if last_crawl_time:
            logging.info(f"Incremental mode: Reading JSON files modified after {last_crawl_time}...")
            reader = reader.option("modifiedAfter", last_crawl_time)
        else:
            logging.info("Full refresh mode: Reading all JSON files...")

        df = reader.json(source_glob_path)

        if df.rdd.isEmpty():
            logging.warning(f"No new or modified JSON files found for the given criteria. Nothing to do.")
            return
        else:
            record_count = df.count()
            logging.info(f"Successfully read {record_count} new/updated records.")

        # --- 2. Extract Partitions (If Needed) ---
        if extract_partitions_from_path:
            df = df.withColumn("filepath", input_file_name())
            path_regex = r"league=([^/]+)/season=([^/]+)/stage_id=([^/]+)(?:/match_id=([^/]+))?"
            df = df.withColumn("league", regexp_extract(col("filepath"), path_regex, 1)) \
                   .withColumn("season", regexp_extract(col("filepath"), path_regex, 2)) \
                   .drop("filepath")

        if table_name == "stage_info":
            df = df.withColumn("stageId_pk", col("data.stageId"))
            df = df.dropDuplicates(["stageId_pk"])
            df = df.drop("stageId_pk")

        final_df = df.withColumn(
            "metadata",
            struct(
                to_timestamp(col("metadata.crawled_at"), "yyyy-MM-dd HH:mm:ss").alias("crawled_at"),
            )
        )

    except AnalysisException as e:
        if "Path does not exist" in str(e):
            logging.warning(f"Source path not found or empty: {source_glob_path}. Skipping.")
            return
        else:
            logging.error(f"FATAL: Failed during READ phase for bronze.{table_name}.", exc_info=True)
            raise
    
    try:
        # --- 3. CREATE or MERGE LOGIC ---
        if not spark.catalog.tableExists(full_table_name):
            logging.info(f"Table '{full_table_name}' does not exist. Creating new external table...")

            writer = (
                final_df.write
                .mode("overwrite")
                .format("delta")
                .option("mergeSchema", "true")
            )

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            # Step 1: Save data to HDFS
            writer.save(table_path)

            # Step 2: Register in metastore
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            logging.info(f"Registering table '{full_table_name}' in Hive metastore.")
            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info(f"✅ Table '{full_table_name}' successfully created and registered.")

        else:
            logging.info(f"Table '{full_table_name}' exists. Merging new data...")

            delta_table = DeltaTable.forName(spark, full_table_name)
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
            
            (delta_table.alias("target")
                .merge(source=final_df.alias("source"), condition=merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logging.info(f"✅ Merge into '{full_table_name}' complete.")

    except Exception as e:
        logging.error(f"FATAL: Failed during WRITE phase for bronze.{table_name}.", exc_info=True)
        raise


def run_bronze_pipeline(spark: SparkSession):
    """
    Orchestrates the ingestion of raw JSON files from HDFS into Bronze Delta tables.
    This is the main entry point for this pipeline layer.
    """
    # Create the database if it doesn't exist. This is idempotent.
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    last_crawl_time = read_last_crawl_time()

    # Read the timestamp of the last successful crawl. This determines if we do an
    # incremental or full load.
    logging.info(f"Starting Bronze pipeline. Processing files modified after: {last_crawl_time}")

    # --- Define tasks in a list for clean, resilient execution ---
    migration_tasks = [
        {
            "schema": stage_schema, "depth": 3, "json_subdir": "stage_data", 
            "target_json": "stage_info.json", "table_name": "stage_info",
            "primary_keys": ["data.stageId"], "extract_partitions_from_path": True
        },
        {
            "schema": match_preview_schema, "depth": 4, "json_subdir": "match_data",
            "target_json": "match_preview.json", "table_name": "match_preview",
            "primary_keys": ["data.id"], "extract_partitions_from_path": False
        },
        {
            "schema": match_data_schema, "depth": 4, "json_subdir": "match_data",
            "target_json": "match_data.json", "table_name": "match_data",
            "primary_keys": ["matchId"], "extract_partitions_from_path": False
        }
    ]
    
    # --- Execute each task, allowing the pipeline to continue on single-table failure ---
    for task in migration_tasks:
        try:
            # Unpack the dictionary into function arguments
            migrate_json_source(spark=spark, **task, last_crawl_time=last_crawl_time)
        except Exception:
            # The detailed error is already logged inside migrate_json_source.
            # We log a high-level message here and continue to the next task.
            logging.error(f"Migration for table '{task['table_name']}' failed. Continuing to next task.")