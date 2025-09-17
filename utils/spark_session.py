import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

HDFS_WAREHOUSE_PATH = os.getenv("HDFS_WAREHOUSE_PATH")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")


def get_spark(app_name="football_etl") -> SparkSession:
    """
    Initializes a SparkSession with configurations based on the execution mode.

    Reads the mode from the `SPARK_ENV` environment variable.
    Defaults to 'dev' if not set.
    'dev' mode is for local/notebook use.
    'prod' mode is for the containerized application.
    """
    # Read configuration from environment variables
    # This makes the function self-contained and easy to test.
    mode = os.getenv("SPARK_ENV", "dev") # Use an env var to control the mode
    builder = SparkSession.builder.appName(app_name)

    logging.info(f"Initializing Spark in '{mode}' mode.")

    if mode == "prod":
        if not HDFS_WAREHOUSE_PATH or not SPARK_MASTER:
            raise ValueError("HDFS_WAREHOUSE_PATH environment variable must be set for 'prod' mode.")

        METASTORE_PATH = "/app/spark-metastore" # Path inside the container
        DERBY_CONNECTION_STRING = f"jdbc:derby:;databaseName={METASTORE_PATH}/metastore_db;create=true"
        
        builder = (
            builder.config("spark.driver.host", SPARK_MASTER)
                   .config("spark.hadoop.dfs.replication", "1")
                   .config("spark.sql.warehouse.dir", HDFS_WAREHOUSE_PATH)
                   #.config("spark.sql.parquet.columnarReaderBatchSize", "512")
                   #.config("spark.sql.parquet.enableVectorizedReader", "false") # Workaround for certain Parquet issues
                   .config("javax.jdo.option.ConnectionURL", DERBY_CONNECTION_STRING)
                   .enableHiveSupport()
        )
    
    else:  # 'dev' mode (local notebook)
        # For local development, we point to a local directory.
        # Spark will automatically create a metastore_db inside this directory.
        builder = (
            builder.master("local[*]") # Common setting for local development
                    .config("spark.sql.warehouse.dir", "spark-warehouse-dev") \
                    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .enableHiveSupport()
        )

        dev_driver_memory = os.getenv("SPARK_DEV_DRIVER_MEMORY")
        if dev_driver_memory:
            logging.info(f"Applying developer override for driver memory: {dev_driver_memory}")
            builder = builder.config("spark.driver.memory", dev_driver_memory)

    return builder.getOrCreate()