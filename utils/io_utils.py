import os, json, logging
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, col
from delta.tables import DeltaTable
from datetime import datetime
from process.cleaning_helpers import camel_to_snake
from dotenv import load_dotenv
from hdfs import HdfsError, InsecureClient
from datetime import timezone

load_dotenv()

HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "user")
HDFS_BRONZE_BASE_PATH = os.getenv("HDFS_BRONZE_BASE_PATH", "/user/user/bronze")

STAGE_PATH = os.path.join(HDFS_BRONZE_BASE_PATH, "stage_data")
MATCH_PATH = os.path.join(HDFS_BRONZE_BASE_PATH, "match_data")

# Ideally all the mapping should be merge (update + append)
# Or run everytime on whole dataset
# It will take a while, so I suggest just get the current mapping

# These mapping will be helpful for writing agg logic in gold
def get_or_create_json_df(spark: SparkSession, path: str, df_generator) -> DataFrame | None:
    """
    Retrieves a DataFrame from a local JSON file cache. If the file doesn't exist,
    it computes the DataFrame, writes it to the cache, and then returns it.
    
    Args:
        spark: The active SparkSession.
        path: The local file path for the JSON cache.
        df_generator: The DataFrame to compute and cache if the file doesn't exist.
                      This MUST be provided if the cache is expected to be created.

    Returns:
        The resulting DataFrame, or None if reading/writing fails.
    """
    # Write if not exists
    if not os.path.exists(path):
        logging.info(f"Cache file '{path}' not found. Generating and creating cache...")

        if df_generator is None:
            logging.error(f"Cache file '{path}' does not exist, and no DataFrame was provided to generate it.")
            return None

        try:
            df = df_generator()
            # Your original logic to collect and write
            rows = [row.asDict() for row in df.collect()]
            
            # Ensure the directory exists before writing
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(rows, f, indent=2, ensure_ascii=False)

            logging.info(f"Successfully wrote {len(rows)} rows to cache file '{path}'.")
            return df_generator # Return the original DF to avoid re-creating it

        except Exception as e:
            logging.error(f"Failed to create cache file at '{path}'. Error: {e}", exc_info=True)
            return None

    # Else read from json
    else:
        logging.info(f"Reading from cache file '{path}'...")
        try:
            with open(path, "r", encoding="utf-8") as f:
                rows = json.load(f)
            
            if not rows:
                logging.warning(f"Cache file '{path}' is empty. Returning an empty DataFrame.")
                return spark.createDataFrame([], schema=StructType([])) # Return empty DF with no schema
            
            # Recreate DataFrame from the loaded rows
            cached_df = spark.createDataFrame([Row(**row) for row in rows])
            return cached_df
            
        except (json.JSONDecodeError, IOError, OSError) as e:
            logging.error(f"Failed to read or parse cache file '{path}'. Error: {e}", exc_info=True)
            return None
    

def get_or_create_json_mapping(
    spark: SparkSession,
    path: str,
    df_generator=None
):
    if not os.path.exists(path):
        print(f"{path} doesn't exist. Creating...")

        if df_generator is None:
            print(f"No DataFrame generator provided to create mapping at '{path}'.")
            return None
        
        df = df_generator()
        rows = [row.asDict() for row in df.collect()]
        result = {row["qualifier_name"]: row["qualifier_id"] for row in rows}
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)

        print(f"Successfully written json to file {path}")

        return df

    else:
        print(f"Reading json from file {path}")
        with open(path, "r", encoding="utf-8") as f:
            rows = json.load(f)

        df = spark.createDataFrame(
            [Row(id=value, name=key) for key, value in rows.items()]
        )
        return df
    

# Create Delta table if not exists, else merge
def write_to_silver(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    primary_keys: list[str], # MERGE requires a list of keys to join on
    partition_by: list[str] | None = None,
    load_date_hour=None 
):
    """
    Writes a DataFrame to a silver Delta table, creating it as an EXTERNAL table
    or merging into it if it already exists.
    load_date_hour is only for partitioning the new data to avoid reprocessing
    the whole data in gold layer.
    """
    full_table_name = f"silver.{table_name}"

    try:
        df_to_write = df
        if partition_by and "load_date_hour" in partition_by and "load_date_hour" not in df.columns:
            df_to_write = df.withColumn("load_date_hour", lit(load_date_hour))

        # --- CREATE LOGIC ---
        if not spark.catalog.tableExists(full_table_name):
            logging.info(f"Table '{full_table_name}' does not exist. Creating new external table...")

            # Define the HDFS path for external table data
            warehouse_path = spark.conf.get("spark.sql.warehouse.dir")
            # Constructing physical path
            table_path = f"{warehouse_path}/{full_table_name.replace('.', '.db/')}"
            
            logging.info(f"Writing data files to: {table_path}")
            writer = df_to_write.write \
                .mode("overwrite") \
                .format("delta") \
                .option("mergeSchema", "true")

            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            # Step 1: Save the data to the specified HDFS path.
            writer.save(table_path)
            
            # Step 2: Create the table in the metastore, pointing to the external location.
            logging.info(f"Registering table '{full_table_name}' in metastore.")
            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info("Table successfully created and registered.")

        # --- MERGE LOGIC ---
        else:
            logging.info(f"Table '{full_table_name}' exists. Merging new data...")
            
            delta_table = DeltaTable.forName(spark, full_table_name)
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
            
            (delta_table.alias("target")
                .merge(source=df_to_write.alias("source"), condition=merge_condition)
                .whenMatchedUpdateAll() 
                .whenNotMatchedInsertAll()
                .execute()
            )
            logging.info(f"Merge into '{full_table_name}' complete.")
            
        return True # Indicate success

    except Exception as e:
        logging.error(f"Failed to write to table '{full_table_name}'. Error: {e}", exc_info=True)
        return False # Indicate failure


def json_to_df(
    spark: SparkSession,
    schema: StructType,
    crawled_after: datetime,
    path: str,
    glob_pattern: str | None = None
) -> DataFrame:
    """
    Reads JSON files from a given path that have been modified after a specific time.

    Args:
        spark: The active SparkSession.
        schema: The schema to apply to the JSON files.
        crawled_after: A datetime object. Only files modified after this time will be read.
        path: The base HDFS or local path to read from.
        glob_pattern: An optional glob pattern to filter files (e.g., "*.json").

    Returns:
        A DataFrame containing the data from the read files.
        Will raise an exception on failure.
    """
    try:
        # Format the timestamp as an ISO 8601 string
        timestamp_str = crawled_after.strftime("%Y-%m-%dT%H:%M:%S")
        
        logging.info(f"Attempting to read JSON files from path '{path}' modified after '{timestamp_str}'.")

        reader = (
            spark.read
            .schema(schema)
            .option("modifiedAfter", timestamp_str) 
        )

        if glob_pattern:
            logging.info(f"Applying glob filter: '{glob_pattern}'")
            reader = reader.option("pathGlobFilter", glob_pattern)
        
        df = reader.json(path)
        
        logging.info(f"Successfully read data into a DataFrame.")
        
        return df

    except Exception as e:
        logging.error(f"Failed to read JSON from path '{path}'. Error: {e}", exc_info=True)
        raise


def read_last_crawl_time():
    client = HdfsClient.get_client(hdfs_url=HDFS_URL, user=HDFS_USER)

    if not client:
        logging.error("HDFS client is not available. Cannot write file.")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    
    file_path = f"/user/{HDFS_USER}/bronze_raw/last_crawl_timestamp.txt"
    logging.info(f"Attempting to read crawl timestamp from HDFS path: {file_path}")
    
    try:
        # Use the hdfscli client's read method
        with client.read(file_path, encoding="utf-8") as reader:
            last_crawl_str = reader.read().strip()
        
        if not last_crawl_str:
            logging.warning(f"Timestamp file found at '{file_path}' but it is empty. Defaulting to epoch.")
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

        logging.info(f"Successfully read timestamp string: '{last_crawl_str}'")
        return datetime.strptime(last_crawl_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    except (FileNotFoundError, HdfsError):
        logging.warning(f"Could not find timestamp file at '{file_path}'. This is normal on the first run. Defaulting to epoch.")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading the crawl timestamp. Error: {e}", exc_info=True)
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def read_df(spark: SparkSession, layer: str, table_name: str, **kwargs) -> DataFrame:
    full_table_name = f"{layer}.{table_name}"

    try:
        df = spark.read.table(full_table_name)
        if "where" in kwargs:
            df = df.where(kwargs["where"])
        print(f"Successfully read {full_table_name}")
        return df
    except Exception as e:
        print(f"Error reading {full_table_name}: {e}")
        raise


def read_new_bronze_data(
    spark: SparkSession, 
    table_name: str, 
    last_crawl_time: datetime,
    should_cache: bool = False
) -> DataFrame | None:
    """
    Reads new records from a specified bronze Delta table based on crawl time.
    """
    logging.info(f"Reading new data from bronze.{table_name} crawled after {last_crawl_time}...")

    # Use your generic read_df helper
    df = read_df(spark, "bronze", table_name) 
    df = df.where(col("metadata.crawled_at") > last_crawl_time)
    # Check for empty data BEFORE caching to save resources.
    if df.rdd.isEmpty():
        logging.warning(f"No new data found in bronze.{table_name}. Returning None.")
        return None
    
    # Conditionally cache the DataFrame if it's going to be reused.
    if should_cache:
        df.cache()
        record_count = df.count() # Trigger cache and get count
        logging.info(f"Found and cached {record_count} new records from bronze.{table_name}.")
    else:
        logging.info(f"Found new records in bronze.{table_name}.")
        # We don't call .count() here to avoid an extra action if not caching.
    
    return df


class HdfsClient:
    """
    A singleton-like manager for the HDFS InsecureClient.
    Ensures that only one client instance is created and shared.
    """
    _instance = None

    @classmethod
    def get_client(cls, hdfs_url: str, user: str) -> InsecureClient | None:
        """
        Gets the shared HDFS client instance. Creates it on the first call.
        Returns None if the connection fails.
        """
        if cls._instance is None:
            logging.info(f"HDFS client not initialized. Creating new instance for URL '{hdfs_url}'...")
            try:
                # Create the new instance and store it in the class variable
                cls._instance = InsecureClient(hdfs_url, user=user)
                # A quick check to ensure the connection is valid
                # cls._instance.status('/')
                logging.info("HDFS client created and connected successfully.")
            except Exception as e:
                logging.critical(f"FATAL: Failed to connect to HDFS at '{hdfs_url}'. Error: {e}")
                cls._instance = None # Ensure it remains None on failure
        
        return cls._instance