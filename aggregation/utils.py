from delta.tables import DeltaTable
from pyspark.sql.functions import DataFrame, col
from pyspark.sql import SparkSession
import json, logging


def get_map_from_json(path: str) -> dict:
    """
    Reads a JSON file from a given path and returns it as a dictionary.
    Includes robust error handling and logging.
    """
    logging.info(f"Reading json from file: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            rows = json.load(f)
        return rows
    except FileNotFoundError as e:
        logging.error(f"Mapping file not found at path: {path}", exc_info=True)
        raise e
    except json.JSONDecodeError as e:
        logging.error(f"Could not decode JSON from file: {path}", exc_info=True)
        raise e


def write_to_gold(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    primary_keys: list[str] = None,
    write_mode: str = "merge"
):
    """
    Writes a DataFrame to a gold Delta table using an "upsert" pattern.
    - If the table does not exist, it is created.
    - If the table exists and mode is 'overwrite', it is completely replaced.
    - If the table exists and mode is 'merge', the data is merged on primary keys.
    """
    full_table_name = f"gold.{table_name}"
    table_existed = spark.catalog.tableExists(full_table_name)

    # If table doesn't exist or overwrite mode -> overwrite
    if not table_existed or write_mode.lower() == 'overwrite':
        
        # --- FULL WRITE (CREATE OR OVERWRITE) ---
        if not table_existed:
            logging.info(f"Table '{full_table_name}' does not exist. Creating new table...")
        else:
            logging.info(f"Table '{full_table_name}' exists. Overwriting as requested...")

        warehouse_path = spark.conf.get("spark.sql.warehouse.dir")
        table_path = f"{warehouse_path}/{full_table_name.replace('.', '.db/')}"
        logging.info(f"  - Writing data to location: {table_path}")

        df.write \
          .mode("overwrite") \
          .format("delta") \
          .option("overwriteSchema", "true") \
          .save(table_path)

        if not table_existed:
            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info("  - Table successfully created and registered.")
        
        logging.info("Write/Create complete.")

    elif write_mode.lower() == 'merge':
        # --- MERGE (UPSERT) ---
        if not primary_keys:
            # Using logging.error before raising an exception
            logging.error("`primary_keys` must be provided for merge mode when the table exists.")
            raise ValueError("`primary_keys` must be provided for merge mode when the table exists.")
        
        # Fixed a small bug here: the f-string was missing the print/logging function
        logging.info(f"Table '{full_table_name}' exists. Merging new data...")
        
        delta_table = DeltaTable.forName(spark, full_table_name)
        
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
        
        (
            delta_table.alias("target")
            .merge(
                source=df.alias("source"),
                condition=merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logging.info("Merge complete.")
    
    else:
        logging.error(f"Invalid write_mode: '{write_mode}'. Must be 'merge' or 'overwrite'.")
        raise ValueError(f"Invalid write_mode: '{write_mode}'. Must be 'merge' or 'overwrite'.")


# GOLD_PATH = "/home/dottier/big_data/gold"

# def write_table_to_gold(
#     df: DataFrame,
#     table_name: str,
#     primary_keys: list[str], 
#     partition_cols: list[str] = None
# ):
#     table_path = f"{GOLD_PATH}/{table_name}"

#     print(f"--- Writing gold table: {table_name} ---")
#     print(f"  - Primary Keys: {primary_keys}")
#     print(f"  - Partition Columns: {partition_cols}")

#     if not DeltaTable.isDeltaTable(spark, table_path):
#         initial_writer = df.write.format("delta")
#         if partition_cols:
#             initial_writer = initial_writer.partitionBy(*partition_cols)
#         initial_writer.save(table_path)

#         print(f"  - Successfully created and wrote data to {table_name}.")
#         return


#     delta_table = DeltaTable.forPath(spark, table_path)
#     merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])

#     (
#         delta_table.alias("target")
#         .merge(
#             source=df.alias("source"),
#             condition=merge_condition
#         )
#         .whenMatchedUpdateAll()
#         .whenNotMatchedInsertAll()
#         .execute()
#     )
#     print(f"  - Merge complete for {table_name}.")