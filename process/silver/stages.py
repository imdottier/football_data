import logging
from pyspark.sql import SparkSession, DataFrame
from process.cleaning_helpers import flatten_df
from utils.io_utils import write_to_silver


def write_stage_data(
    spark: SparkSession,
    new_stage_info_df: DataFrame,
    job_run_date: str
):
    """
    Reads NEW stage data from the bronze.stage_info Delta table, transforms it, 
    and merges it into the silver.dim_stages table.
    """
    logging.info("--- Starting Silver processing for dim_stages ---")

    try:
        # --- 2. Transformation Step ---
        transformed_df = flatten_df(
            new_stage_info_df.select(
                "data.*",
                "metadata.crawled_at",
                "league",
                "season"
            )
        ).dropDuplicates(["stage_id"])
        
        # --- 3. Write Data to the Silver Layer ---
        logging.info(f"Writing {transformed_df.count()} transformed records to silver.dim_stages...")

        success = write_to_silver(
            spark=spark,
            df=transformed_df,
            table_name="dim_stages",
            primary_keys=["stage_id"],
            partition_by=["load_date_hour"],
            load_date_hour=job_run_date
        )

        if not success:
            raise RuntimeError("write_to_silver function reported a failure.")
            
        logging.info("--- Successfully processed and wrote data for dim_stages ---")

    except Exception as e:
        logging.error(f"Failed during transformation or write for dim_stages. Error: {e}", exc_info=True)
        raise