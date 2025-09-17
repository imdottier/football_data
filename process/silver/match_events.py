import logging, os
from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql.functions import (
    col, explode, transform, filter as spark_filter, lower,
    regexp_replace, when, monotonically_increasing_id,
    row_number, map_from_entries, collect_list, struct,
    element_at, broadcast
)
from process.cleaning_helpers import flatten_df
from process.utils import get_or_create_json_mapping, get_or_create_json_df
from utils.expression_utils import get_select_expressions
from utils.io_utils import write_to_silver

# Get directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.abspath(os.path.join(script_dir, "..", "..", "configs"))

QUALIFIER_MAPPING_PATH = os.path.join(config_dir, "qualifier_mapping.json")
EVENT_TYPE_MAPPING_PATH = os.path.join(config_dir, "event_type_mapping.json")


def write_match_events_data(
    spark: SparkSession, 
    new_match_data_df: DataFrame, 
    job_run_date: str
):
    """
    Orchestrates the creation of the silver.fct_match_events table using a
    pre-loaded DataFrame of new match data.
    """
    logging.info("--- Starting Silver processing for fct_match_events ---")
    
    exploded_df = None # For safe unpersisting in the finally block
    try:
        # --- 1. CORE TRANSFORMATION PIPELINE ---
        
        # The source DataFrame is now passed in as an argument.
        # The first step is to call our optimized explode function.
        exploded_df = extract_and_explode_events(new_match_data_df)
        
        # This mapping is no longer used here but will be helpful in the gold layer
        # Again no need to do so if ori json mapping still there
        get_or_create_json_mapping(
            spark=spark,
            path=QUALIFIER_MAPPING_PATH,
            df=generate_qualifier_mapping(exploded_df)
        )

        # Add qualifier names and ids to df for aggregation
        processed_qualifiers_df = process_event_qualifiers(exploded_df)
        
        # Also add _event_id to be used as PK along with match_id
        keyed_df = generate_surrogate_event_key(processed_qualifiers_df)
        
        # Then flatten df
        base_events_df = create_base_events_table(keyed_df)

        # --- 2. ENRICHMENT ---
        # Ori df only has a list of numbers
        # For easier agg in the next step, need to convert them to
        # array of strings here
        event_type_mapping_df = get_or_create_json_mapping(
            spark, path=EVENT_TYPE_MAPPING_PATH
        )
        if event_type_mapping_df is None:
            raise ValueError("Event type mapping is missing. Cannot proceed.")

        final_df = enrich_with_event_types(base_events_df, event_type_mapping_df)

        # --- 3. FINAL SELECTION & WRITE ---
        # For columns order
        final_select_expressions = get_select_expressions(layer="silver", table_name="fct_match_events")
        final_df_to_write = final_df.select(*final_select_expressions)

        success = write_to_silver(
            spark=spark,
            df=final_df_to_write,
            table_name="fct_match_events",
            primary_keys=["match_id", "_event_id"], # Assuming these are the correct keys
            partition_by=["load_date_hour"],
            load_date_hour=job_run_date
        )
        if not success:
            raise RuntimeError("Failed to write to silver.fct_match_events.")
            
        logging.info("Successfully wrote to silver.fct_match_events.")

    except Exception as e:
        logging.error("A critical error occurred while processing fct_match_events.", exc_info=True)
        raise
    finally:
        # Unpersist only the DataFrames created and cached *within this function*.
        if exploded_df is not None:
            exploded_df.unpersist()
            logging.info("Unpersisted the exploded events DataFrame.")


def extract_and_explode_events(raw_match_data_df: DataFrame) -> DataFrame:
    """
    Takes the raw match data DataFrame, selects only the necessary columns for events,
    and explodes the nested 'events' array into individual event rows.
    """
    logging.info("Transform: Selecting and exploding nested match events array...")

    # 1. Select only the match identifier and the events array to process.
    # This is a critical performance optimization (columnar pruning).
    events_subset_df = raw_match_data_df.select(
        col("matchId").alias("match_id"), # The top-level match identifier
        col("matchCentreData.events").alias("events") # The array we want to explode
    )

    # 2. Explode the events array into a new column named "event".
    # This creates a new row for each event in the array.
    exploded_df = events_subset_df.withColumn(
        "event", explode(col("events"))
    )
    
    # 3. Drop the original 'events' array column to keep the DataFrame clean.
    final_df = exploded_df.drop("events")
    
    return final_df


def process_event_qualifiers(exploded_events_df: DataFrame) -> DataFrame:
    """
    Parses the nested 'qualifiers' array into two new array columns:
    one for display names and one for values.
    """
    logging.info("Transform: Processing event qualifiers...")

    # The logic for extracting qualifier names and values is complex and perfect for a function.
    # Note: Using spark_filter to avoid conflict with Python's built-in filter.
    processed_df = (
        exploded_events_df
        .withColumn(
            "qualifiers_display_names",
            transform(
                spark_filter(col("event.qualifiers"), lambda q: q.value.isNull()),
                lambda q: lower(regexp_replace(q.type.displayName, r"([a-z])([A-Z])", r"$1_$2"))
            )
        )
        .withColumn(
            "qualifiers_values",
            transform(
                spark_filter(col("event.qualifiers"), lambda q: q.value.isNull()),
                lambda q: (q.type.value)
            )
        )
        .withColumn("event", col("event").dropFields("qualifiers"))
    )
    return processed_df


def generate_surrogate_event_key(events_df: DataFrame) -> DataFrame:
    """
    Adds a unique, sequential event ID for each match, preserving the original event order.
    """
    logging.info("Transform: Generating surrogate event keys...")
    
    # Using monotonically_increasing_id is correct for a temporary unique ID
    temp_df = events_df.withColumn("surrogate_event_id", monotonically_increasing_id())
    
    window_spec = Window.partitionBy("match_id").orderBy("surrogate_event_id")
    
    final_df = (
        temp_df.withColumn("_event_id", row_number().over(window_spec))
               .drop("surrogate_event_id")
    )
    return final_df


def create_base_events_table(processed_events_df: DataFrame) -> DataFrame:
    """
    Flattens the event struct, cleans column names, and adds the 'is_successful' flag.
    """
    logging.info("Transform: Creating the base events table...")

    # Columns to clean (values, not names)
    categorical_cols_to_clean = {"period_display_name", "type_display_name"}

    # Flatten the main event struct
    df_to_flatten = processed_events_df.select(
        col("match_id"),
        col("_event_id"),
        "event.*",
        col("qualifiers_display_names"),
        col("qualifiers_values"),
    )

    base_df = flatten_df(df_to_flatten, categorical_cols_to_clean)

    # Add derived columns and drop unnecessary ones
    final_base_df = base_df.withColumn(
        "is_successful", when(col("outcome_type_value") == 1, True).otherwise(False)
    ).drop("outcome_type_value")
    
    return final_base_df


def generate_qualifier_mapping(exploded_events_df: DataFrame) -> DataFrame:
    """Generates a mapping of qualifier names to qualifier IDs."""
    logging.info("Transform: Generating qualifier name to ID mapping...")
    
    mapping_df = (
        exploded_events_df
        .select(explode("event.qualifiers").alias("q"))
        .filter(col("q.value").isNull()) # Your logic for flags
        .select(
            col("q.type.value").alias("qualifier_id"),
            col("q.type.displayName").alias("qualifier_name_raw")
        )
        .distinct()
        .withColumn(
            "qualifier_name",
            lower(regexp_replace(col("qualifier_name_raw"), r"([a-z])([A-Z])", r"$1_$2"))
        )
        .select("qualifier_name", "qualifier_id")
    )
    return mapping_df


def enrich_with_event_types(base_events_df: DataFrame, event_type_mapping_df: DataFrame) -> DataFrame:
    """Enriches the events table by mapping satisfiedEventsTypes IDs to their names."""
    logging.info("Transform: Enriching events with satisfied event type names...")

    # Create a broadcastable lookup map for performance
    lookup_map = (
        event_type_mapping_df
        .groupBy()
        .agg(map_from_entries(collect_list(struct("id", "name"))).alias("map"))
    )

    enriched_df = (
        base_events_df
        .crossJoin(broadcast(lookup_map))
        .withColumn(
            "satisfied_events_types_names",
            transform(
                "satisfied_events_types",
                lambda x: element_at(col("map"), x)
            )
        )
        .drop("map")
    )
    return enriched_df