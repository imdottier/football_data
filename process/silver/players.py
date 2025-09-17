import logging, os
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, explode, arrays_zip, row_number, broadcast
)
from utils.io_utils import write_to_silver, get_or_create_json_df
 

def write_player_data(
    spark: SparkSession,
    new_match_data_df: DataFrame, # <-- NEW: Accepts a DataFrame
    job_run_date: str
):
    """
    Orchestrates the creation of player-related tables in the Silver layer
    using a pre-loaded DataFrame of new match data,
    including dim_players and fct_match_player_participation
    """
    logging.info("--- Starting Silver processing for all Player tables ---")
    
    # We will only cache the intermediate DataFrames created within this function
    base_player_df = None
    try:
        # --- 1. PREPARE INTERMEDIATE DATA ---
        # The input 'new_match_data_df' is already the raw data we need.
        
        logging.info("Extracting and flattening player data...")
        base_player_df = extract_and_flatten_players(new_match_data_df)
        
        logging.info("Calculating granular player slot information...")
        granular_player_slots_df = calculate_granular_slots(new_match_data_df)

        # --- 2. GET OR CREATE THE POSITION MAPPING ---
        # basically inside local configs folder
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_dir = os.path.abspath(os.path.join(script_dir, "..", "..", "configs"))
        POSITION_MAPPING_PATH = os.path.join(config_dir, "position_mapping.json")

        # the gen_pos_map func shouldn't be called in action if you keep position_mapping.json      
        mapping_position_df = get_or_create_json_df(
            spark=spark,
            path=POSITION_MAPPING_PATH,
            df_generator=lambda: generate_position_mapping(granular_player_slots_df, base_player_df)
        )
        if mapping_position_df is None:
            raise ValueError("Could not load or generate position mapping DataFrame.")

        # --- 3. CALCULATE FINAL TABLES ---
        logging.info("Calculating final player dimension and fact tables...")
        # get the first position of a player in a match
        player_positions_df = calculate_player_positions(granular_player_slots_df, mapping_position_df)
        dim_players, fct_participation = create_final_player_tables(base_player_df, player_positions_df)

        # dim_players.show(5, truncate=False)
        # fct_participation.show(5, truncate=False)

        # --- 4. WRITE TABLES TO SILVER ---
        logging.info("Writing final tables to Silver layer...")
        # Including player_id and player_name, should have things like
        # dob or nationality but with the current crawl it's impossible
        # Need to integrate with another source 
        write_to_silver(
            spark=spark, df=dim_players, table_name="dim_players",
            primary_keys=["player_id"], partition_by=["load_date_hour"], load_date_hour=job_run_date
        )
        # Player participation in each match with position
        write_to_silver(
            spark=spark, df=fct_participation, table_name="fct_player_match_participation",
            primary_keys=["match_id", "team_id", "player_id"], partition_by=["load_date_hour"], load_date_hour=job_run_date
        )
        logging.info("Successfully wrote all player tables to Silver.")

    except Exception as e:
        logging.error("A critical error occurred while processing player tables.", exc_info=True)
        raise
    finally:
        # Clean up only the DataFrames cached *within this function*.
        # The source DataFrame will be unpersisted by the main orchestrator.
        if base_player_df is not None:
            base_player_df.unpersist()
            logging.info("Unpersisted the base player DataFrame.")



def extract_and_flatten_players(raw_match_data_df: DataFrame) -> DataFrame:
    """
    Reads raw match data, explodes the nested player arrays, and returns a
    single flattened DataFrame of all player participations (home and away).
    This serves as the base for both the dimension and fact tables.
    """
    logging.info("Transform: Extracting and flattening base player participation data...")

    def _extract_side(df: DataFrame, side: str) -> DataFrame:
        """Helper to explode the player list for either the home or away side."""
        return df.select(
            explode(f"matchCentreData.{side}.players").alias("player"),
            col(f"matchCentreData.{side}.teamId").alias("team_id"),
            col("matchId").alias("match_id"),
        )

    home_players = _extract_side(raw_match_data_df, "home")
    away_players = _extract_side(raw_match_data_df, "away")
    
    unioned_df = home_players.unionByName(away_players)

    # This flattened DataFrame contains all the info we need for downstream tasks.
    flattened_df = unioned_df.select(
        col("match_id"),
        col("team_id"),
        col("player.playerId").alias("player_id"),
        col("player.name").alias("player_name"),
        col("player.shirtNo").alias("shirt_no"),
        col("player.isFirstEleven").alias("is_starting_eleven"),
        col("player.position").alias("original_position") # Keep the original position for mapping
    ).filter(col("player_id").isNotNull())
    
    return flattened_df


def calculate_granular_slots(raw_match_data_df: DataFrame) -> DataFrame:
    """
    Takes raw match data and unnests the formation arrays to create a granular
    DataFrame of which player was in which slot at what time.
    """
    logging.info("Transform: Calculating granular player slot data from formations...")
    
    def _extract_formations(df: DataFrame, side: str) -> DataFrame:
        return df.select(
            explode(f"matchCentreData.{side}.formations").alias("formation"),
            col(f"matchCentreData.{side}.teamId").alias("team_id"),
            col("matchId").alias("match_id"),
        )
        
    home_f = _extract_formations(raw_match_data_df, "home")
    away_f = _extract_formations(raw_match_data_df, "away")
    
    granular_player_slots_df = (
        home_f.unionByName(away_f)
        .withColumn("pairs", arrays_zip("formation.formationSlots", "formation.playerIds"))
        .withColumn("pair", explode("pairs"))
        .select(
            "match_id", "team_id",
            col("formation.formationName").alias("formation_name"),
            col("formation.startMinuteExpanded").alias("start_minute"),
            col("pair.playerIds").alias("player_id"),
            col("pair.formationSlots").alias("slot_id")
        )
        .filter(col("slot_id") > 0)
    )
    return granular_player_slots_df


def generate_position_mapping(granular_player_slots_df: DataFrame, base_player_df: DataFrame) -> DataFrame:
    """
    Generates the formation_name/slot_id to position mapping based on starting players.
    This is your exact, correct logic.
    """
    logging.info("Transform: Generating formation slot to position mapping...")
    
    # Find the formations used at the start of matches
    formation_starter_slots_df = (
        granular_player_slots_df
        .filter(col("start_minute") == 0)
        .select("match_id", "player_id", "team_id", "formation_name", "slot_id")
        .dropDuplicates()
    )

    # Join with the base player data to get their originally scraped position
    mapping_position_df = (
        broadcast(formation_starter_slots_df)
        .join(
            base_player_df,
            ["match_id", "player_id", "team_id"],
        )
        .select("formation_name", "slot_id", "original_position")
        .withColumnRenamed("original_position", "position")
        .filter(col("position").isNotNull())
        .dropDuplicates(["formation_name", "slot_id"])
    )
    return mapping_position_df


def calculate_player_positions(granular_slots_df: DataFrame, mapping_position_df: DataFrame) -> DataFrame:
    """
    Calculates the primary position (GK) from formation_name + slot_id
    pair ("442", "1") for each player
    in each match using the generated mapping.
    If a player appears in multiple slots take the earliest one
    """
    logging.info("Transform: Calculating primary positions for players in each match...")
    
    window_spec = Window.partitionBy("match_id", "player_id").orderBy(col("start_minute").asc())
    
    primary_slot_df = (
        granular_slots_df
        .withColumn("appearance_rank", row_number().over(window_spec))
        .filter(col("appearance_rank") == 1)
        .select("match_id", "player_id", "team_id", "formation_name", "slot_id")
    )

    final_positions_df = primary_slot_df.join(
        broadcast(mapping_position_df),
        on=["formation_name", "slot_id"],
        how="left"
    ).select("match_id", "player_id", "team_id", "position")
    
    return final_positions_df


def create_final_player_tables(base_player_df: DataFrame, player_positions_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Takes the base player data and the calculated positions to create the final
    dimension (dim_players) and fact (fct_player_match_participation) tables.
    """
    logging.info("Transform: Creating final dim_players and fct_player_match_participation tables...")

    # Create dim_players
    dim_players_df = (
        base_player_df.select("player_id", "player_name")
                      .dropDuplicates(["player_id"])
    )

    # Create fct_player_match_participation by enriching the base data
    fct_participation_df = (
        base_player_df
        .join(player_positions_df, ["match_id", "player_id", "team_id"], "left")
        .select(
            "match_id", "team_id", "player_id",
            col("is_starting_eleven"),
            col("shirt_no"),
            col("position") # The final, calculated position
        )
        .fillna(False, subset=["is_starting_eleven"])
    )
    
    return dim_players_df, fct_participation_df