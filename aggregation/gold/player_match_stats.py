import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, array_contains, struct, when, count, last,
    lit, coalesce, greatest, least, lag, lead
)
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

from utils.expression_utils import (
    get_expressions, get_select_expressions, get_group_by_cols, get_derived_expressions
)


# Simple logic function to calculate minutes played
# Has some loopholes can be improved
def get_minutes_played_df(
    participation: DataFrame,
    event: DataFrame,
    qualifier_map: dict
) -> DataFrame:
    """
    Calculates the minutes played for each player in each match.

    This function enriches player participation data with calculated playing time,
    derived from substitution and card events, and capped by the official match duration.
    It assumes the input 'event' DataFrame is cached by the caller if it is used multiple times.

    :param participation: DataFrame containing player participation info (is_starting_eleven, etc.).
    :param event: DataFrame containing detailed match events.
    :param qualifier_map: A Python dictionary mapping qualifier names to their integer IDs.
    :return: A DataFrame with minutes played for each player-match combination.
    """
    logging.info("--- Starting calculation for player minutes played ---")
    
    try:
        # --- 1. Input Validation ---
        required_event_cols = ["match_id", "player_id", "type_display_name", "minute", "period_value", "qualifiers_values"]
        required_participation_cols = ["match_id", "player_id", "is_starting_eleven"]
        
        for c in required_event_cols:
            if c not in event.columns:
                raise ValueError(f"Input 'event' DataFrame is missing required column: '{c}'")
        for c in required_participation_cols:
            if c not in participation.columns:
                raise ValueError(f"Input 'participation' DataFrame is missing required column: '{c}'")

        # --- 2. Calculate Substitution and Red Card Timings ---
        logging.info("Step 1/4: Identifying substitution and red card event timings...")
        event_with_subs = (
            event
            .withColumn("start_minute", when(col("type_display_name") == "substitution_on", col("minute") + lit(1)))
            .withColumn("end_minute", when(
                (col("type_display_name") == "substitution_off") |
                (array_contains(col("qualifiers_values"), qualifier_map["second_yellow"])) |
                (array_contains(col("qualifiers_values"), qualifier_map["red"])),
                col("minute") + lit(1)
            ))
        )

        player_match_summary_df = event_with_subs.groupBy("match_id", "player_id").agg(
            sf.min("start_minute").alias("sub_on_minute"),
            sf.max("end_minute").alias("sub_off_minute")
        )

        # --- 3. Calculate Official Match Durations ---
        logging.info("Step 2/4: Calculating official match durations (90 or 120 mins)...")
        match_end_times_df = event.filter(
            col("period_value") < 5 # Ignore Penalties
        ).groupBy("match_id").agg(
            sf.max("minute").alias("actual_match_end_minute"),
            sf.max("period_value").alias("max_period")
        ).withColumn(
            "official_match_duration",
            when(col("max_period") > 2, lit(120)).otherwise(lit(90))
        )

        # --- 4. Join and Resolve Final Timings ---
        logging.info("Step 3/4: Joining participation data with event timings...")
        participation_with_minutes = (
            participation
            .join(player_match_summary_df, ["match_id", "player_id"], "left")
            .join(match_end_times_df, "match_id", "left")

            # filter out player who didn't play
            .filter(col("is_starting_eleven") | col("sub_on_minute").isNotNull())
        )

        logging.info("Step 4/4: Resolving final start/end times and calculating total minutes played...")
        final_df = (
            participation_with_minutes
            # Resolve non subbed in/out player stats
            .withColumn("player_start_minute", when(col("is_starting_eleven"), lit(0)).otherwise(col("sub_on_minute")))
            .withColumn("player_end_minute", coalesce(col("sub_off_minute"), col("actual_match_end_minute")))
            
            # 1 <= minutes_played <= 90 or 120
            .withColumn("minutes_played", greatest(lit(1), least(col("player_end_minute") - col("player_start_minute"), col("official_match_duration"))))
            .select(
                "match_id",
                "player_id",
                "team_id",
                "shirt_no",
                "is_starting_eleven",
                "position",
                "minutes_played"
            )
        )
        
        logging.info("--- Successfully calculated player minutes played ---")
        return final_df

    except Exception as e:
        logging.error(f"Failed during calculation of player minutes played. Error: {e}", exc_info=True)
        raise


def get_corner_stats(
    event: DataFrame,
    qualifier_map: dict,  
) -> DataFrame:
    """
    Calculates corner-related statistics for each player in each match.

    This function identifies corner takers using a window function and then aggregates
    the total corners taken and the number of those corners that led to a goal.

    :param event: A DataFrame of detailed match events, assumed to be cached by the caller.
    :param qualifier_map: A Python dictionary mapping qualifier names to their integer IDs.
    :return: A DataFrame with player-specific corner stats per match.
    """
    logging.info("--- Starting calculation for corner statistics ---")
    
    try:
        # --- 1. Input Validation ---
        required_cols = ["match_id", "_event_id", "qualifiers_values", "player_id", "type_display_name"]
        required_keys = ["corner_taken", "from_corner"]
        
        for c in required_cols:
            if c not in event.columns:
                raise ValueError(f"Input 'event' DataFrame is missing required column: '{c}'")
        for k in required_keys:
            if k not in qualifier_map.keys():
                raise KeyError(f"Input 'qualifier_map' dictionary is missing required key: '{k}'")

        # --- 2. Identify Corner Takers using a Window Function ---
        logging.info("Step 1/2: Attributing subsequent events to the last known corner taker...")
        
        # Define a window partitioned by match and ordered by event, to track event sequence
        w = Window.partitionBy("match_id").orderBy("_event_id")

        # Create 'corner_taker' column to track how many corners each player took
        # Then, use last(ignorenulls=True) to propagate that player's ID forward 
        # to all subsequent events until the next corner is taken.
        event_with_corner_flags_df = (
            event
            .withColumn(
                "corner_taker", 
                when(array_contains(col("qualifiers_values"), qualifier_map["corner_taken"]), col("player_id"))
            )
            .withColumn(
                "current_corner_taker", 
                last("corner_taker", ignorenulls=True).over(w)
            )
        )

        # --- 3. Aggregate Stats per Player ---
        logging.info("Step 2/2: Grouping by corner taker to aggregate final statistics...")
        
        # Group by the propagated corner taker ID to aggregate stats for that player.
        final_df = (
            event_with_corner_flags_df
            .groupBy("match_id", "current_corner_taker")
            .agg(
                # count('corner_taker') correctly counts only the non-null rows where a corner was taken.
                count("corner_taker").alias("corners_taken"),
                # Conditionally count goals that occurred following a corner
                # Has to contains from_corner field in qualifiers_values
                count(
                    when(
                        (array_contains(col("qualifiers_values"), qualifier_map["from_corner"])) &
                        (col("type_display_name") == "goal"),
                        True
                    )
                ).alias("corners_leading_to_goal")
            )
            .withColumnRenamed("current_corner_taker", "player_id")
            .filter(col("player_id").isNotNull()) # Remove rows where no corners were ever taken
        )
        
        logging.info("--- Successfully calculated corner statistics ---")
        return final_df

    except Exception as e:
        logging.error(f"Failed during calculation of corner statistics. Error: {e}", exc_info=True)
        raise


def add_flags(
    event: DataFrame, 
    qualifier_map: dict,
    event_type_map: dict
) -> DataFrame:
    """
    Adds various derived feature flags to the event data, indicating
    progressive passes, carries, and other game-related insights.

    This function transforms the base event data by creating numerous new
    columns based on qualifier and event type mappings.

    :param event: DataFrame of match events.
    :param qualifier_map: Mapping of qualifier names to integer IDs.
    :param event_type_map: Mapping of event type names to integer IDs.
    :return: DataFrame with added feature flags.
    """
    logging.info("--- Starting to add derived feature flags to event data ---")

    try:
        # --- 1. Input Validation ---
        required_cols = ["match_id", "_event_id", "qualifiers_values", "satisfied_events_types",
                         "type_display_name", "minute", "second", "x", "y", "end_x", "end_y", "team_id", "player_id", "period_value"]

        for c in required_cols:
            if c not in event.columns:
                raise ValueError(f"Input 'event' DataFrame is missing required column: '{c}'")
        if not qualifier_map or not event_type_map:
            raise ValueError("Qualifier and event type maps must be non-empty dictionaries.")

        # --- 2. Preparation: Create Flag Expressions ---
        logging.info("Step 1/4: Creating base flag expressions from mappings...")
        qualifier_names = set(qualifier_map.keys())
        event_type_names = set(event_type_map.keys())

        overlapping_names = qualifier_names.intersection(event_type_names)
        qualifier_only_names = qualifier_names - overlapping_names
        event_type_only_names = event_type_names - overlapping_names

        qual_only_exprs = [
            array_contains(col("qualifiers_values"), qualifier_map[name]).alias(f"is_{name}")
            for name in qualifier_only_names
        ]
        event_only_exprs = [
            array_contains(col("satisfied_events_types"), event_type_map[name]).alias(f"is_{name}")
            for name in event_type_only_names
        ]
        overlapping_exprs = [
            (array_contains(col("qualifiers_values"), qualifier_map[name]) |
             array_contains(col("satisfied_events_types"), event_type_map[name])).alias(f"is_{name}")
            for name in overlapping_names
        ]

        all_flag_exprs = qual_only_exprs + event_only_exprs + overlapping_exprs

        logging.info(f"Creating base and custom expressions...")
        # create base expression then use them for custom exprs (simple sql expressions)
        base_exprs, custom_exprs = get_derived_expressions("silver", "fct_match_events")

        # --- 3. Add Base Flags and Derived Columns ---
        logging.info("Step 2/4: Adding initial flags and base expressions...")
        
        event_with_flags = (
            event
            .withColumn("all_flags", struct(*all_flag_exprs))
            .select("*", "all_flags.*")
            .drop("all_flags")
            .select("*", *base_exprs)
            .select("*", *custom_exprs)
        )
        

        # --- 4. Calculate Progressive Passes ---
        # These are somewhat detailed so I don't put it in config
        # Progressive Pass: at least 25% forward starting from the attacking 2/3rd
        logging.info("Step 3/4: Calculating progressive pass flags...")
        
        event_with_flags = (
            event_with_flags
            .withColumn(
                "is_progressive_25",
                when(col("end_x") - col("x") >= 25.0 - 1e-6, True).otherwise(False)
            ).withColumn(
                "is_attacking_two_thirds",
                when(col("is_mid_third") | col("is_final_third"), True).otherwise(False)
            ).withColumn(
                "is_progressive_pass",
                when(col("is_attacking_two_thirds") & col("is_progressive_25"), True).otherwise(False)
            )
        )
            

        # --- 5. Calculate Progressive Carries (Most Complex Logic) ---
        # Progressive Carry: a carry that starts from the attacking half,
        # and is at least 10 yards (~9.14m) towards opposing goal line
        # or any carry into the opposing box
        
        # Gods know how Opta gets it from the raw data so I create 
        # it from my guesswork. Result seems close enough.
        logging.info("Step 4/4: Calculating progressive carry flags...")
        
        # Constants for field size
        FIELD_LENGTH = 115
        FIELD_WIDTH = 74
        PEN_BOX_DEPTH_YARDS = 18
        PEN_BOX_WIDTH_YARDS = 44

        ten_yard_pct = 100 * 10 / FIELD_LENGTH

        # X (length) for attacking box
        pen_box_x_min = 100 - 100 * PEN_BOX_DEPTH_YARDS / FIELD_LENGTH
        pen_box_x_max = 100

        # Y (width), centered
        pen_box_y_min = (100 - 100 * PEN_BOX_WIDTH_YARDS / FIELD_WIDTH) / 2
        pen_box_y_max = 100 - pen_box_y_min

        # Window for event order: PARTITION BY match, ORDER BY time
        w = Window.partitionBy('match_id').orderBy('period_value', 'minute', 'second', '_event_id')

        # Get next event's player/team for carry assignment and coordinate flipping
        event_with_flags = (
            event_with_flags
            .withColumn('next_player_id', lead('player_id').over(w))
            .withColumn('next_team_id', lead('team_id').over(w))
            .withColumn('next_x', lead('x').over(w))
            .withColumn('next_y', lead('y').over(w))
            .withColumn('next_type', lead('type_display_name').over(w))
        )

        # Identify start triggers (pass, clearance, ball_recovery, aerial)
        is_pass = col('type_display_name') == 'pass'
        is_clearance = col('type_display_name') == 'clearance'
        is_ball_recovery = col('type_display_name') == 'ball_recovery'
        is_aerial = col('type_display_name') == 'aerial'

        # Carry into box check
        start_outside_box = ~(
            (col('carry_start_x') >= pen_box_x_min) &
            (col('carry_start_x') <= pen_box_x_max) &
            (col('carry_start_y') >= pen_box_y_min) &
            (col('carry_start_y') <= pen_box_y_max)
        )
        carry_enters_box = (
            start_outside_box &
            (col('x') >= pen_box_x_min) & (col('x') <= pen_box_x_max) &
            (col('y') >= pen_box_y_min) & (col('y') <= pen_box_y_max)
        )

        # Carry start location logic
        event_with_flags = event_with_flags.withColumn(
            'start_carry_x',
            when(is_pass | is_clearance, col('end_x'))
            .when(is_ball_recovery, col('x'))
            .when(is_aerial, col('x'))
        ).withColumn(
            'start_carry_y',
            when(is_pass | is_clearance, col('end_y'))
            .when(is_ball_recovery, col('y'))
            .when(is_aerial, col('y'))
        ).withColumn(
            'carry_team_id',
            when(is_pass | is_clearance | is_ball_recovery | is_aerial, col('team_id'))
        )

        event_with_flags = event_with_flags.withColumn('carry_start_x', lag('start_carry_x').over(w))
        event_with_flags = event_with_flags.withColumn('carry_start_y', lag('start_carry_y').over(w))
        event_with_flags = event_with_flags.withColumn('carry_team_id', lag('carry_team_id').over(w))

        # Only keep rows where the player/team matches the carry_team_id (i.e., the player is in possession)
        event_with_flags = event_with_flags.withColumn('is_in_carry', col('team_id') == col('carry_team_id'))

        # Calculate carry progression from starting point
        event_with_flags = event_with_flags.withColumn(
            'carry_progression',
            col('x') - col('carry_start_x')
        )

        event_with_flags = event_with_flags.withColumn(
            "is_progressive_carry",
            when(
                (
                    (col('carry_start_x') > 50) &
                    (col('carry_progression') > ten_yard_pct) &
                    (col('is_in_carry'))
                )
                | carry_enters_box, True
            ).otherwise(False)
        )
            
        logging.info("--- Successfully added derived feature flags to event data ---")
        return event_with_flags

    except Exception as e:
        logging.error(f"A critical error occurred while calculating derived flags. Error: {e}", exc_info=True)
        raise


def aggregate_player_stats(event_with_flags_df: DataFrame) -> DataFrame:
    """
    Aggregates the flagged event data to produce per-player, per-match statistics.
    """
    logging.info("--- Starting aggregation of player-match statistics ---")
    
    try:
        # --- 1. Input Validation ---
        if "is_goal" not in event_with_flags_df.columns:
            raise ValueError("Input DataFrame is missing expected flag columns. Did add_flags() run correctly?")

        # --- 2. Aggregation ---
        logging.info("Step 1/2: Grouping by player and match to aggregate stats...")
        pm_group_by_cols = get_group_by_cols("gold", "player_match_stats")
        pm_agg_expressions = get_expressions("gold", "player_match_stats")

        player_match_stats = (
            event_with_flags_df
            .groupBy(*pm_group_by_cols)
            .agg(*pm_agg_expressions)
        )

        # --- 3. Post-Aggregation Calculation (Possession %) ---
        # Possession is like a touch, aggregable
        # but not really meaningful outside a match context
        logging.info("Step 2/2: Calculating possession percentages...")
        w_match = Window.partitionBy("match_id")
        
        final_df = (
            player_match_stats
            .withColumn("pass_success_percentage", sf.round(col("accurate_passes") * 100 / col("total_passes"), 2))
            .withColumn("total_possession", sf.sum("possession").over(w_match))
            .withColumn("possession_percentage", sf.round(col("possession") * 100 / col("total_possession"), 2))
            .drop("total_possession")
        )
        
        logging.info("--- Successfully aggregated player-match statistics ---")
        return final_df

    except Exception as e:
        logging.error(f"Failed during player stat aggregation. Error: {e}", exc_info=True)
        raise


def enrich_player_stats(
    player_match_stats: DataFrame,
    player: DataFrame,
    match: DataFrame,
    participation_with_minutes: DataFrame,
    team: DataFrame,
    stage: DataFrame,
    corner_stats: DataFrame
) -> DataFrame:
    """
    Enriches the aggregated player stats with dimensional data (player names,
    team names, match info, etc.) through a series of joins.
    """
    logging.info("--- Starting enrichment of player statistics with dimensional data ---")
    
    try:
        player_match_stats_filtered = player_match_stats.filter(col("player_id").isNotNull())
        logging.info(f"Starting with {player_match_stats_filtered.count()} aggregated stat rows.")

        # --- 1. Core Joins and First Calculation ---
        logging.info("Performing core joins and calculating opponent details...")
        stats_with_opponent = (
            player_match_stats_filtered.alias("ps")
            .join(player.alias("pl"), "player_id", "left")
            .join(match.alias("m"), "match_id", "left")
            .join(participation_with_minutes.alias("pt"), ["player_id", "match_id"], "left")
            .withColumn("is_home", when(col("home_team_id") == col("pt.team_id"), True).otherwise(False))
            .withColumn("opposing_team_id", when(col("is_home"), col("away_team_id")).otherwise(col("home_team_id")))
        )

        # --- 2. Enrich with Team Names (Home and Opponent) ---
        # This is the most complex joined DataFrame. We will cache this one.
        stats_with_teams = (
            stats_with_opponent
            .join(team.alias("t1"), col("pt.team_id") == col("t1.team_id"), "left")
            .join(team.alias("t2"), col("opposing_team_id") == col("t2.team_id"), "left")
        )

        # --- 3. Final Calculations and Joins on Cached Data ---
        logging.info("Calculating final scores and joining remaining dimensions...")
        final_stats = (
            stats_with_teams
            .withColumn("team_score", when(col("is_home"), col("home_score")).otherwise(col("away_score")))
            .withColumn("opposing_team_score", when(col("is_home"), col("away_score")).otherwise(col("home_score")))
            .withColumn("is_clean_sheet", when(col("opposing_team_score") == 0, True).otherwise(False))
            .join(stage, "stage_id", "left")
            .join(corner_stats, ["player_id", "match_id"], "left")
        )

        # --- 4. Final Selection and Cleanup ---
        pm_select_expressions = get_select_expressions("gold", "player_match_stats")
        final_df = final_stats.select(*pm_select_expressions).fillna(0, subset=["corners_taken", "corners_leading_to_goal"])
        
        logging.info(f"--- Successfully enriched player stats. Final row count: {final_df.count()} ---")
        return final_df

    except Exception as e:
        logging.error(f"Failed during player stat enrichment. Error: {e}", exc_info=True)
        raise


def build_player_match_stats(
    event: DataFrame,
    player: DataFrame,
    match: DataFrame,
    participation: DataFrame,
    team: DataFrame,
    stage: DataFrame,
    qualifier_map: dict,
    event_type_map: dict
) -> DataFrame:
    """
    Constructs the final, enriched player-match statistics gold table.
    This function orchestrates all the necessary transformations and manages caching
    of the foundational 'event' DataFrame.
    """
    logging.info("--- Starting build of the gold.player_match_stats table ---")
    
    # The 'event' DataFrame is used multiple times, so we MUST cache it.
    # event.cache()
    # logging.info("Cached the foundational 'event' DataFrame for multiple downstream uses.")
    
    try:
        # --- 1. Derive Independent Metrics from Source Tables ---
        logging.info("Orchestrator: Calling 'get_minutes_played_df' to derive player participation times.")
        participation_with_minutes = get_minutes_played_df(
            participation, event, qualifier_map
        )
        logging.info("Orchestrator: Successfully derived minutes played.")

        logging.info("Orchestrator: Calling 'get_corner_stats' to derive corner statistics.")
        corner_stats = get_corner_stats(
            event, qualifier_map
        )
        logging.info("Orchestrator: Successfully derived corner stats.")

        # --- 2. Perform the Core Feature Engineering and Aggregation ---
        logging.info("Orchestrator: Calling 'add_flags' to create detailed event flags.")
        event_with_flags = add_flags(
            event, qualifier_map, event_type_map
        )
        logging.info("Orchestrator: Successfully created flagged event data.")

        logging.info("Orchestrator: Calling 'aggregate_player_stats' to summarize the flagged data.")
        player_match_stats = aggregate_player_stats(event_with_flags)
        logging.info("Orchestrator: Successfully aggregated player stats.")

        # --- 3. Enrich with all Dimensional Data ---
        logging.info("Orchestrator: Calling 'enrich_player_stats' to join all data sources.")
        enriched_player_stats = enrich_player_stats(
            player_match_stats,
            player,
            match,
            participation_with_minutes,
            team,
            stage,
            corner_stats
        )
        logging.info("Orchestrator: Successfully enriched the final dataset.")

        logging.info("--- ✅ Successfully built the final player_match_stats DataFrame ---")
        return enriched_player_stats
        
    except Exception as e:
        logging.error("A critical error occurred in the 'build_player_match_stats' orchestrator.", exc_info=True)
        raise
    finally:
        # --- 4. CLEANUP ---
        event.unpersist()
        logging.info("Unpersisted the 'event' DataFrame.")