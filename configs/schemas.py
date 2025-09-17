from pyspark.sql.types import (
    StructField, StructType, StringType, IntegerType,
    ArrayType, BooleanType, FloatType, TimestampType
)

stage_schema = StructType([
    StructField("metadata", StructType([
        StructField("crawled_at", StringType(), True),
    ]), True),
    
    StructField("data", StructType([
        StructField("stageId", IntegerType(), True),
        StructField("regionId", IntegerType(), True),
        StructField("regionName", StringType(), True),
        StructField("tournamentId", IntegerType(), True),
        StructField("tournamentName", StringType(), True),
        StructField("seasonId", IntegerType(), True),
        StructField("seasonName", StringType(), True),
        StructField("stageName", StringType(), True)
    ]), True)
])


match_preview_schema = StructType([
    StructField("metadata", StructType([
        StructField("crawled_at", StringType(), True),
    ]), True),
    StructField("data", StructType([
        StructField("stageId", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("homeTeamId", IntegerType(), True),
        StructField("homeTeamName", StringType(), True),
        StructField("homeTeamCountryName", StringType(), True),
        StructField("awayTeamId", IntegerType(), True),
        StructField("awayTeamName", StringType(), True),
        StructField("awayTeamCountryName", StringType(), True),
        StructField(("startTimeUtc"), TimestampType(), True)
    ]), True)
])


match_data_schema = StructType([
    StructField("metadata", StructType([
        StructField("crawled_at", StringType(), True),
    ]), True),
    
    StructField("matchId", IntegerType(), True),
    
    StructField("matchCentreData", StructType([
        StructField("attendance", IntegerType(), True),
        StructField("venueName", StringType(), True),
        StructField("score", StringType(), True),
        StructField("htScore", StringType(), True),
        StructField("ftScore", StringType(), True),
        StructField("etScore", StringType(), True),
        StructField("pkScore", StringType(), True),

        StructField("home", StructType([
            StructField("teamId", IntegerType(), True),
            StructField("players", ArrayType(
                StructType([
                    StructField("playerId", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("shirtNo", IntegerType(), True),
                    StructField("isFirstEleven", BooleanType(), True),
                    StructField("position", StringType(), True),
                ])
            ), True),
            StructField("formations", ArrayType(
                StructType([
                    StructField("formationName", StringType(), True),
                    StructField("playerIds", ArrayType(IntegerType(), True), True),
                    StructField("formationSlots", ArrayType(IntegerType(), True), True),
                    StructField("startMinuteExpanded", IntegerType(), True)
                ])
            ), True)
        ]), True),

        StructField("away", StructType([
            StructField("teamId", IntegerType(), True),
            StructField("players", ArrayType(
                StructType([
                    StructField("playerId", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("shirtNo", IntegerType(), True),
                    StructField("isFirstEleven", BooleanType(), True),
                    StructField("position", StringType(), True),
                ])
            ), True),
            StructField("formations", ArrayType(
                StructType([
                    StructField("formationName", StringType(), True),
                    StructField("playerIds", ArrayType(IntegerType(), True), True),
                    StructField("formationSlots", ArrayType(IntegerType(), True), True),
                    StructField("startMinuteExpanded", IntegerType(), True)
                ])
            ), True)
        ]), True),

        StructField("events", ArrayType(
            StructType([
                StructField("eventId", IntegerType(), True),
                StructField("minute", IntegerType(), True),
                StructField("second", IntegerType(), True),
                StructField("teamId", IntegerType(), True),
                StructField("playerId", IntegerType(), True),
                StructField("x", FloatType(), True),
                StructField("y", FloatType(), True),
                StructField("endX", FloatType(), True),
                StructField("endY", FloatType(), True),
                StructField("period", StructType([
                    StructField("value", IntegerType(), True),
                    StructField("displayName", StringType(), True),
                ]), True),
                StructField("type", StructType([
                    StructField("displayName", StringType(), True),
                ]), True),
                StructField("outcomeType", StructType([
                    StructField("value", IntegerType(), True),
                ]), True),
                StructField("qualifiers", ArrayType(
                    StructType([
                        StructField("type", StructType([
                            StructField("value", IntegerType(), True),
                            StructField("displayName", StringType(), True),
                        ]), True),
                        StructField("value", StringType(), True)
                    ])
                ), True),
                StructField("satisfiedEventsTypes", ArrayType(IntegerType(), True), True)
            ])
        ), True)
    ]), True)
])
