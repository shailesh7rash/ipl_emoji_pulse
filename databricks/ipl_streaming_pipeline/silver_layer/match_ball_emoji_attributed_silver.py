# Enriches matchprogress with PostgreSQL dim_player (both batter_id and bowler_id called 'Dimensional role playing').
# Uses Stream-Static Broadcast Join where matchprogress is the stream (JSON) and dim_player is the batch cum static table.
# We explode salt_key 0 to 7 so every ball matches all 8 emoji salt buckets in enriched_emoji_reactions (to be used in stream - stream join).
# We don't save this join (called match_progress_enriched_df) in any persisted table as we use this joined df to execute stream-stream join.


# Salted Stream-Stream Join
# Attributes every emoji reaction to the exact delivery (ball_id) that occurred within the preceding 14 seconds using key salting (0..7) to eliminate single-match partition skew.
# We join persisted enriched_emoji_reactions (partitioned with salt_key in 8 buckets) and temporarily created match_ball_progress above where each ball is duplicated 8 times for skew addressing.
# However each (duplicated) ball joins to set of 'divided' emojis partitioned across buckets which cumulatively gives emojis sent for that ball. Timeline to segregate emojis for particular ball is 14 seconds as each ball delivery cycle is of 15 seconds (match progress generator). 

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


@dp.table (
    name = "ipl_reaction.silver.ball_emoji_attributed",
    comment = "Each ball has emojis attributed to it. Each emoji is attributed to a particular ball based on time window of 14 seconds"
)
def ball_emoji_attributed_silver() -> DataFrame:

    match_progress_silver_df = spark.readStream.table("ipl_reaction.silver.matchprogress")
    dim_player_silver_df = spark.read.table("ipl_reaction.silver.dim_player")

    dim_batter_df = dim_player_silver_df.select(
            F.col("player_id").alias("batter_id"),
            F.col("player_name").alias("batter_name"),
            F.col("fan_popularity_tier").alias("batter_tier")
        )
    
    dim_bowler_df = dim_player_silver_df.select(
            F.col("player_id").alias("bowler_id"),
            F.col("player_name").alias("bowler_name"),
            F.col("fan_popularity_tier").alias("bowler_tier")
        )
    
    match_progress_enriched_df = (
                    match_progress_silver_df
                    .join(F.broadcast(dim_batter_df), on="batter_id", how="left")
                    .join(F.broadcast(dim_bowler_df), on="bowler_id", how="left")
                    .withColumn("salt_key", F.explode(F.array([F.lit(i) for i in range(8)])))
    )


    match_progress_enriched_WithWaterMark = match_progress_enriched_df.withWatermark("ball_timestamp", "2 minutes")
    enriched_emoji_reactions_df = spark.readStream.table("ipl_reaction.silver.enriched_emoji_reactions")
    enriched_emoji_reactions_WithWaterMark = enriched_emoji_reactions_df.withWatermark("event_timestamp", "2 minutes")

    match_ball_emoji_attributed = (
                enriched_emoji_reactions_WithWaterMark.alias("e")
                    .join(
                        match_progress_enriched_WithWaterMark.alias("b"),
                        on=F.expr("""
                            e.match_id = b.match_id AND
                            e.salt_key = b.salt_key AND
                            e.event_timestamp >= b.ball_timestamp AND
                            e.event_timestamp <= b.ball_timestamp + INTERVAL 14 SECONDS
                            """),
                        how="inner"
                    ).select(
                            F.col("e.event_id"),
                            F.col("e.match_id"),
                            F.col("b.innings"),
                            F.col("b.match_phase"),
                            F.col("b.over_number"),
                            F.col("b.ball_in_over"),
                            F.col("b.ball_id"),
                            F.col("b.batting_team"),
                            F.col("b.bowling_team"),
                            F.col("b.batter_name"),
                            F.col("b.batter_tier"),
                            F.col("b.bowler_name"),
                            F.col("b.bowler_tier"),
                            F.col("b.runs_off_bat"),
                            F.col("b.is_boundary"),
                            F.col("b.is_wicket"),
                            F.col("b.cumulative_score"),
                            F.col("b.cumulative_wickets"),
                            F.col("b.target_runs"),
                            F.col("b.balls_remaining"),
                            F.col("b.ball_timestamp"),
                            F.col("e.user_id"),
                            F.col("e.supported_team"),
                            F.col("e.user_city"),
                            F.col("e.user_state"),
                            F.col("e.device_type"),
                            F.col("e.emoji_symbol"),
                            F.col("e.mood_category"),
                            F.col("e.sentiment_polarity"),
                            F.col("e.arousal_intensity"),
                            F.col("e.tap_streak"),
                            F.col("e.event_timestamp"),
                            (F.unix_timestamp("e.event_timestamp") - F.unix_timestamp("b.ball_timestamp")).alias("reaction_lag_sec")
                        )
    )

    return match_ball_emoji_attributed

    







