# Enriches emoji_reaction_silver with PostgreSQL dim_emoji + filters out bot spam (tap_streak > 15).
# Uses Stream-Static Broadcast Join where emoji_reaction_silver is the stream and dim_emoji is the batch cum static table.
# dim_emoji being static, it is broadcasted to all executors.
# Data Quality / Bot Guardrail: Drop scripted rapid-fire spam taps (tap_streak > 15 taps/3s)
# We have added Salt Key (0..7) to distribute single-match load evenly across Spark partitions (0 to 7 = 8 total partitions)
# This table will power 'Last-1-Minute Rolling Pulse' in Gold.

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


@dp.table (
    name = "ipl_reaction.silver.enriched_emoji_reactions",
    comment = "Enriched emoji stream with static emoji dim table"
)
@dp.expect_or_drop("valid_tap_streak", "tap_streak <= 15")
def emoji_reaction_silver() -> DataFrame:


    dim_emoji_silver_df = spark.read.table("ipl_reaction.silver.dim_emoji")
    emojireactions_df = spark.readStream.table("ipl_reaction.silver.emojireactions")

    enriched_emoji_reactions_df = (
                    emojireactions_df
                    .withWatermark("event_timestamp", "3 hours")
                    .dropDuplicatesWithinWatermark(["event_id"])
                    .join(F.broadcast(dim_emoji_silver_df), on="emoji_id", how="inner")
                    .withColumn("salt_key", (F.abs(F.hash(F.col("user_id"))) % 8).cast("int"))
                    .select(
                        "event_id", "match_id", "salt_key", "user_id", "session_id",
                        "supported_team", "user_city", "user_state", "device_type", "stream_quality",
                        "emoji_id", "emoji_symbol", "emoji_name", "mood_category",
                        F.col("sentiment_polarity").cast("double").alias("sentiment_polarity"),
                        F.col("arousal_intensity").cast("double").alias("arousal_intensity"),
                        "is_positive", "tap_streak", "event_timestamp"
                    )
    )

    return enriched_emoji_reactions_df



