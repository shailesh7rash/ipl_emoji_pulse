# Sliding/tumbling 1-minute high-volume crowd mood meter.
# window("event_timestamp", "1 minute", "15 seconds").
# Sentiment Polarity => -1.00 (Extreme Negative) to +1.00 (Extreme Positive)
# Arousal Intensity => 0.10 (Calm) to 1.00 (Explosive Crowd Frenzy)


from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


@dp.table (
    name = "ipl_reaction.gold.last_1min_mood",
    comment = "Calculates aggregated crowd's mood of last 1 minute"
)
def last_1min_mood_gold() -> DataFrame:

    silver_enriched_emojis_df = spark.readStream.table("ipl_reaction.silver.enriched_emoji_reactions")

    gold_1min_df = (
        silver_enriched_emojis_df
            .withWatermark("event_timestamp", "2 minutes")
            .groupBy(
                    F.col("match_id"),
                    F.window(F.col("event_timestamp"), "1 minute", "15 seconds").alias("time_window")
                )
            .agg(
                    F.count("event_id").alias("total_reactions_1m"),
                    F.approx_count_distinct("user_id").alias("unique_active_fans_1m"),
                    F.round(F.avg("sentiment_polarity") * 100, 2).alias("mood_index_score"),
                    F.round(F.avg("arousal_intensity") * 100, 2).alias("crowd_hype_index"),
                    F.sum(F.when(F.col("mood_category") == "Euphoria", 1).otherwise(0)).alias("euphoria_taps"),
                    F.sum(F.when(F.col("mood_category") == "Heartbreak", 1).otherwise(0)).alias("heartbreak_taps"),
                    F.sum(F.when(F.col("mood_category") == "Shock", 1).otherwise(0)).alias("shock_taps"),
                    F.sum(F.when(F.col("mood_category") == "Appreciation", 1).otherwise(0)).alias("appreciation_taps"),
                    F.sum(F.when(F.col("mood_category") == "Amusement", 1).otherwise(0)).alias("amusement_taps"),
                    F.sum(F.when(F.col("mood_category") == "Frustration", 1).otherwise(0)).alias("frustration_taps"),
                    F.sum(F.when(F.col("mood_category") == "Hype", 1).otherwise(0)).alias("hype_taps"),
                    F.mode("emoji_symbol").alias("dominant_emoji")
                ).select(
                    "match_id",
                    F.col("time_window.start").alias("window_start"),
                    F.col("time_window.end").alias("window_end"),
                    "total_reactions_1m",
                    "unique_active_fans_1m",
                    "mood_index_score",
                    "crowd_hype_index",
                    "dominant_emoji",
                    "euphoria_taps",
                    "heartbreak_taps",
                    "shock_taps",
                    "appreciation_taps",
                    "amusement_taps",
                    "frustration_taps",
                    "hype_taps"
                    )
    )

    return gold_1min_df








