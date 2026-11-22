# Regional mood heatmap - State & city-level fan sentiment across India (Maharashtra vs. Tamil Nadu vs. Karnataka).

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


@dp.table(
    name="ipl_reaction.gold.regional_mood_heatmap",
    comment="1-minute tumbling regional fan mood and dominant emoji heatmap across Indian states and cities"
)
def gold_regional_mood_heatmap() -> DataFrame:
    df_silver_emojis = spark.readStream.table("ipl_reaction.silver.enriched_emoji_reactions")

    return (
        df_silver_emojis
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            "match_id",
            F.window("event_timestamp", "1 minute").alias("w"),
            "user_state",
            "user_city"
        )
        .agg(
            F.count("event_id").alias("regional_taps"),
            F.round(F.avg("sentiment_polarity") * 100, 2).alias("regional_mood_index"),
            F.mode("emoji_symbol").alias("regional_top_emoji")
        )
        .select(
            "match_id",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "user_state",
            "user_city",
            "regional_taps",
            "regional_mood_index",
            "regional_top_emoji"
        )
    )

