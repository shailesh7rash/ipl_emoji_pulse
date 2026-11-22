# Team Polarization - Real-time both team (here CSK vs. MI) fanbase mood clash on every ball.

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


@dp.materialized_view(
    name="ipl_reaction.gold.team_polarization",
    comment="Ball-by-ball fan polarization comparing supported teams (e.g., CSK vs MI) mood and dominant emojis"
)
def gold_team_polarization() -> DataFrame:
    attributed_df = spark.read.table("ipl_reaction.silver.ball_emoji_attributed")

    return (
        attributed_df
        .groupBy(
            "match_id",
            "innings",
            "over_number",
            "ball_id",
            "supported_team"
        )
        .agg(
            F.count("event_id").alias("fan_taps"),
            F.round(F.avg("sentiment_polarity") * 100, 2).alias("team_mood_index"),
            F.mode("emoji_symbol").alias("dominant_team_emoji")
        )
    )
