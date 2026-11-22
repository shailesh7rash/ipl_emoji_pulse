# Mood per ball and over - Granular ball-by-ball mood summary and entire over rollup.

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame


@dp.materialized_view(
    name="ipl_reaction.gold.ball_and_over_mood",
    comment="Ball-level and Over-level fan mood, hype, and exact match score metrics using GROUPING SETS"
)
def gold_ball_and_over_mood() -> DataFrame:
    return spark.sql("""
        WITH ball_level_base AS (
            SELECT
                match_id,
                innings,
                match_phase,
                over_number,
                ball_id,
                MAX(batting_team) AS batting_team,
                MAX(batter_name) AS batter_name,
                MAX(bowler_name) AS bowler_name,
                CAST(MAX(runs_off_bat) AS BIGINT) AS ball_runs,
                CAST(MAX(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS BIGINT) AS ball_wicket,
                COUNT(event_id) AS ball_emoji_count,
                SUM(sentiment_polarity) AS sum_sentiment_polarity,
                SUM(arousal_intensity) AS sum_arousal_intensity,
                SUM(reaction_lag_sec) AS sum_reaction_lag_sec
            FROM ipl_reaction.silver.ball_emoji_attributed
            GROUP BY
                match_id,
                innings,
                match_phase,
                over_number,
                ball_id
        ),
        grouped_metrics AS (
            SELECT
                match_id,
                innings,
                match_phase,
                over_number,
                COALESCE(ball_id, CONCAT('OVER_', CAST(over_number AS STRING), '_TOTAL')) AS granularity_key,
                CASE
                    WHEN ball_id IS NULL THEN 'OVER_SUMMARY'
                    ELSE 'BALL_LEVEL'
                END AS granularity_level,
                MAX(batting_team) AS batting_team,
                MAX(batter_name) AS primary_batter,
                MAX(bowler_name) AS primary_bowler,
                SUM(ball_runs) AS total_runs_in_scope,
                SUM(ball_wicket) AS wickets_in_scope,
                SUM(ball_emoji_count) AS total_emoji_reactions,
                ROUND((SUM(sum_sentiment_polarity) / NULLIF(SUM(ball_emoji_count), 0)) * 100, 2) AS avg_mood_score,
                ROUND((SUM(sum_arousal_intensity) / NULLIF(SUM(ball_emoji_count), 0)) * 100, 2) AS avg_hype_score,
                ROUND(SUM(sum_reaction_lag_sec) / NULLIF(SUM(ball_emoji_count), 0), 2) AS avg_fan_reaction_lag_sec
            FROM ball_level_base
            GROUP BY GROUPING SETS (
                (match_id, innings, match_phase, over_number, ball_id),
                (match_id, innings, match_phase, over_number)
            )
        ),
        emoji_counts AS (
            SELECT
                match_id,
                innings,
                match_phase,
                over_number,
                COALESCE(ball_id, CONCAT('OVER_', CAST(over_number AS STRING), '_TOTAL')) AS granularity_key,
                emoji_symbol,
                COUNT(event_id) AS emoji_freq
            FROM ipl_reaction.silver.ball_emoji_attributed
            GROUP BY GROUPING SETS (
                (match_id, innings, match_phase, over_number, ball_id, emoji_symbol),
                (match_id, innings, match_phase, over_number, emoji_symbol)
            )
        ),
        ranked_emojis AS (
            SELECT
                match_id,
                innings,
                match_phase,
                over_number,
                granularity_key,
                emoji_symbol AS top_emoji,
                ROW_NUMBER() OVER (
                    PARTITION BY match_id, innings, match_phase, over_number, granularity_key
                    ORDER BY emoji_freq DESC, emoji_symbol ASC
                ) AS rn
            FROM emoji_counts
        )
        SELECT
            g.match_id,
            g.innings,
            g.match_phase,
            g.over_number,
            g.granularity_key,
            g.granularity_level,
            g.batting_team,
            g.primary_batter,
            g.primary_bowler,
            g.total_runs_in_scope,
            g.wickets_in_scope,
            g.total_emoji_reactions,
            g.avg_mood_score,
            g.avg_hype_score,
            g.avg_fan_reaction_lag_sec,
            r.top_emoji,
            CURRENT_TIMESTAMP() AS updated_at
        FROM grouped_metrics g
        LEFT JOIN ranked_emojis r
            ON g.match_id = r.match_id
           AND g.innings = r.innings
           AND g.match_phase = r.match_phase
           AND g.over_number = r.over_number
           AND g.granularity_key = r.granularity_key
           AND r.rn = 1
    """)
