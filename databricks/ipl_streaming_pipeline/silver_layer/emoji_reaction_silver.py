from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, TimestampType
import json


@dp.table (
    name = "ipl_reaction.silver.emojireactions",
    comment = "Emoji reactions kafka stream data parsed, transformed, enriched and loaded into silver"
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
def emoji_reaction_silver() -> DataFrame:

    bronze_emojireactions_df = spark.readStream.table("ipl_reaction.bronze.emojireactions")

    emoji_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("match_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("supported_team", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("user_state", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("stream_quality", StringType(), True),
    StructField("emoji_id", StringType(), False),
    StructField("tap_streak", IntegerType(), False),
    StructField("event_timestamp", TimestampType(), False)
    ])

    transformed_emojireactions_df = bronze_emojireactions_df.select(
        F.from_json(F.col("value"), emoji_schema).alias("data"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
        ).select(
        F.col("data.*"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
            )

    return transformed_emojireactions_df



