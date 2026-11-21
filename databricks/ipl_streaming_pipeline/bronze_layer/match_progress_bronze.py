from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, TimestampType


@dp.table (
    name = "ipl_reaction.bronze.matchprogress",
    comment = "Ball to Ball Match progress JSON file data"
)
def match_progress_bronze() -> DataFrame:

    # Setting json file path for Autoloader
    json_files_source_path = '/Volumes/ipl_reaction/source/match_progress/match_progress_json_data/'

    # We will provide schema to Autoloader for readStream
    ball_schema = StructType([
        StructField("match_id", StringType(), False),
        StructField("match_date", StringType(), True),
        StructField("venue_name", StringType(), True),
        StructField("venue_city", StringType(), True),
        StructField("innings", IntegerType(), False),
        StructField("batting_team", StringType(), False),
        StructField("bowling_team", StringType(), False),
        StructField("match_phase", StringType(), False),
        StructField("over_number", IntegerType(), False),
        StructField("ball_in_over", IntegerType(), False),
        StructField("ball_id", StringType(), False),
        StructField("batter_id", StringType(), False),
        StructField("non_striker_id", StringType(), True),
        StructField("bowler_id", StringType(), False),
        StructField("runs_off_bat", IntegerType(), False),
        StructField("extras", IntegerType(), False),
        StructField("extra_type", StringType(), True),
        StructField("is_boundary", BooleanType(), False),
        StructField("is_wicket", BooleanType(), False),
        StructField("wicket_type", StringType(), True),
        StructField("dismissed_player_id", StringType(), True),
        StructField("ball_speed_kph", DoubleType(), True),
        StructField("cumulative_score", IntegerType(), False),
        StructField("cumulative_wickets", IntegerType(), False),
        StructField("target_runs", IntegerType(), True),
        StructField("balls_remaining", IntegerType(), False),
        StructField("ball_timestamp", TimestampType(), False)
    ])

    # Reading data from Volume directory containing JSON files
    matchprogress_stream = ( spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .schema(ball_schema)
                .load(json_files_source_path)
                .withColumn("source_file_path", F.col("_metadata.file_path"))
                .withColumn("ingestion_timestamp", F.current_timestamp())
        )
    
    
    return matchprogress_stream
    




