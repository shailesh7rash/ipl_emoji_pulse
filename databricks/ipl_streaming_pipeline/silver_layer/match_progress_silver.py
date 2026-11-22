from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


@dp.table (
    name = "ipl_reaction.silver.matchprogress",
    comment = "Ball to Ball Match progress data enriched in Silver"
)
def match_progress_silver() -> DataFrame:

    bronze_matchprogress_df = spark.readStream.table("ipl_reaction.bronze.matchprogress")

    transformed_matchprogress_df = ( bronze_matchprogress_df
                            .withColumnRenamed("ingestion_timestamp", "bronze_ingestion_timestamp")
                            .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    )
    
    
    return transformed_matchprogress_df
    




