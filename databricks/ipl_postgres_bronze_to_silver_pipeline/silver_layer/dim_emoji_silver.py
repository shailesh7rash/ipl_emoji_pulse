from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


@dp.table (
    name = "ipl_reaction.silver.dim_emoji",
    comment = "Parsed and cleaned emoji dimension in silver"
)
def dim_emoji_silver() -> DataFrame:
    bronze_dim_emoji_df = spark.readStream.table("ipl_reaction.bronze.dim_emoji")

    transformed_bronze_dim_emoji_df = ( bronze_dim_emoji_df
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    )
    return transformed_bronze_dim_emoji_df
    



