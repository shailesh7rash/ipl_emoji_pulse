from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


@dp.table (
    name = "ipl_reaction.silver.dim_player",
    comment = "Parsed and cleaned player dimension in silver"
)
def dim_player_silver() -> DataFrame:
    bronze_dim_player_df = spark.readStream.table("ipl_reaction.bronze.dim_player")

    transformed_bronze_dim_player_df = ( bronze_dim_player_df
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    )
    return transformed_bronze_dim_player_df
    



