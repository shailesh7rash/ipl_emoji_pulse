from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
import json


@dp.table (
    name = "ipl_reaction.bronze.emojireactions",
    comment = "Emoji reactions kafka stream data"
)
def emoji_reaction_bronze() -> DataFrame:

    secret_scope_name = "ipl-reaction-scope"
    key='kafka_connection_details'
    # Getting kafka config details
    kafka_connection_details_json = dbutils.secrets.get(scope = secret_scope_name, key = key)

    kafka_connection_details = json.loads(kafka_connection_details_json)

    CONFLUENT_BOOTSTRAP_SERVERS = kafka_connection_details['bootstrap_servers']
    KAFKA_TOPIC = kafka_connection_details['topic']
    CONFLUENT_API_KEY = kafka_connection_details['api_key']
    CONFLUENT_API_SECRET = kafka_connection_details['api_secret']
    # JAAS config
    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{CONFLUENT_API_KEY}" password="{CONFLUENT_API_SECRET}";'

    # Reading data from kafka stream
    emojireactions_stream = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", CONFLUENT_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", jaas_config)
                .option("startingOffsets", "earliest")
                .load()
                )
    
    # We need to parse binary key and value columns into regular string data type
    emojireactions_stream_parsed = emojireactions_stream.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
        F.col("timestampType"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

    return emojireactions_stream_parsed
    




