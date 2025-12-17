import os
import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, lower, trim, when

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "iot_smartech")

silver_path = os.environ.get("SILVER_PATH", "data/output/silver_delta")
checkpoint_path = os.environ.get("CHECKPOINT_PATH", "data/checkpoints/silver_kafka")

iot_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("building", StringType(), True),
    StructField("floor", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
])

def make_spark():
    spark_version = pyspark.__version__
    extra_packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}",
        f"org.apache.spark:spark-token-provider-kafka-0-10_2.13:{spark_version}",
    ]

    builder = (
        pyspark.sql.SparkSession.builder
        .appName("smartech_silver_kafka_streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

spark = make_spark()

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")  # "earliest" si on veut reprendre du début
    .load()
)

json_df = kafka_df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_time"),
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("json_str"),
)

parsed = (
    json_df
    .withColumn("data", from_json(col("json_str"), iot_schema))
    .filter(col("data").isNotNull())
    .select("topic","partition","offset","kafka_time","kafka_key", col("data.*"))
)

clean = (
    parsed
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssX"))
    .drop("timestamp")
    .withColumn("type", lower(trim(col("type"))))
    .withColumn("ingest_time", current_timestamp())
    .filter(col("device_id").isNotNull() & col("building").isNotNull() & col("event_time").isNotNull())
    .filter(
        when(col("type") == "humidity", (col("value") >= 0) & (col("value") <= 100))
        .when(col("type") == "temperature", (col("value") >= -40) & (col("value") <= 85))
        .when(col("type") == "co2", (col("value") >= 0) & (col("value") <= 5000))
        .otherwise(False)
    )
)

silver_df = clean.select(
    "event_time","device_id","building","floor","type","value","unit",
    "ingest_time","topic","partition","offset","kafka_time","kafka_key"
)

q = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(silver_path)
)

print("✅ Spark consumer lancé")
print("Kafka:", BOOTSTRAP, "Topic:", TOPIC)
print("Silver:", silver_path)
print("Checkpoint:", checkpoint_path)
q.awaitTermination()
