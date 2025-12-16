import os
import pyspark
from delta import configure_spark_with_delta_pip

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import col, to_timestamp, current_timestamp, when


def make_spark(app_name: str = "smartech_bronze_streaming"):
    builder = (
        pyspark.sql.SparkSession.builder
        .appName(app_name)
        # Activation Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    input_path = os.environ.get("INPUT_PATH", "data/input")
    bronze_path = os.environ.get("BRONZE_PATH", "data/output/bronze_output")
    checkpoint_path = os.environ.get("CHECKPOINT_PATH", "data/checkpoints/bronze_checkpoints")

    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("building", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
    ])

    spark = make_spark()

    # 1) Lecture streaming JSON (NDJSON: 1 ligne = 1 objet JSON)
    raw = (
        spark.readStream
        .schema(schema)
        .json(input_path)
    )

    # 2) Nettoyage / typage
    clean = (
        raw
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssX"))
        .drop("timestamp")
        .withColumn("ingest_time", current_timestamp())
    )

    # 3) Filtrage basique (valeurs plausibles)
    filtered = (
        clean
        .filter(col("device_id").isNotNull() & col("building").isNotNull() & col("type").isNotNull() & col("event_time").isNotNull())
        .filter(
            when(col("type") == "humidity", (col("value") >= 0) & (col("value") <= 100))
            .when(col("type") == "temperature", (col("value") >= -40) & (col("value") <= 85))
            .when(col("type") == "co2", (col("value") >= 0) & (col("value") <= 5000))
            .otherwise(False)
        )
    )

    # 4) Projection finale Bronze
    bronze_df = filtered.select(
        "event_time", "device_id", "building", "floor", "type", "value", "unit", "ingest_time"
    )

    # 5) Écriture Delta + checkpoint
    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(bronze_path)
    )

    print("✅ Streaming lancé.")
    print(f"📥 Input: {input_path}")
    print(f"🥉 Bronze (Delta): {bronze_path}")
    print(f"🧷 Checkpoint: {checkpoint_path}")
    print("➡️ Copie des fichiers JSON dans data/input/ pour simuler le flux.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
