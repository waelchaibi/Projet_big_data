from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum as spark_sum, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType, StringType

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather_transformed"
SPARK_MASTER = "spark://spark-master:7077"
CHECKPOINT_DIR = "/opt/spark/work-dir/checkpoints/weather_agg"


def main():
    spark = SparkSession.builder \
        .appName("WeatherAggregation") \
        .master(SPARK_MASTER) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("high_wind_alert", BooleanType(), True),
        StructField("time", StringType(), True)
    ])

    # Streaming read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
    parsed = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
    # Example input: "2026-01-22T10:45"
    parsed = parsed.withColumn("event_time", to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm"))

    agg = parsed.groupBy(
        window(col("event_time"), "1 minute")
    ).agg(
        avg("temperature").alias("avg_temp_c"),
        spark_sum(col("high_wind_alert").cast("int")).alias("alert_count")
    )

    # Streaming output to console; you'll see continuous updates in logs.
    query = (
        agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
