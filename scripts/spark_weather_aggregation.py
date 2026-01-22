from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType, StringType, TimestampType

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather_transformed"
SPARK_MASTER = "spark://spark-master:7077"


def main():
    spark = SparkSession.builder \
        .appName("WeatherAggregation") \
        .master(SPARK_MASTER) \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("high_wind_alert", BooleanType(), True),
        StructField("time", StringType(), True)
    ])

    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
    parsed = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
    parsed = parsed.withColumn("event_time", col("time").cast(TimestampType()))

    agg = parsed.groupBy(
        window(col("event_time"), "1 minute")
    ).agg(
        avg("temperature").alias("avg_temp_c"),
        spark_sum(col("high_wind_alert").cast("int")).alias("alert_count")
    )

    agg.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
