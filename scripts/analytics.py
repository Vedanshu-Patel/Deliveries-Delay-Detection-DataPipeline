from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, when, broadcast, lit, round, date_trunc
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
# 1. Create SparkSession
spark = SparkSession.builder \
    .appName("DelayDetect") \
    .config("spark.sql.shuffle.partitions", 2) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema for incoming Kafka messages
delivery_schema = StructType([
    StructField("delivery_id", StringType()),
    StructField("city", StringType()),
    StructField("pickup_time", StringType()),
    StructField("driver_id", StringType()),
    StructField("expected_time", StringType()),
    StructField("delivery_time", StringType())
])

# 3. Read stream from Kafka
raw_deliveries = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "deliveries") \
    .option("startingOffsets", "earliest") \
    .load()

#| key | value | topic | partition | offset | timestamp | timestampType|

# 4. Deserialize JSON and select fields
deliveries = raw_deliveries.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), delivery_schema).alias("data")) \
    .select("data.*")
deliveries = deliveries.filter(col("pickup_time").isNotNull() & col("expected_time").isNotNull())

# 5. Convert string timestamps to TimestampType
deliveries = deliveries.withColumn("pickup_time", to_timestamp(col("pickup_time"))) \
                       .withColumn("expected_time", to_timestamp(col("expected_time"))) \
                       .withColumn("delivery_time", to_timestamp(col("delivery_time")))

# 6. Load Weather and Traffic static tables
weather_df = spark.read.option("header", "true").csv("data\weather_data.csv")
traffic_df = spark.read.option("header", "true").csv("data\traffic_data.csv")

weather_df = weather_df.withColumn("timestamp", to_timestamp(col("timestamp")))
traffic_df = traffic_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# 7. Enrich deliveries with weather and traffic data
deliveries = deliveries.withColumn("pickup_hour", date_trunc("hour", col("pickup_time")))
weather_df = weather_df.withColumn("weather_hour", date_trunc("hour", col("timestamp"))) \
                       .select("city", "weather_hour", "condition")

traffic_df = traffic_df.withColumn("traffic_hour", date_trunc("hour", col("timestamp"))) \
                       .select("city", "traffic_hour", "traffic_level")

# Now join
enriched = deliveries \
    .join(broadcast(weather_df), 
          (deliveries.city == weather_df.city) & 
          (deliveries.pickup_hour == weather_df.weather_hour), 
          "left") \
    .drop(weather_df.city) \
    .drop(weather_df.weather_hour)

enriched = enriched \
    .join(broadcast(traffic_df), 
          (deliveries.city == traffic_df.city) & 
          (deliveries.pickup_hour == traffic_df.traffic_hour), 
          "left") \
    .drop(traffic_df.city) \
    .drop(traffic_df.traffic_hour)

#Mega table
#delivery_id|city|pickup_time|delivery_time|expected_time|pickup_hour|condition|traffic_level
enriched.printSchema()

# 8. Add "is_late" column
enriched = enriched.withColumn(
    "is_late",
    when(
        col("delivery_time").isNull() | col("expected_time").isNull(), lit(None)
    ).when(
        (col("delivery_time").cast("long") - col("expected_time").cast("long")) >= 7200,  # 2 hours = 7200 seconds
        lit(1)
    ).otherwise(
        lit(0)
    )
)

# 9. Calculate late delivery minutes and put them in categories
enriched = enriched.withColumn(
    "delay_minutes",
    when(
        col("delivery_time").isNull() | col("expected_time").isNull(), 
        lit(None)
    ).otherwise(
        round((col("delivery_time").cast("long") - col("expected_time").cast("long")) / 60.0, 1)
    )
)

enriched = enriched.withColumn(
    "delay_category",
    when(col("delay_minutes").isNull(), lit(None))
    .when(col("delay_minutes") < 0, lit("Early"))
    .when(col("delay_minutes") < 30, lit("On Time"))
    .when(col("delay_minutes") < 120, lit("Minor Delay"))
    .otherwise(lit("Major Delay"))
)

# 10. Create Normalized Scores for traffic, weather and delay
enriched = enriched.withColumn(
    "delay_score",
    when(col("delay_minutes") <= 0, lit(0.0))
    .otherwise(when(col("delay_minutes") >= 120, lit(1.0))
                .otherwise(col("delay_minutes") / 120))
)

enriched = enriched.withColumn(
    "traffic_score",
    (lit(4) - col("traffic_level")) / lit(3)
)

enriched = enriched.withColumn(
    "weather_score",
    when(col("condition") == "clear", lit(1.0))
    .when(col("condition") == "clouds", lit(0.7))
    .when(col("condition") == "fog", lit(0.4))
    .when(col("condition") == "rain", lit(0.2))
    .otherwise(lit(0.5))  # fallback in case of unknown weather
)

# 11. Weighted sum to calculate driver fault
enriched = enriched.withColumn(
    "driver_fault_percentage",
    ((col("delay_score") * 0.5) +
     (col("traffic_score") * 0.30) +
     (col("weather_score") * 0.20)) * 100
) 

# 12. Write final output to parquet files
output_path = "path to output folder"
checkpoint_path = "path to checkpoint folder"

query = enriched.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="100 seconds") \
    .start()

query.awaitTermination()
