from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

spark = SparkSession.builder \
    .appName("FleetTelemetry") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("Vehicle ID", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Speed (km/h)", DoubleType(), True)
])

df = spark.readStream \
    .option("header", True) \
    .schema(schema) \
    .csv("incoming")

df = df.withWatermark("Timestamp", "1 minute")

windowed_df = df.groupBy(
    window("Timestamp", "1 minute", "10 seconds"),
    "Vehicle ID"
).agg(
    avg("Speed (km/h)").alias("avg_speed")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("Vehicle ID"),
    col("avg_speed")
)
query = windowed_df.writeStream \
    .format("memory") \
    .queryName("speed_windows") \
    .outputMode("complete") \
    .start()

query.processAllAvailable()

ordered_df = spark.sql("""
SELECT
    window_start,
    `Vehicle ID`,
    avg_speed
FROM speed_windows
ORDER BY window_start
""")

window_spec = Window.orderBy("window_start")

alert_df = ordered_df.withColumn(
    "previous_avg",
    lag("avg_speed").over(window_spec)
)

alert_df = alert_df.withColumn(
    "speed_drop",
    col("previous_avg") - col("avg_speed")
)

alerts = alert_df.filter(col("speed_drop") > 20)

print("\n=== SPEED DROP ALERTS ===")
alerts.show(truncate=False)

query.awaitTermination()