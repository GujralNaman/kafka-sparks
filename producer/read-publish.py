from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime

schema = StructType([
    StructField("DateTime", StringType(), True),
    StructField("LV_ActivePower", FloatType(), True),
    StructField("Wind_Speed", FloatType(), True),
    StructField("Theoretical_Power_Curve", FloatType(), True),
    StructField("Wind_Direction", FloatType(), True)
])

spark = SparkSession.builder \
    .appName("Read CSV with Headers") \
    .getOrCreate()

csv_df = spark.readStream.schema(schema).csv("/home/namangujral/Downloads/data", header=True)

query = csv_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

ds = csv_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "kafka-task") \
    .option("checkpointLocation", "temp/checkpoint/") \
    .start()

ds.awaitTermination()
