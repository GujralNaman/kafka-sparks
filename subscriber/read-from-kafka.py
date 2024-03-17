from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Define schema for the CSV data
schema = StructType([
    StructField("DateTime", StringType(), True),
    StructField("LV_ActivePower", FloatType(), True),
    StructField("Wind_Speed", FloatType(), True),
    StructField("Theoretical_Power_Curve", FloatType(), True),
    StructField("Wind_Direction", FloatType(), True)
])

# Read data from Kafka and apply schema
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-task") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Extracting date and time from DateTime
split_col = split(df['DateTime'], ' ')
time = concat_ws(":", split_col.getItem(3), lit("00"))
date = concat_ws("-", split_col.getItem(2), split_col.getItem(1), split_col.getItem(0))

# Define the schema for the delta table
delta_schema = StructType([
    StructField("signal_date", DateType(), True),
    StructField("signal_ts", TimestampType(), True),
    StructField("create_date", DateType(), True),
    StructField("create_ts", TimestampType(), True),
    StructField("signals", MapType(StringType(), StringType()), True)
])

# Prepare the DataFrame for writing to delta format
delta_df = df \
    .withColumn('signal_date', to_date(lit(date), 'yyyy-MM-dd')) \
    .withColumn('signal_ts', time) \
    .withColumn('create_date', current_date()) \
    .withColumn('create_ts', current_timestamp()) \
    .withColumn('signals', create_map(
        lit('LV_ActivePower'), col('LV_ActivePower'),
        lit('Wind_Speed'), col('Wind_Speed'),
        lit('Theoretical_Power_Curve'), col('Theoretical_Power_Curve'),
        lit('Wind_Direction'), col('Wind_Direction')
    )) \
    .select('signal_date', 'signal_ts', 'create_date', 'create_ts', 'signals')



# Write the data to delta format
query = delta_df \
    .writeStream \
    .format("delta") \
    .option("truncate", "false") \
    .outputMode("append") \
    .option("checkpointLocation", "temp") \
    .start("delta_table")

# Await termination
query.awaitTermination()
