from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, col

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Read data from Delta Lake
delta_df = spark.read.format("delta").load("delta_table")

# Extract hour from signal_ts
delta_df = delta_df.withColumn("hour", hour("signal_ts"))

# Group by hour and calculate average for each signal
avg_signals_per_hour = delta_df.groupby("hour").agg(
    avg(col("LV_ActivePower")).alias("avg_LV_ActivePower"),
    avg(col("Wind_Speed")).alias("avg_Wind_Speed"),
    avg(col("Theoretical_Power_Curve")).alias("avg_Theoretical_Power_Curve"),
    avg(col("Wind_Direction")).alias("avg_Wind_Direction")
)


# Show the result
avg_signals_per_hour.show()
