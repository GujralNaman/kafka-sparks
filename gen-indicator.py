from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Read data from Delta Lake
delta_df = spark.read.format("delta").load("path_to_delta_table")

# Define generation indicator based on LV_ActivePower
delta_df = delta_df.withColumn("generation_indicator", 
    when(delta_df["LV_ActivePower"] < 200, "Low")
    .when((delta_df["LV_ActivePower"] >= 200) & (delta_df["LV_ActivePower"] < 600), "Medium")
    .when((delta_df["LV_ActivePower"] >= 600) & (delta_df["LV_ActivePower"] < 1000), "High")
    .when(delta_df["LV_ActivePower"] >= 1000, "Exceptional")
)

# Show the DataFrame
delta_df.show()
