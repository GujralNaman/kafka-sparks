from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
delta_df = spark.read.format("delta").load("delta_table")

delta_table = delta_df.withColumn("date-hour", date_format(col('signal_ts'), "yyyy-MM-dd HH"))

avg_signals_per_hour = delta_df.groupby("date-hour").agg(
    avg(col["signals.LV_ActivePower"]).alias("avg_LV_ActivePower"),
    avg(col["signals.Wind_Speed"]).alias("avg_Wind_Speed"),
    avg(col["signals.Theoretical_Power_Curve"]).alias("avg_Theoretical_Power_Curve"),
    avg(col["signals.Wind_Direction"]).alias("avg_Wind_Direction")
)

avg_signals_per_hour.show()
