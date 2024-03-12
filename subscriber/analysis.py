from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

delta_df = spark.read.format("delta").load("delta_table")

distinct_signal_ts_per_day = delta_df.groupby("signal_date").agg(countDistinct("signal_ts").alias("distinct_signal_ts_per_day"))

delta_df.show()
distinct_signal_ts_per_day.show()