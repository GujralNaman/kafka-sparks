from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.getOrCreate()

got_df = spark.read.format("delta").load("delta_table")
got_df.show()