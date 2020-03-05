from pyspark.sql import SparkSession
from pyspark.sql.types import *

# note: this assumes that you have already executed parquet.py in the Q1 folder to create the trip parquet files
if __name__ == "__main__":
	# create spark session
	sparkSession = SparkSession.builder.appName("Q2_parquet").getOrCreate()

	# create vendor schema
	vendorSchema = StructType([
		StructField("tripID", StringType(), False),
		StructField("vendorID", StringType(), True)
	])

	# transform .csv vendor file to .parquet file
	vendors = sparkSession.read.schema(vendorSchema).csv("hdfs://master:9000/yellow_tripvendors_1m.csv")
	vendors.write.mode("overwrite").parquet("hdfs://master:9000/yellow_tripvendors_1m.parquet")

	# stop spark session
	sparkSession.stop()
