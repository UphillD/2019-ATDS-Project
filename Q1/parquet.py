from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
	# create spark session
	sparkSession = SparkSession.builder.appName("Q1_parquet").getOrCreate()

	# create trip schema
	tripSchema = StructType([
		StructField("tripID", StringType(), False),
		StructField("startDate", StringType(), True),
		StructField("finishDate", StringType(), True),
		StructField("startLongitude", StringType(), True),
		StructField("startLatitude", StringType(), True),
		StructField("finishLongitude", StringType(), True),
		StructField("finishLatitude", StringType(), True),
		StructField("cost", StringType(), True)
	])

	# transform .csv trip file into .parquet file
	trips = sparkSession.read.schema(tripSchema).csv("hdfs://master:9000/yellow_tripdata_1m.csv")
	trips.write.mode("overwrite").parquet("hdfs://master:9000/yellow_tripdata_1m.parquet")

	# stop spark session
	sparkSession.stop()
