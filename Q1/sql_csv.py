from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
	# create spark session
	sparkSession = SparkSession.builder.appName("Q1_sql_csv").getOrCreate()

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

	# insert the input data in the trip schema
	trips = sparkSession.read.schema(tripSchema).csv("hdfs://master:9000/yellow_tripdata_1m.csv")
	# create view with the relevant data
	trips = trips.selectExpr("tripID",
		"date_format(cast(startDate as timestamp), 'HH') as HourOfDay",
		"(cast(cast(finishDate as Timestamp) as Double) - cast(cast(startDate as Timestamp) as Double)) / 60 as Duration")
	trips.createOrReplaceTempView("trips")

	# perform SQL query
	output = sparkSession.sql("SELECT HourOfDay, AVG(Duration) AS AverageTripDuration \
		FROM trips \
		GROUP BY HourOfDay \
		ORDER BY HourOfDay")

	# manage and export output
	output.write.format("csv").mode("overwrite").options(delimiter = '\t').save("hdfs://master:9000/Q1_sql_csv")

	# stop spark session
	sparkSession.stop()
