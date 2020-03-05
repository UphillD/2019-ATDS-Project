from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
	# create spark session
	sparkSession = SparkSession.builder.appName("Q1_sql_parquet").getOrCreate()

	# import data
	trips = sparkSession.read.parquet("hdfs://master:9000/yellow_tripdata_1m.parquet")

	# create view with the relevant data
	trips = trips.selectExpr("tripID",
		"date_format(cast(startDate as Timestamp), 'HH') as HourOfDay",
		"(cast(cast(finishDate as Timestamp) as Double) - cast(cast(startDate as Timestamp) as Double)) / 60 as Duration")
	trips.createOrReplaceTempView("trips")

	# perform sql query
	output = sparkSession.sql("SELECT HourOfDay, AVG(Duration) AS AverageTripDuration \
		FROM trips \
		GROUP BY HourOfDay \
		ORDER BY HourOfDay")

	# format and export output
	output.write.format("csv").mode("overwrite").options(delimiter = '\t').save("hdfs://master:9000/Q1_sql_parquet")

	# stop spark session
	sparkSession.stop()
