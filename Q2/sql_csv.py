from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys
import datetime
import math

# trip duration calculator function
def durationCalc (startDate, finishDate):
	finishDateTmp = datetime.datetime.strptime(finishDate, '%Y-%m-%d %H:%M:%S')
	startDateTmp = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
	tripDuration = (finishDateTmp - startDateTmp).total_seconds() / 60

	return tripDuration

# trip distance calculator function
def distanceCalc (startLong, startLat, finishLong, finishLat):
	startLongTmp = float(startLong)
	startLatTmp = float(startLat)
	finishLongTmp = float(finishLong)
	finishLatTmp = float(finishLat)

	Df = finishLatTmp - startLatTmp
	Dl = finishLongTmp - startLongTmp
	a = math.pow(math.sin(Df / 2), 2) + math.cos(startLatTmp) * math.cos(finishLatTmp) * math.pow(math.sin(Dl / 2), 2)
	c = math.atan2(math.sqrt(a), math.sqrt(1 - a))
	R = 6371
	tripDistance = R * c

	return tripDistance

# trip speed calculator function
def speedCalc (startDate, finishDate, startLong, startLat, finishLong, finishLat):
	tripDuration = durationCalc(startDate, finishDate)
	if (tripDuration == 0):
		tripSpeed = 0
	else:
		tripDistance = distanceCalc(startLong, startLat, finishLong, finishLat)
		tripSpeed = tripDistance / tripDuration

	return tripSpeed

if __name__ == "__main__":
	# create spark session
	sparkSession = SparkSession.builder.appName("Q2_sql_csv").getOrCreate()

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

	# import trip data into trip schema
	trips = sparkSession.read.schema(tripSchema).csv("hdfs://master:9000/yellow_tripdata_1m.csv")

	# create vendor schema
	vendorSchema = StructType([
		StructField("tripID", StringType(), False),
		StructField("vendorID", StringType(), True)
	])

	# import vendor data into vendor schema
	vendors = sparkSession.read.schema(vendorSchema).csv("hdfs://master:9000/yellow_tripvendors_1m.csv")

	# calculate trip speeds
	tripSpeeds = udf(speedCalc, DoubleType())

	# create required view
	trips = trips.filter("cast(startDate as DATE) > cast('2015-03-10' as DATE)"). \
		withColumn("tripSpeed", tripSpeeds("startDate", "finishDate",
			"startLongitude", "startLatitude", "finishLongitude", "finishLatitude")). \
		select("tripID", "tripSpeed").orderBy("tripSpeed", ascending = False).limit(5). \
		join(vendors, "tripID", "left").orderBy("tripSpeed", ascending = False)

	# format and export output
	trips.write.format("csv").mode("overwrite").options(delimiter = '\t').save("hdfs://master:9000/Q2_sql_csv")

	# stop spark session
	spark.stop()
