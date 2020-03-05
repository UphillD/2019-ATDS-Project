from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys
import datetime
import math

def durationCalc (startDate, finishDate):
	startDateTmp = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
	finishDateTmp = datetime.datetime.strptime(finishDate, '%Y-%m-%d %H:%M:%S')
	tripDuration = (finishDateTmp - startDateTmp).total_seconds() / 60
	return tripDuration

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
	sparkSession = SparkSession.builder.appName("Q2_sql_parquet").getOrCreate()

	# import dataframes
	trips = sparkSession.read.parquet("hdfs://master:9000/yellow_tripdata_1m.parquet")
	vendors = sparkSession.read.parquet("hdfs://master:9000/yellow_tripvendors_1m.parquet")

	# calculate speeds
	tripSpeeds = udf(speedCalc, DoubleType())
	trips = trips.filter("cast(startDate as DATE) > cast('2015-03-10' as DATE)"). \
		withColumn("tripSpeed", tripSpeeds("startDate", "finishDate",
			"startLongitude", "startLatitude", "finishLongitude", "finishLatitude")). \
		select("tripID", "tripSpeed").orderBy("tripSpeed", ascending = False).limit(5). \
		join(vendors, "tripID", "left").orderBy("tripSpeed", ascending = False)

	# format & export output
	trips.write.format("csv").mode("overwrite").options(delimiter = '\t').save("hdfs://master:9000/Q2_sql_parquet")

	# stop spark session
	spark.stop()
