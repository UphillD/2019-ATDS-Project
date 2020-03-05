from __future__ import print_function
from pyspark import SparkContext
import sys
import datetime
import math

# date filtering function
def filterDate (line):
	record = line.split(",")
	startDate = record[1]
	startDateTmp = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
	startDateTmp2 = startDateTmp.date()
	filterDate = datetime.datetime.strptime("2015-03-10", '%Y-%m-%d')
	filterDateTmp = filterDate.date()
	return (startDateTmp2 > filterDateTmp)

# trip speed mapper function
def speedMapper (line):
	record = line.split(",")
	tripID = record[0]
	startDate = record[1]
	startDate = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
	finishDate = record[2]
	finishDate = datetime.datetime.strptime(finishDate, '%Y-%m-%d %H:%M:%S')
	tripDuration = (finishDate - startDate).total_seconds() / 60

	if (tripDuration == 0):
		tripSpeed = 0
	else:
		startLongitude = float(record[3])
		startLatitude = float(record[4])
		finishLongitude = float(record[5])
		finishLatitude = float(record[6])
		Df = finishLatitude - startLatitude
		Dl = finishLongitude - startLongitude
		a = math.pow(math.sin(Df / 2), 2) + math.cos(startLatitude) * math.cos(finishLatitude) * math.pow(math.sin(Dl / 2), 2)
		c = math.atan2(math.sqrt(a), math.sqrt(1 - a))
		R = 6371
		tripDistance = R * c
		tripSpeed = tripDistance / tripDuration

	return (tripID, tripSpeed)

def vendorMapper (line):
	record = line.split(",")
	tripID = record[0]
	vendorID = record[1]
	return (tripID, vendorID)

if __name__ == "__main__":
	# create spark context
	sparkContext = SparkContext(appName="Q2_mapReduce_csv")

	# import, filter and map trip data
	trips = sparkContext.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")
	trips = trips.filter(filterDate).map(speedMapper)
	trips = trips.sortBy(lambda line : line[1], False).take(5)
	tripsList = [ x[0] for x in trips ]
	trips = sparkContext.parallelize(trips)

	# import and map vendor data
	vendors = sparkContext.textFile("hdfs://master:9000/yellow_tripvendors_1m.csv")
	vendors = vendors.map(vendorMapper)
	vendors = vendors.filter(lambda line : line[0] in tripsList )

	trips = trips.leftOuterJoin(vendors).sortBy(lambda line : line[1][0], False)
	trips = trips.map(lambda line : line[0]+'\t'+ str(line[1][0])+'\t'+line[1][1])

	# format and export output
	finalRDD = sparkContext.parallelize(["Ride\t\tSpeed\t\tVendor"])
	finalRDD = finalRDD.union(trips)
	finalRDD.saveAsTextFile("hdfs://master:9000/Q2_mapReduce_csv")
