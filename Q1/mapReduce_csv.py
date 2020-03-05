from __future__ import print_function
from pyspark import SparkContext
import sys
import datetime

# First mapper function
def mapper (line):
	# extract the relevant data from the file
	record = line.split(",")
	startDate = record[1]
	finishDate = record[2]
	startHour = startDate.split(" ")[1].split(":")[0]

	# format the dates accordingly
	startDate = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
	finishDate = datetime.datetime.strptime(finishDate, '%Y-%m-%d %H:%M:%S')

	# calculate the duration of each route
	duration = (finishDate - startDate).total_seconds() / 60

	return (startHour, (duration, 1))

# Reducer function
def reducer (a, b):
	return (a[0] + b[0], a[1] + b[1])

# Second mapper (arranger) function
def arranger (line):
	# format the output
	return ("" + line[0] + "\t\t" + str(line[1][0] / line[1][1]))

# Main Function
if __name__ == "__main__":
	# create Spark Context
	sparkContext = SparkContext(appName = "Q1_mapReduce_csv")

	# read input file
	inputFile = sparkContext.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")

	# mapReduce
	avgTimes = inputFile.map(mapper).reduceByKey(reducer).sortByKey().map(arranger)

	# manage and export output
	finalRDD = sparkContext.parallelize(["HourOfDay\tAverageTripDuration"])
	finalRDD = finalRDD.union(avgTimes)
	finalRDD.saveAsTextFile("hdfs://master:9000/Q1_mapReduce_csv")
