"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = "../data/example.text"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('e')).count()
numBs = logData.filter(logData.value.contains('t')).count()

print("Lines with e: %i, lines with t: %i" % (numAs, numBs))

spark.stop()