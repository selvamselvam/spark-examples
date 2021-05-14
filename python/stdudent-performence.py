from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")

studentsdf = spark.read.csv("../data/StudentsPerformance.csv",header='true')

#print the schema and data
studentsdf.printSchema()
studentsdf.show()

#explain the sort
studentsdf.sort("math score").explain()


studentsdf.createOrReplaceTempView("studentsperformence")

print("#### Print the data using SQL Temp View")
sqlway = spark.sql("""select * from studentsperformence""")
sqlway.show()

dataFrameWay = sqlway.groupBy("gender").count()
dataFrameWay.show()

