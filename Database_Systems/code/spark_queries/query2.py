from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("query2").getOrCreate()

if sys.argv[1] == 'c':
	ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/ratings.csv")
elif sys.argv[1] == 'p':
	ratings = spark.read.parquet("hdfs://master:9000/user/user/ratings.parquet")

ratings.registerTempTable("ratings")

if sys.argv[1] == 'c':
        sqlString = \
	        "select 100*(select count(*) " + \
                "from (select (_c0) from ratings " + \
                "group by (_c0) having avg(_c2) > 3))/(select count(*) from (select distinct(_c0) from ratings)) as Percentage"
elif sys.argv[1] == 'p':
	sqlString = \
               "select 100*(select count(*) " + \
                "from (select (User) from ratings " + \
                "group by (User) having avg(Rating) > 3))/(select count(*) from (select distinct(User) from ratings)) as Percentage"




t1 = time.time()		
res = spark.sql(sqlString)

res.show()
t2 = time.time()
print("\n")
total_time=t2-t1
print("Time:", total_time)
print("\n")