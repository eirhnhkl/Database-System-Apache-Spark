from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("query1").getOrCreate()

if sys.argv[1] == 'c':
	movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/movies.csv")
elif sys.argv[1] == 'p':
	movies = spark.read.parquet("hdfs://master:9000/user/user/movies.parquet")


movies.registerTempTable("movies")

if sys.argv[1] == 'c':
	sqlString = \
		"select (movies._c1) as Movie, m.year as Year " +\
		"from (select YEAR(movies._c3) as year, max((((movies._c6) - (movies._c5))*100)/(movies._c5)) as pr " + \
		"from movies " + \
		"where (movies._c3) is not null AND (movies._c5)<>0 AND (movies._c6)<>0 " + \
		"group by YEAR(movies._c3) " +\
		"having YEAR(movies._c3)>=2000) m, movies " +\
		"where m.year = YEAR(movies._c3) and m.pr = ((((movies._c6) - (movies._c5))*100)/(movies._c5))" + \
                "order by Year asc" 
elif sys.argv[1] == 'p':
	sqlString = \
		"select (movies.Title) as Movie, m.year as Year " +\
		"from (select YEAR(movies.Timestamp) as year, max((((movies.Income) - (movies.cost))*100)/(movies.cost)) as pr " + \
		"from movies " + \
		"where (movies.Timestamp) is not null AND (movies.Cost)<>0 AND (movies.Income)<>0 " + \
		"group by YEAR(movies.Timestamp) " +\
		"having YEAR(movies.Timestamp)>=2000) m, movies " +\
		"where m.year = YEAR(movies.Timestamp) and m.pr = ((((movies.Income) - (movies.Cost))*100)/(movies.Cost)) order by Year" 


t1 = time.time()		
res = spark.sql(sqlString)

res.show()
t2 = time.time()
print("\n")
total_time=t2-t1
print("Time:", total_time)
print("\n")