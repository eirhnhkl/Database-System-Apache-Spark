from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("query3").getOrCreate()

if sys.argv[1] == 'c':
	ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/ratings.csv")
	movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/movie_genres.csv")

elif sys.argv[1] == 'p':
	ratings = spark.read.parquet("hdfs://master:9000/user/user/ratings.parquet")
	movie_genres = spark.read.parquet("hdfs://master:9000/user/user/movie_genres.parquet")


ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")

if sys.argv[1] == 'c':
        sqlString = \
	  "select MovieCategory, AverageRating, NumberOfMoviesForEachCategory from"+ \
          "(select (a.category) as MovieCategory,avg(a.avgmovie) as AverageRating , count(*) as NumberOfMoviesForEachCategory  " + \
          "from (select (mg._c0), (mg._c1) as category ,avg(r._c2) as avgmovie " + \
	  "from (movie_genres as mg) join (ratings as r) " + \
	  "on (mg._c0) == (r._c1) group by (mg._c1),(mg._c0)) as a " + \
          "group by a.category)" + \
          "order by MovieCategory asc"
if sys.argv[1] == 'p':
        sqlString = \
          "select MovieCategory, AverageRating, NumberOfMoviesForEachCategory from"+ \
          "(select (a.category) as MovieCategory,avg(a.avgmovie) as AverageRating , count(*) as NumberOfMoviesForEachCategory  " + \
          "from (select (mg.Movie_ID), (mg.Category) as category ,avg(r.Rating) as avgmovie " + \
	  "from (movie_genres as mg) join (ratings as r) " + \
	  "on (mg.Movie_ID) == (r.Movie) group by (mg.Category),(mg.Movie_ID)) as a " + \
          "group by a.category)" + \
          "order by MovieCategory asc"



t1 = time.time()		
res = spark.sql(sqlString)

res.show()
t2 = time.time()
print("\n")
total_time=t2-t1
print("Time:", total_time)
print("\n")