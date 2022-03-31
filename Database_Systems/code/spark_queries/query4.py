from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("query4").getOrCreate()

if sys.argv[1] == 'c':
	movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/movies.csv")
	movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/movie_genres.csv")

elif sys.argv[1] == 'p':
	movies = spark.read.parquet("hdfs://master:9000/user/user/movies.parquet")
	movie_genres = spark.read.parquet("hdfs://master:9000/user/user/movie_genres.parquet")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")

def quinquennium (year):
    if(year>1999 and year<2005):
        return "2000-2004"
    elif(year>2004 and year<2010):
        return "2005-2009"
    elif(year>2009 and year<2015):
        return "2010-2014"
    elif(year>2014 and year<2020):
        return "2015-2019"
spark.udf.register("quinquennium",quinquennium)
    
if sys.argv[1] == 'c':
        sqlString = \
	      "select t.years as Quinquennium, avg(words) as AVGSUMMARY " + \
               "from (select movies._c0, (length(movies._c2) - length(replace(movies._c2, ' ', ''))+1) as words, quinquennium(YEAR(movies._c3)) as years " + \
               "from movies full outer join movie_genres on movies._c0=movie_genres._c0 " + \
	       "where movie_genres._c1 = 'Drama' AND YEAR(movies._c3)>1999 AND YEAR(movies._c3)<2020 AND (YEAR(movies._c3) is not null)) as t " + \
               "group by Quinquennium order by Quinquennium asc "
if sys.argv[1] == 'p':
        sqlString = \
              "select t.years as Quinquennium, avg(words) as AVGSUMMARY " + \
              "from (select movies.MovieID, (length(movies.Description) - length(replace(movies.Description, ' ', ''))+1) as words, quinquennium(YEAR(movies.Timestamp)) as years " + \
              "from movies full outer join movie_genres on movies.MovieID=movie_genres.Movie_ID " + \
	      "where movie_genres.Category = 'Drama' AND YEAR(movies.Timestamp)>1999 AND YEAR(movies.Timestamp)<2020 AND (YEAR(movies.Timestamp) is not null)) as t " + \
              "group by Quinquennium order by Quinquennium asc "



t1 = time.time()		
res = spark.sql(sqlString)

res.show()
t2 = time.time()
print("\n")
total_time=t2-t1
print("Time:", total_time)
print("\n")