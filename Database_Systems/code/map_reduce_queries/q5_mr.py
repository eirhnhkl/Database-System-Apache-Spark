from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("query5").getOrCreate()

if sys.argv[1] == 'csv':
	movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/movies.csv")
	movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/movie_genres.csv")
	ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_data/ratings.csv")

elif sys.argv[1] == 'parquet':
	movies = spark.read.parquet("hdfs://master:9000/movie_data/movies.parquet")
	movie_genres = spark.read.parquet("hdfs://master:9000/movie_data/movie_genres.parquet")
	ratings = spark.read.parquet("hdfs://master:9000/movie_data/ratings.parquet")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")
ratings.registerTempTable("ratings")

if sys.argv[1] == 'parquet':
	sqlString = \
				" select  q5.category as category,first(q5.user) as user,first(q5.count) as count,first(q5.title) as title,first(q5.rate) as rate from " + \
                    "(select  q4.category as category,q4.user as user,q4.count as count,mv.title as title,q4.rate as rate ,mv.popularity as popularity from " + \
                          "(select q2.category as category,q2.user as user,q2.rates_count as count,q3.movie_id as movie_id,q3.rate as rate   from  " + \
                               "( select  r.user as  user , " + \
                               " mg.category as category ," + \
                               " count(*) as rates_count" + \
                               " from movie_genres mg , ratings r " + \
                               " where mg.movieid==r.movie group by r.user , mg.category) q2" + \
                               " inner  join" + \
                               " ( select  r.user as  user , " + \
                               " mg.category as category ," + \
                               " r.movie as movie_id ,"+ \
                               " r.rating as rate " + \
                               " from  ratings r , movie_genres mg where  " + \
                               "  mg.movieid==r.movie ) q3" + \
                               " on q2.user=q3.user and q2.category=q3.category order by q2.category,q3.rate desc) q4 "+ \
                         " inner join movies mv on mv.movieid=q4.movie_id " + \
                          " order by q4.category,q4.count desc , q4.rate desc,mv.popularity desc) q5 " + \
                         " group by  q5.category order by q5.category"
						 
						 
t1 = time.time()		
res = spark.sql(sqlString)

res.show()
t2 = time.time()
print("\n")
print("Total time of execution is:", t2 - t1)
print("\n")