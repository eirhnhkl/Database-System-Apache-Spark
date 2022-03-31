from pyspark.sql import SparkSession
from io import StringIO
import csv,time

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]

kritikesHDFS = "hdfs://master:9000/movie_data/ratings.csv"
tainiesHDFS = "hdfs://master:9000/movie_data/movies.csv"
genresHDFS = "hdfs://master:9000/movie_data/movie_genres.csv"

spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
sparkSC = spark.sparkContext
# [movieID , [ movieID,bathmologia ]]
kritikesList = sparkSC.textFile(kritikesHDFS) \
        .map(lambda entry : entry.split(',') )\
        .map(lambda  entry :  (   entry[1] ,  entry[1:] ))


tainiesList = sparkSC.textFile(tainiesHDFS) \
        .map(lambda entry : split_complex(entry))\
        .map(lambda  entry : [  entry[0], entry   ] )
#  [movieID, [movieID, titlos] ]

movieGenres = sparkSC.textFile(genresHDFS) \
        .map(lambda  entry :  [ int(entry.split(',')[0] ),  entry.split(',')[1]    ] )


sunenwsiTainiesKritikes = tainiesList.join(kritikesList)\
        .map(lambda entry : [entry[0], [ float(entry[1][1][1]) ,  1 ]  ])\
        .reduceByKey(lambda x,y : [ x[0] + y[0], x[1] + y[1] ] )\
        .map(lambda entry : [ int(entry[0]) ,  entry[1][0]/ entry[1][1]])

t1 = time.time()


sinenwsiTainiesKritikesGenres = movieGenres.join(sunenwsiTainiesKritikes)\
        .map(lambda entry : [entry[0], entry[1][1], entry[1][0]])\
        .map(lambda entry: [entry[2],  [ entry[1],1 ]      ])\
        .reduceByKey(lambda x,y :  [ x[0] + y[0], x[1] + y[1] ])\
        .map(lambda entry: (entry[0], entry[1][0]/entry[1][1] , entry[1][1]) )

for entry in sinenwsiTainiesKritikesGenres.collect():
    print(entry)

t2 = time.time()
print("Total Time Computing: ", t2 - t1)

