from pyspark.sql import SparkSession
from datetime import datetime, timezone
from io import StringIO
import csv,time

spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
sparkSC = spark.sparkContext
sparkSC.setLogLevel("ERROR")


ratingsHDFS = "hdfs://master:9000/movie_data/ratings.csv"

t1 = time.time()
# Arxika thelw na mathw posoi xristes exoun meso oro vathmologias tous megalitero apo to 3
# Opote gia kathe entry tou ratings.csv to kanw ( userID, [ rating, 1 ]  )
# wste otan ta kanw reduceByKey na prosthesw ola ta ratings mazi kai olous tous assous mazi
# opote meta to reduceByKey() tha exw gia kathe user mia toupla tis morfis
# (userID, [sunolikoAthrismaRating, posaRatingExeiKanei])
# opote meta menei na kanw ena map gia na brw ton MO, dld tha exw
# [userId, MesoOroVathmologias]

medianBiggerThan3 = sparkSC.textFile(ratingsHDFS) \
        .map(lambda entry: (entry.split(',')[0], [entry.split(',')[2] , 1]))\
        .reduceByKey(lambda x,y: [float(x[0]) + float(y[0]),x[1]+1]  )\
        .map(lambda x : [ x[0] , float( x[1][0]) / float( x[1][1]) ])

# Sti sinexeia tha prepei na kanw filter kai na kratisw ta entries pou exoun vathmologia
# megaluteri tou 3 kai kanw apla count gia na brw ton arithmo twn xristwn

medianBiggerThan3 = medianBiggerThan3\
        .filter(lambda x : x[1] > 3.0 ) \
        .map(lambda x: ( x[1],x[0]))\
        .sortByKey()\
        .count()

# Edw to idio apla kanoume kai distinct gia na mathoume ton sinoliko arithmo twn users
distinctUsers = sparkSC.textFile(ratingsHDFS) \
        .map(lambda entry: (entry.split(',')[0]))\
        .distinct().count()
print("\n")
print("Result --> ", int(medianBiggerThan3) / int(distinctUsers))

t2 = time.time()
print("Time: ", t2 - t1)
print("\n")
