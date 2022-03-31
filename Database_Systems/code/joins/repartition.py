from pyspark.sql import SparkSession
from datetime import datetime, timezone
import itertools
from io import StringIO
import csv,time


spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


leftTableHDFS = "hdfs://master:9000/movie_data/ratings.csv"
rightTableHDFS = "hdfs://master:9000/movie_data/movie_genres.csv"

# pairnw oti prepei apo ta movies kai bazw ena 0 ws tag gia na kserw oti i toupla einai to movies
leftTable = sc.textFile(leftTableHDFS) \
        .map(lambda line:  line.split(','))\
        .map(lambda line : ( int(line[0]) , 0, line ))

# omoiws apla vazw 1 anti gia 0
rightTable = sc.textFile(rightTableHDFS) \
        .map(lambda line : line.split(','))\
        .map(lambda line : ( int(line[0]) , 1, line ))

# ara menei na kanw union ta 2 panw kai reduceByKey kai na prosthetw ta arrays mesa
# tous wste na exw  teliko  (movieId, [genre1,genre2,...] , [movie1,movie2,...]
# sto telos kanw to ginomeno twn duo panw wste na exw tis telikes touples
t1 = time.time()

# twra ta kanw union wste opws sto pseudokodika na einai ola ta emit mazi
sinenwsiTables = leftTable.union(rightTable)

# twra tha ftiaksw tous pinakes BR kai BL
sinenwsiTables = sinenwsiTables\
    .map(lambda entry:  ( entry[0], [ [],[entry[2]] ] ) if entry[1] == 0 else ( entry[0], [ [entry[2][1]],[] ] )    )\
    .sortByKey(False)

# opote oi touples apo ta movies exoun keno BR = [] (keno dld) kai BL = [entry]
# kai oi touples twn genres exoun BR = [] kai BL = [entry]
# opote arkei na kanw + ola ta BR metaksu tous kai ola ta BL metaksi tous opws leei ston pseudokwdika
# kathws isxuei oti [] + [entry] = [entry] !!
sinenwsiTables = sinenwsiTables\
        .reduceByKey(lambda entryN, entryNplus1:  [entryN[0] + entryNplus1[0] , entryN[1] + entryNplus1[1]]) \
        .map(lambda line: itertools.product(*(line[1])))
# meta kanw to ginomeno opws leei o pseudokodikas

for entry in sinenwsiTables.take(30):
    for sub_entry in entry:
        print(sub_entry)

t2 = time.time()

print("Time: ", t2 - t1)
