from pyspark.sql import SparkSession
import itertools
import time

spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# paradeigma me ratings kai movie_genres
# Estw oti to left table einai pio mikro ara auto kanoume broadcast
leftTableHDFS = "hdfs://master:9000/user/user/movie_genres2.csv"
rightTableHDFS = "hdfs://master:9000/movie_data/ratings.csv"


leftTable = sc.textFile(leftTableHDFS) \
        .map(lambda line: line.split(','))\
        .map(lambda line : [int(line[0])] + [line[0:]])\
        .map(lambda line : ( int(line[0]) ,[line] ))

rightTable = sc.textFile(rightTableHDFS) \
        .map(lambda line : line.split(','))\
        .map(lambda line :  (  int(line[0]) ,[line[0:]]    )   )

t1 = time.time()

# ta kanoume reduceByKey gia na ta mazepsoume se ena entry
# auta ta entries p exoun idio id
rightTable = rightTable.reduceByKey(lambda x, y : x + y)

# Edw upoxrewtika (!) kanoume reduceByKey gia na baloume ta entries me idio
# id se ena entry, akthws se diaforetiki periptwsi to collectAsMap
# den ta krataei kai ta 2
leftTable  = leftTable.reduceByKey(lambda x, y : x + y).collectAsMap()

leftTableBroadcasted = sc.broadcast(leftTable)

rightTable = rightTable\
        .map(lambda line : [line] + [[] if line[0] not in leftTableBroadcasted.value else leftTableBroadcasted.value[line[0]]])\
        .filter(lambda line:line!=[])

# kai kanoume to ginomemeno gia na broume to teliko apotelesma
rightTable = rightTable\
        .map(lambda line: itertools.product(line[0][1], line[1]))

for i in rightTable.take(700):
    for k in i:
        print(k)

t2 = time.time()

print("Time: ", t2 - t1)



