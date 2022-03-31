from pyspark.sql import SparkSession
from io import StringIO
import csv, time

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]


tainiesHDFS = "hdfs://master:9000/movie_data/movies.csv"
genresHDFS = "hdfs://master:9000//movie_data/movie_genres.csv"


dates_to_check = [[2000, 2004], [2005, 2009], [2010, 2014], [2015, 2019]]
t1 = time.time()

for i in dates_to_check:
        spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
        sparkSC = spark.sparkContext
        sparkSC.setLogLevel("ERROR")

        moviesList = sparkSC.textFile(tainiesHDFS) \
                .filter(lambda line : split_complex(line)[3] != ""   and    int( split_complex(line)[3].split("-")[0]) >= i[0] and int( split_complex(line)[3].split("-")[0]) <= (i[1])  )\
                .map(lambda line : (split_complex(line)[0], split_complex(line)[0:]))


        genresList = sparkSC.textFile(genresHDFS) \
                .filter(lambda line : line.split(',')[1] == 'Drama')\
                .map(lambda line : (line.split(',')[0], line.split(',')[1] ) )


        sinenwsiGenresMovies = genresList.join(moviesList)\
                .map(lambda line : line[1][1])\
                .map(lambda line : (1 ,   ( len(line[2].split(" ")) ,1  )  ))\
                .reduceByKey(lambda x,y : (x[0] + y[0], x[1]+y[1]) )\
                .map(lambda line : line[1])

        take = sinenwsiGenresMovies.take(1)
        print(str(i[0]), " - ", str(i[1]), " ----> ", take[0][0] / take[0][1])
t2 = time.time()
print("Time: ", t2 - t1)




