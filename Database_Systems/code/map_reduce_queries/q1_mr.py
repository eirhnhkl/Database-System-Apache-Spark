from pyspark.sql import SparkSession
from datetime import datetime, timezone
from io import StringIO
import csv,time

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
sparkSC = spark.sparkContext

tainiesHDFS = "hdfs://master:9000/movie_data/movies.csv"

t1 = time.time()

# Den tha prepei na exei kena esoda, kena eksoda kai keni imerominia
tainies = sparkSC.textFile(tainiesHDFS) \
        .filter(lambda entry : split_complex(entry)[6] != "0" and split_complex(entry)[5] != "0" and  split_complex(entry)[3] != "")\

# Vazw sto telos tou kathe entry tin imerominia kai to esodo
# epeita ftiaxnw toupla tis morfis (xronologiaTainias, olaTaDedomenaTisTainias), olaTaDedomenaTIsTainias einai array. Diladi ta proetoimazw gia to ReduceByKey
# meta filter kai krataw tis tainies p exoun year > 2000
# kai meta reduceByKey me key to YEAR kai krataw tin tainia me ta megalitera esoda
tainies = tainies \
        .map(lambda entry: split_complex(entry) + [int((split_complex(entry))[3].split("-")[0])] + [((int(split_complex(entry)[6]) - int(split_complex(entry)[5])) * 100) / (int(split_complex(entry)[5]))]) \
        .map(lambda entry: (entry[8], entry)) \
        .filter(lambda entry: entry[0] >= 2000) \
        .reduceByKey(lambda x, y: y if x[9] < y[9] else x)

# ta morfopoiw katallila gia ektipwsi
# kai meta sortByKey opou KEY einai to year gia na ektupwthoun me seira
tainies = tainies \
        .map(lambda entry: (entry[1][8], entry)) \
        .sortByKey(False).collect()
# kai meta ta ektupwnw
for i in tainies:
	print("For ", int(i[0]), " result is:", i[1][1])
	print("\n")

t2 = time.time()
print("Time: ", t2 - t1)