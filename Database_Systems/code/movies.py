from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from io import StringIO
import csv

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]



if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField("MovieID", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Description", StringType(), True),
	StructField("Timestamp", StringType(), True),
        StructField("Duration", StringType(), True),
        StructField("Cost", StringType(), True),
	StructField("Income", StringType(), True),
        StructField("Popularity", StringType(), True)])

    rdd = sc.textFile("hdfs://master:9000/movie_data/movies.csv").map(lambda line: split_complex(line))
    df = sqlContext.createDataFrame(rdd, schema)
    df.write.mode('overwrite').parquet('./movies.parquet')