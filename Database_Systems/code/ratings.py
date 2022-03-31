from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from io import StringIO
import csv



if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField("User", StringType(), True),
        StructField("Movie", StringType(), True),
        StructField("Rating", StringType(), True),
        StructField("Timestamp", StringType(), True)])

    rdd = sc.textFile("hdfs://master:9000/movie_data/ratings.csv").map(lambda line: line.split(','))
    df = sqlContext.createDataFrame(rdd, schema)
    df.write.parquet('./ratings.parquet')