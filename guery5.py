from pyspark.sql.functions import count
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

adult_counts = ratings.filter(ratings.isAdult == 1).groupBy("region").agg(count("*").alias("adult_count"))

adult_counts = adult_counts.orderBy("adult_count", ascending=False)

adult_counts.show(100)
