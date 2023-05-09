from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

ukr_titles = df.filter(col("language") == "ukr").select("title")
ukr_titles.show()
