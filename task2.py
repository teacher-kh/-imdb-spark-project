from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("IMDB data").getOrCreate()

title_basics_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("title_type", StringType(), True),
    StructField("primary_title", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("is_adult", IntegerType(), True),
    StructField("start_year", IntegerType(), True),
    StructField("end_year", IntegerType(), True),
    StructField("runtime_minutes", IntegerType(), True),
    StructField("genres", StringType(), True)])

title_ratings_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("average_rating", DoubleType(), True),
    StructField("num_votes", IntegerType(), True)])

name_basics_schema = StructType([
    StructField("nconst", StringType(), True),
    StructField("primary_name", StringType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("death_year", IntegerType(), True),
    StructField("primary_profession", StringType(), True),
    StructField("known_for_titles", StringType(), True)])

title_principals_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True)])

title_crew_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)])

movie_gross_schema = StructType([
    StructField("title", StringType(), True),
    StructField("studio", StringType(), True),
    StructField("domestic_gross", StringType(), True),
    StructField("foreign_gross", StringType(), True),
    StructField("year", IntegerType(), True)])

title_basics_df = spark.read.format("csv").option("header", "true").schema(title_basics_schema).load("/data.tsv")
name_basics_df = spark.read.format("csv").option("header", "true").schema(name_basics_schema).load("/data.tsv")
