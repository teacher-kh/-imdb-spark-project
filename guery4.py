from pyspark.sql.functions import col
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


join_expr = (title_principals_df["tconst"] == title_basics_df["tconst"]) & \
            (title_principals_df["nconst"] == name_basics_df["nconst"])
joined_df = title_principals_df.join(title_basics_df, join_expr).join(name_basics_df, join_expr)

result_df = joined_df.select(col("primaryName").alias("person_name"), \
                             col("originalTitle").alias("movie_title"), \
                             col("characters"))
result_df.show(truncate=False)
