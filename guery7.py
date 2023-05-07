from pyspark.sql.functions import floor

df_movies = df_movies.withColumn("decade", floor(df_movies.startYear/10)*10)

df_movies_grouped = df_movies.groupBy("decade", "primaryTitle") \
                             .agg({"averageRating": "max", "numVotes": "max"}) \
                             .orderBy("decade", ["max(averageRating)", "max(numVotes)"], ascending=False)

from pyspark.sql.window import Window

window = Window.partitionBy("decade").orderBy(["max(averageRating)", "max(numVotes)"], ascending=False)

df_movies_top10 = df_movies_grouped.select("*", rank().over(window).alias("rank")) \
                                   .filter(col("rank") <= 10) \
                                   .orderBy("decade", "rank")

df_movies_top10.show(100, truncate=False)
