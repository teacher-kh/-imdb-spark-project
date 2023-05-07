from pyspark.sql.functions import col

long_movies = movies.filter(col('duration') > 120)
long_movie_titles = long_movies.select('title').collect()

for title in long_movie_titles:
    print(title[0])
