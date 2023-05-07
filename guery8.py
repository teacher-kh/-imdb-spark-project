joined_df = title_basics.join(title_ratings, on="tconst")

grouped_df = joined_df.groupBy("genres").agg(avg("averageRating").alias("avg_rating")).orderBy(desc("avg_rating"))

for genre in grouped_df.select("genres").collect():
    genre = genre.genres
    genre_df = joined_df.filter(col("genres") == genre).orderBy(desc("averageRating")).limit(10)
    print(f"\nTop 10 titles of {genre}:\n")
    genre_df.select("primaryTitle", "averageRating").show()
