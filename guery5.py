from pyspark.sql.functions import count

adult_counts = ratings.filter(ratings.isAdult == 1).groupBy("region").agg(count("*").alias("adult_count"))

adult_counts = adult_counts.orderBy("adult_count", ascending=False)

adult_counts.show(100)
