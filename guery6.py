from pyspark.sql.functions import count

episode_counts = title_basics.filter(title_basics.titleType == 'tvSeries') \
                             .groupBy('parent_tconst') \
                             .agg(count('*').alias('episode_count'))
episode_counts.orderBy('episode_count', ascending=False).show(50)
