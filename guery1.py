from pyspark.sql.functions import col

ukr_titles = df.filter(col("language") == "ukr").select("title")
ukr_titles.show()
