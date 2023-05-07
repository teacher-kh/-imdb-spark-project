from pyspark.sql.functions import year

names_19th_century = name_basics.filter((year(name_basics.birthYear) >= 1800) & (year(name_basics.birthYear) < 1900))

names_19th_century.select("primaryName").show(truncate=False)
