from read_write import write
from pyspark.sql.functions import col
title_basics.printSchema()
title_basics.select([col(c) for c in title_basics.columns[:5]]).show(5)


def task1(df):
    df.filter().withColumn

    write(df, directory)

def task2(df):
    df.write.csv('path/file.csv', header=True, mode='overwrite')