%spark2.pyspark

from pyspark.sql.functions import *
from pyspark.sql.functions import date_format

textfile = sc.textFile("hdfs:///user/maria_dev/final/my_real_love.csv")
counts = textfile.flatMap(lambda line: line.split(","))\
.map(lambda line: (line.split(" ")[0],1))\
.reduceByKey(lambda a, b: a+b)\
.sortBy(lambda x: x[1], ascending=False)


counts2 = counts.toDF()

z.show(counts2)
