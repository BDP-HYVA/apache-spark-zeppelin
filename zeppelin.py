%spark2.pyspark
from pyspark.sql.functions import *
from pyspark.sql.functions import date_format


textfile = sc.textFile("hdfs:///user/maria_dev/final/preprocessed02.csv")
counts = textfile.flatMap(lambda line: line.split("\n"))\
.map(lambda line: (line.split(" ")[0],1))\
.reduceByKey(lambda a, b: a+b)


counts2 = counts.toDF()
counts2.show()
