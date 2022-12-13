# zeppelin notebook
%spark2.pyspark

from pyspark.sql.functions import *
from pyspark.sql.functions import date_format

textfile = sc.textFile("hdfs:///user/maria_dev/final/preprocessed_data.csv")
ans = textfile.flatMap(lambda line: line.split(","))\
  .map(lambda line: (line.split(" ")[0],1))\
  .reduceByKey(lambda a, b: a+b)\
  .sortBy(lambda x: x[1], ascending=False)

final_ans = ans.toDF()

z.show(final_ans)
