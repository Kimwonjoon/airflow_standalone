from pyspark.sql import SparkSession
import sys
import json
import os

spark = SparkSession.builder.appName("Select-Dynamic").getOrCreate()
dt = sys.argv[1]
file_path = f'/home/kimpass189/data/movies/dynamic-parq/year={dt}/data.parquet'
df = spark.read.parquet(file_path)
df.createOrReplaceTempView("movie")

result1 = spark.sql("""
select company, count(movieCd) as movieCnt
from movie
group by company
""")
print("="*10 + "회사별 영화 개수" + "="*10)
result1.show()
print("="*25)

result2 = spark.sql("""
select director, count(movieCd) as movieCnt
from movie
group by director
""")
print("="*10 + "감독별 영화 개수" + "="*10)
result2.show()
print("="*25)

