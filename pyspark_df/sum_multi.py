from pyspark.sql import SparkSession
import sys
LOAD_DT = sys.argv[1]

spark = SparkSession.builder.appName("Sum_multi").getOrCreate()

df1 = spark.read.parquet(f"/home/kimpass189/data/movie/hive/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("first_day")

df_n = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    multiMovieYn,
    '{LOAD_DT}' AS load_dt
FROM first_day
GROUP BY multiMovieYn
""")
df_n.write.mode('append').partitionBy("load_dt").parquet("/home/kimpass189/data/movie/sum-multi")

df_r = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    repNationCd,
    '{LOAD_DT}' AS load_dt
FROM first_day
GROUP BY repNationCd
""")

df_r.write.mode('append').partitionBy("load_dt").parquet("/home/kimpass189/data/movie/sum-nation")

spark.stop()
