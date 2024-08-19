from pyspark.sql import SparkSession
import sys
import json
import os
import pandas as pd
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, ArrayType
spark = SparkSession.builder.appName("Dynamic-spark-json").getOrCreate()
dt = sys.argv[1]
file_path = f'/home/kimpass189/data/movies/dynamic-parq/year={dt}/data.parquet'
if os.path.isfile(file_path):
    print(f"데이터가 이미 존재합니다 : {file_path}")
else:
    jdf = spark.read.option("multiline","true").json(f'/home/kimpass189/data/movies/dynamic/year={dt}/data.json')
    ## companys, directors 값이 다중으로 들어가 있는 경우 찾기 위해 count 컬럼 추가
    from pyspark.sql.functions import explode, col, size, explode_outer
    ccdf = jdf.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))

    # # 펼치기
    edf = ccdf.withColumn("company", explode_outer("companys"))

    # # 또 펼치기
    eedf = edf.withColumn("director", explode_outer("directors"))
    eedf.write.mode("append").parquet(file_path)
