from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
spark = (SparkSession.builder.master("spark://127.0.0.1:7077")
         .appName("cluster_check").config("spark.executor.memory", "2g").getOrCreate())
f = udf(lambda v: float(v) * 2, DoubleType())
print(spark.range(1000000).select(f("id").alias("y")).agg({"y": "sum"}).collect())
input("App is live. Refresh http://localhost:8080, take the screenshot, then press Enter...")
spark.stop()
