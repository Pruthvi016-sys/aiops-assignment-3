from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.master("local[2]").appName("smoke").getOrCreate()
df = spark.range(1000).toDF("x")
f = udf(lambda v: float(v) * 2, DoubleType())
print("SPARK UDF SUM:", df.select(f("x").alias("y")).agg({"y": "sum"}).collect())
spark.stop()

import ray, ray.data
ray.init(num_cpus=2)
ds = ray.data.range(1000).map_batches(lambda b: {"y": b["id"] * 2}, batch_format="numpy")
print("RAY SUM:", ds.sum("y"))
