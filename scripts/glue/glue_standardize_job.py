import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME','INPUT_PATH','OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.json(args['INPUT_PATH'])
# email normalization
df = df.withColumn('email', F.lower(F.col('email')))
# trim whitespace
for c in df.columns:
    df = df.withColumn(c, F.trim(F.col(c)))
# basic phone normalization (remove non-digits)
df = df.withColumn('phone', F.regexp_replace(F.col('phone'), r'[^0-9]', ''))
# address standardization placeholder (split into components)
df = df.withColumn('address', F.trim(F.col('address')))
# write parquet
df.write.mode('overwrite').parquet(args['OUTPUT_PATH'])
