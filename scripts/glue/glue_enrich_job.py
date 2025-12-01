import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME','INPUT_PATH','OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.parquet(args['INPUT_PATH'])
# Example enrichment: flag probable high-value customers (placeholder)
df = df.withColumn('high_value_flag', F.when(F.size(F.col('orders'))>2, True).otherwise(False))
df.write.mode('overwrite').parquet(args['OUTPUT_PATH'])
