import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME','CRM_PATH','CLICKSTREAM_PATH','ORDERS_PATH','OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

crm = spark.read.parquet(args['CRM_PATH'])
click = spark.read.parquet(args['CLICKSTREAM_PATH'])
orders = spark.read.parquet(args['ORDERS_PATH'])

# Deterministic matching on email
cust = crm.alias('crm').join(click.alias('click'), F.col('crm.email')==F.col('click.email'), 'fullouter')

# If email missing, try phone
cust = cust.fillna({'crm.phone':'','click.phone':''})
cust = cust.withColumn('match_email', F.when(F.col('crm.email').isNotNull(), F.col('crm.email')).otherwise(F.col('click.email')))
cust = cust.withColumn('match_phone', F.when(F.col('crm.phone').isNotNull(), F.col('crm.phone')).otherwise(F.col('click.phone')))

# Simple dedupe: assign primary_customer_id by email or phone
cust = cust.withColumn('primary_id', F.coalesce(F.col('match_email'), F.col('match_phone')))

# Probabilistic matching (simple name similarity placeholder)
# create candidate buckets by first letter of last name
cust = cust.withColumn('lname_bucket', F.lower(F.substring(F.col('last_name'),1,1)))
# Aggregate profiles per primary_id
gold = cust.groupBy('primary_id').agg(
    F.first('first_name').alias('first_name'),
    F.first('last_name').alias('last_name'),
    F.collect_set('email').alias('emails'),
    F.collect_set('phone').alias('phones')
)
# join orders
orders = orders.withColumnRenamed('user_id','primary_id')
gold = gold.join(orders.groupBy('primary_id').agg(F.collect_list('id').alias('orders')), on='primary_id', how='left')

gold.write.mode('overwrite').parquet(args['OUTPUT_PATH'])
