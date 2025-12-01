import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---- Read arguments from Glue ----
args = getResolvedOptions(sys.argv, ["JOB_NAME", "database_name", "raw_bucket"])
database_name = args["database_name"]
raw_bucket = args["raw_bucket"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---- Helper functions ----
def normalize_email_col(col):
    return F.lower(F.trim(col))

def normalize_name_col(col):
    return F.initcap(F.trim(col))


raw_crm_path = f"s3://{raw_bucket}/raw/crm/"

print(f"Reading CRM data from: {raw_crm_path}")

# Try to read as multiline JSON (handles array-of-objects & pretty JSON)
df_raw = spark.read.option("multiline", "true").json(raw_crm_path)

print("Raw CRM schema:")
df_raw.printSchema()

cols = df_raw.columns
print(f"Columns in raw CRM: {cols}")

# If Spark only sees _corrupt_record, JSON format is wrong for the default reader
if cols == ["_corrupt_record"]:
    print("ERROR: Spark could not parse the JSON correctly and only found _corrupt_record.")
    print("Sample of _corrupt_record values:")
    df_raw.select("_corrupt_record").show(5, truncate=False)
    raise Exception(
        "CRM JSON appears to be malformed or in an unexpected format. "
        "Check if the file is a valid JSON array or NDJSON. "
        "You may need to adjust the schema or input format."
    )

df_std = df_raw

# ---- Email normalization (only if column exists) ----
if "email" in cols:
    df_std = df_std.withColumn("email_normalized", normalize_email_col(F.col("email")))
elif "Email" in cols:
    df_std = df_std.withColumn("email_normalized", normalize_email_col(F.col("Email")))
else:
    df_std = df_std.withColumn("email_normalized", F.lit(None).cast("string"))
    print("WARNING: No email column found in CRM data; email_normalized set to NULL")

# ---- First name normalization ----
if "first_name" in cols:
    df_std = df_std.withColumn("first_name_std", normalize_name_col(F.col("first_name")))
elif "FirstName" in cols:
    df_std = df_std.withColumn("first_name_std", normalize_name_col(F.col("FirstName")))
else:
    df_std = df_std.withColumn("first_name_std", F.lit(None).cast("string"))
    print("WARNING: No first_name column found in CRM data; first_name_std set to NULL")

# ---- Last name normalization ----
if "last_name" in cols:
    df_std = df_std.withColumn("last_name_std", normalize_name_col(F.col("last_name")))
elif "LastName" in cols:
    df_std = df_std.withColumn("last_name_std", normalize_name_col(F.col("LastName")))
else:
    df_std = df_std.withColumn("last_name_std", F.lit(None).cast("string"))
    print("WARNING: No last_name column found in CRM data; last_name_std set to NULL")

# ---- Add technical fields ----
df_std = (
    df_std
    .withColumn("source_system", F.lit("crm"))
    .withColumn("ingest_date", F.current_date())
)

std_crm_path = f"s3://{raw_bucket}/standardized/crm/"

print(f"Writing standardized CRM data to: {std_crm_path}")

(
    df_std
    .repartition(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .save(std_crm_path)
)

print("CRM standardization job completed successfully.")

job.commit()
