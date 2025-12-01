import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import boto3
import json

# ---- Read arguments from Glue ----
args = getResolvedOptions(sys.argv, ["JOB_NAME", "database_name", "raw_bucket"])
database_name = args["database_name"]
raw_bucket = args["raw_bucket"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Identity Resolution job started. raw_bucket={raw_bucket}, db={database_name}")

# ---------- Helper: safe reader for Parquet ----------
def safe_read_parquet(path_desc, path, cols_needed):
    """
    Try to read a Parquet path.
    If it doesn't exist, return empty DataFrame with the needed columns.
    """
    print(f"Reading {path_desc} from: {path}")
    try:
        df = spark.read.parquet(path)
        print(f"{path_desc} schema:")
        df.printSchema()
        return df
    except Exception as e:
        print(f"WARNING: Could not read {path_desc} at {path}: {e}")
        schema = T.StructType([
            T.StructField(c_name, T.StringType(), True) for c_name in cols_needed
        ])
        empty_df = spark.createDataFrame([], schema)
        print(f"Created empty {path_desc} DataFrame with columns: {cols_needed}")
        return empty_df

# ---------- 1. Read standardized CRM (anchor dataset) ----------
std_crm_path = f"s3://{raw_bucket}/standardized/crm/"
print(f"Reading standardized CRM from: {std_crm_path}")

df_crm = safe_read_parquet(
    "CRM",
    std_crm_path,
    ["email_normalized", "first_name_std", "last_name_std"]
)

crm_cols = df_crm.columns
print(f"CRM columns: {crm_cols}")

# If email_normalized doesn't exist, try to derive it
if "email_normalized" not in crm_cols:
    if "email" in crm_cols:
        df_crm = df_crm.withColumn("email_normalized", F.lower(F.trim(F.col("email"))))
        print("Created email_normalized from email in CRM.")
    else:
        df_crm = df_crm.withColumn("email_normalized", F.lit(None).cast("string"))
        print("WARNING: No email/email_normalized in CRM; identity resolution will be limited.")

# Add a surrogate customer360_id (for demo; in real life, make it stable)
w = Window.orderBy(F.monotonically_increasing_id())
df_customers = (
    df_crm
    .withColumn("customer360_id", F.row_number().over(w).cast("long"))
    .withColumn("source_system", F.lit("crm"))
)

# ---------- 2. Read standardized Orders / Marketing / Clickstream ----------
# We will try to match them to CRM based on email_normalized.

cols_needed_other = ["email_normalized"]

std_orders_path = f"s3://{raw_bucket}/standardized/orders/"
std_marketing_path = f"s3://{raw_bucket}/standardized/marketing/"
std_clickstream_path = f"s3://{raw_bucket}/standardized/clickstream/"

df_orders = safe_read_parquet("Orders", std_orders_path, cols_needed_other)
df_marketing = safe_read_parquet("Marketing", std_marketing_path, cols_needed_other)
df_clickstream = safe_read_parquet("Clickstream", std_clickstream_path, cols_needed_other)

# Make sure email_normalized exists in each; if not, create a NULL column
for name, df in [("orders", df_orders), ("marketing", df_marketing), ("clickstream", df_clickstream)]:
    if "email_normalized" not in df.columns:
        print(f"WARNING: {name} has no email_normalized column; adding NULL column.")
        df = df.withColumn("email_normalized", F.lit(None).cast("string"))
    # replace back
    if name == "orders":
        df_orders = df
    elif name == "marketing":
        df_marketing = df
    else:
        df_clickstream = df

# ---------- 3. Deterministic matching on email_normalized ----------
# Inner join CRM customers with each source by email_normalized.

def make_mapping(df_source, source_name):
    df_m = (
        df_source
        .filter(F.col("email_normalized").isNotNull())
        .select("email_normalized")
        .dropDuplicates()
        .join(
            df_customers.select("customer360_id", "email_normalized"),
            on="email_normalized",
            how="inner"
        )
        .withColumn("source_system", F.lit(source_name))
    )
    return df_m

map_orders = make_mapping(df_orders, "orders")
map_marketing = make_mapping(df_marketing, "marketing")
map_clickstream = make_mapping(df_clickstream, "clickstream")

# Combine mappings (excluding CRM itself for now)
df_mappings_other = map_orders.unionByName(map_marketing, allowMissingColumns=True) \
                              .unionByName(map_clickstream, allowMissingColumns=True)

# Add CRM mapping itself (for completeness)
map_crm = (
    df_customers
    .select("customer360_id", "email_normalized")
    .withColumn("source_system", F.lit("crm"))
)

df_mappings = map_crm.unionByName(df_mappings_other, allowMissingColumns=True)

# ---------- 4. Simple probabilistic score placeholder ----------
# For now, deterministic matches get score 1.0.
# Later you can extend this to use name/address similarity (e.g., levenshtein).
df_mappings = df_mappings.withColumn("match_score", F.lit(1.0))

# ---------- 5. Build master customer table ----------
# For now we just take CRM standardized columns + customer360_id.
master_cols = ["customer360_id", "email_normalized"]
if "first_name_std" in df_customers.columns:
    master_cols.append("first_name_std")
if "last_name_std" in df_customers.columns:
    master_cols.append("last_name_std")
if "ingest_date" in df_customers.columns:
    master_cols.append("ingest_date")

df_master_customer = df_customers.select(*master_cols)
df_master_customer = df_master_customer.withColumn(
    "customer360_id_str",
    F.col("customer360_id").cast("string")
)

# ---------- 6. Write outputs to S3 (master zone) ----------
master_customer_path = f"s3://{raw_bucket}/master/customer/"
master_mapping_path = f"s3://{raw_bucket}/master/customer_ids/"

print(f"Writing master customer to: {master_customer_path}")
df_master_customer.repartition(1).write.mode("overwrite").parquet(master_customer_path)

print(f"Writing customer ID mappings to: {master_mapping_path}")
df_mappings.repartition(1).write.mode("overwrite").parquet(master_mapping_path)

print("Identity Resolution job completed successfully.")

print("Starting DynamoDB batch write to customer360_profiles")

def write_partition_to_dynamodb(rows_iter):
    """
    This runs on each Spark partition.
    We create a DynamoDB resource & batch_writer per partition to be efficient.
    """
    dynamo = boto3.resource("dynamodb")
    table = dynamo.Table("customer360_profiles")

    with table.batch_writer(overwrite_by_pkeys=["customer360_id"]) as batch:
        for row in rows_iter:
            row_dict = row.asDict()

            # Use string key for DynamoDB
            customer_id = row_dict.get("customer360_id_str") or str(row_dict.get("customer360_id"))

            if not customer_id:
                # Skip rows without an ID
                continue

            item = {
                "customer360_id": customer_id,
                "email_normalized": row_dict.get("email_normalized"),
                "first_name_std": row_dict.get("first_name_std"),
                "last_name_std": row_dict.get("last_name_std"),
                "ingest_date": str(row_dict.get("ingest_date")) if row_dict.get("ingest_date") else None,
            }

            # Remove None values (DynamoDB doesn't like explicit None)
            item = {k: v for k, v in item.items() if v is not None}

            batch.put_item(Item=item)

# Use foreachPartition so we don't collect to the driver
df_master_customer.select(
    "customer360_id",
    "customer360_id_str",
    "email_normalized",
    "first_name_std",
    "last_name_std",
    "ingest_date"
).rdd.foreachPartition(write_partition_to_dynamodb)

print("Completed DynamoDB batch write.")


job.commit()
