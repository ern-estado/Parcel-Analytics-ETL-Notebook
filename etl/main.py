#THIS IS THE MAIN ETL 
# parses source json csv, applies schema to fact and dims as per data model
# runs simple dq checks before parsing, after parsing, and after building tables - stores dq report in metadata
# builds fact and dims in parquet 
# tables are then analysed in notebooks/report.ipynb

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, year, month, dayofmonth, quarter, dayofweek, to_date, concat_ws, expr)
from pyspark.sql.types import *
import pandas as pd
import os
from datetime import datetime

#START SPARK SESSION
spark = SparkSession.builder \
    .appName("Fact and Dim Tables Builder with UUIDs and DQ") \
    .getOrCreate()

# dq checks functions setup
def run_dq_checks(df, check_point_name):
    checks = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_count = df.count()

    # non-empty dataframe check
    if total_count > 0:
        checks.append({
            "check_point": check_point_name,
            "check_name": "Non-empty dataframe",
            "status": "PASS",
            "details": f"Row count: {total_count}",
            "timestamp": ts
        })
    else:
        checks.append({
            "check_point": check_point_name,
            "check_name": "Non-empty dataframe",
            "status": "FAIL",
            "details": "Dataframe is empty",
            "timestamp": ts
        })

    # check for nulls in key columns (picked on 3 columns)
    cols_to_check = df.columns[:3]
    for c in cols_to_check:
        null_count = df.filter(col(c).isNull()).count()
        if null_count == 0:
            checks.append({
                "check_point": check_point_name,
                "check_name": f"No nulls in column '{c}'",
                "status": "PASS",
                "details": f"Null count: {null_count}",
                "timestamp": ts
            })
        else:
            checks.append({
                "check_point": check_point_name,
                "check_name": f"No nulls in column '{c}'",
                "status": "FAIL",
                "details": f"Null count: {null_count}",
                "timestamp": ts
            })

    return checks


### paths to read source json and write fact, dims, and dq report
input_path = "/workspaces/inpost_analytics/input/Dane_zadanie_rekrutacyjne.csv"
metadata_path = "/workspaces/inpost_analytics/etl/metadata/dq_report.csv"
output_base_parquet = "/workspaces/inpost_analytics/pipeline"
output_base_csv = "/workspaces/inpost_analytics/warehouse"

### JSON PARSER SETUP
df = spark.read.option("header", True) \
               .option("escape", '"') \
               .option("multiLine", True) \
               .option("quote", '"') \
               .option("sep", ",") \
               .csv(input_path)

# 1st dq check before parsing
dq_results = run_dq_checks(df, "Before Parsing")

### JSON SCHEMA SETUP
event_schema = StructType([
    StructField("event_code", StringType()),
    StructField("event_date", TimestampType()),
    StructField("entry_date", TimestampType()),
    StructField("event_nature", StringType()),
    StructField("event_sub_code", StringType()),
    StructField("shipping", StructType([
        StructField("sign_code", StringType()),
        StructField("brand_code_alpha", StringType()),
        StructField("collection", StructType([
            StructField("prestation_code", StringType()),
            StructField("round", StructType([
                StructField("codeAgence", IntegerType()),
                StructField("pays", StringType())
            ]))
        ])),
        StructField("paid", StringType()),
        StructField("sav_folder", BooleanType()),
        StructField("is_replaced", BooleanType()),
        StructField("state", StructType([
            StructField("code", StringType()),
            StructField("date", StringType()),
            StructField("nature", StringType()),
            StructField("sousCode", StringType())
        ])),
        StructField("canceled_state", StringType()),
        StructField("shipping_id", StringType()),
        StructField("delivery", StructType([
            StructField("prestation_code", StringType()),
            StructField("round", StructType([
                StructField("codeAgence", IntegerType()),
                StructField("pays", StringType())
            ]))
        ])),
        StructField("parcel_number", IntegerType()),
        StructField("shipping_number", IntegerType()),
        StructField("options", StringType()),
        StructField("parcel_sequence", IntegerType())
    ]))
])

# parse json column
df_parsed = df.withColumn("event_json", from_json(col("event"), event_schema))

# dq checks after parsed
dq_results += run_dq_checks(df_parsed, "After Parsing")

### FLATTENS JSON BEFORE FACT AND DIMS BUILD
df_flat = df_parsed.select(
    col("event_json.event_code").alias("event_code"),
    col("event_json.event_sub_code").alias("event_sub_code"),
    col("event_json.event_date").alias("event_date"),
    col("event_json.entry_date").alias("entry_date"),
    col("event_json.event_nature").alias("event_nature"),

    col("event_json.shipping.shipping_id").alias("shipping_id"),
    col("event_json.shipping.sign_code").alias("sign_code"),
    col("event_json.shipping.brand_code_alpha").alias("brand_code_alpha"),

    col("event_json.shipping.collection.round.codeAgence").alias("collection_codeAgence"),
    col("event_json.shipping.collection.round.pays").alias("collection_pays"),

    col("event_json.shipping.delivery.round.codeAgence").alias("delivery_codeAgence"),
    col("event_json.shipping.delivery.round.pays").alias("delivery_pays"),

    col("event_json.shipping.sav_folder").alias("sav_folder"),
    col("event_json.shipping.is_replaced").alias("is_replaced"),
    col("event_json.shipping.parcel_number").alias("parcel_number"),
    col("event_json.shipping.parcel_sequence").alias("parcel_sequence"),
    col("event_json.shipping.shipping_number").alias("shipping_number"),

    col("event_json.shipping.state.code").alias("state_code"),
    col("event_json.shipping.state.date").alias("state_date"),
    col("event_json.shipping.state.nature").alias("state_nature"),
    col("event_json.shipping.state.sousCode").alias("state_subcode")
)

# DIM CLIENT
from pyspark.sql.functions import concat_ws

dim_client = df_flat.select(
    concat_ws("_", col("brand_code_alpha"), col("sign_code")).alias("client_key"),
    col("brand_code_alpha"),
    col("sign_code")
).filter(col("brand_code_alpha").isNotNull() & col("sign_code").isNotNull()) \
 .dropDuplicates(["client_key"]) \
 .withColumn("client_id", expr("uuid()"))

# DIM LOCATION
collection_location = df_flat.select(
    col("collection_codeAgence").alias("codeAgence"),
    col("collection_pays").alias("pays"),
    lit("collection").alias("location_type")
).dropDuplicates()

delivery_location = df_flat.select(
    col("delivery_codeAgence").alias("codeAgence"),
    col("delivery_pays").alias("pays"),
    lit("delivery").alias("location_type")
).dropDuplicates()

dim_location = collection_location.union(delivery_location) \
    .dropDuplicates() \
    .withColumn("location_id", expr("uuid()"))

# DIM STATE
dim_state = df_flat.select(
    col("state_code"),
    col("state_date"),
    col("state_nature"),
    col("state_subcode")
).dropDuplicates() \
 .withColumn("state_id", expr("uuid()"))

# DIMDATE
dim_date = df_flat.select(col("event_date")) \
    .withColumn("date", to_date("event_date")) \
    .select(
        col("date"),
        year("date").alias("year"),
        month("date").alias("month"),
        dayofmonth("date").alias("day"),
        quarter("date").alias("quarter"),
        dayofweek("date").alias("weekday")
    ).dropDuplicates() \
    .withColumn("date_id", expr("uuid()"))

# DIMSHIPPING
df_shipping = df_flat.alias("d") \
    .join(dim_client.alias("c"),
          concat_ws("_", col("d.brand_code_alpha"), col("d.sign_code")) == col("c.client_key"),
          "left") \
    .join(dim_location.alias("loc_col"),
          (col("d.collection_codeAgence") == col("loc_col.codeAgence")) &
          (col("d.collection_pays") == col("loc_col.pays")) &
          (col("loc_col.location_type") == lit("collection")),
          "left") \
    .join(dim_location.alias("loc_del"),
          (col("d.delivery_codeAgence") == col("loc_del.codeAgence")) &
          (col("d.delivery_pays") == col("loc_del.pays")) &
          (col("loc_del.location_type") == lit("delivery")),
          "left") \
    .join(dim_state.alias("st"),
          (col("d.state_code") == col("st.state_code")) &
          (col("d.state_date") == col("st.state_date")) &
          (col("d.state_nature") == col("st.state_nature")) &
          (col("d.state_subcode") == col("st.state_subcode")),
          "left")

dim_shipping = df_shipping.select(
    col("shipping_id"),
    col("c.client_id"),
    col("loc_col.location_id").alias("collection_location_id"),
    col("loc_del.location_id").alias("delivery_location_id"),
    col("st.state_id"),
    col("sav_folder"),
    col("is_replaced"),
    col("parcel_number"),
    col("parcel_sequence")
).dropDuplicates(["shipping_id"]).where(col("shipping_id").isNotNull())

# FACT SHIPPING EVENTS
fact_shipping_event = df_flat.alias("d") \
    .join(dim_shipping.alias("s"),
          col("d.shipping_id") == col("s.shipping_id"),
          "left") \
    .join(dim_date.alias("dt"),
          to_date(col("d.event_date")) == col("dt.date"),
          "left") \
    .select(
        expr("uuid()").alias("event_id"),
        col("d.event_code"),
        col("d.event_sub_code"),
        col("d.event_date"),
        col("d.entry_date"),
        col("d.event_nature"),
        col("s.shipping_id"),
        col("dt.date_id")
    )

# checks on final tables
dq_results += run_dq_checks(dim_client, "After Tables Build: DimClient")
dq_results += run_dq_checks(dim_location, "After Tables Build: DimLocation")
dq_results += run_dq_checks(dim_state, "After Tables Build: DimState")
dq_results += run_dq_checks(dim_date, "After Tables Build: DimDate")
dq_results += run_dq_checks(dim_shipping, "After Tables Build: DimShipping")
dq_results += run_dq_checks(fact_shipping_event, "After Tables Build: FactShippingEvent")

# just to preview tables when running the script
print("DimClient")
dim_client.show(5, truncate=False)

print("DimLocation")
dim_location.show(5, truncate=False)

print("DimState")
dim_state.show(5, truncate=False)

print("DimDate")
dim_date.show(5, truncate=False)

print("DimShipping")
dim_shipping.show(5, truncate=False)

print("FactShippingEvent")
fact_shipping_event.show(5, truncate=False)

# WRITE PARQUETS TO /WAREHOUSE
output_base = "/workspaces/inpost_analytics/warehouse"

dim_client.write.mode("overwrite").parquet(f"{output_base}/DimClient")
dim_location.write.mode("overwrite").parquet(f"{output_base}/DimLocation")
dim_state.write.mode("overwrite").parquet(f"{output_base}/DimState")
dim_date.write.mode("overwrite").parquet(f"{output_base}/DimDate")
dim_shipping.write.mode("overwrite").parquet(f"{output_base}/DimShipping")
fact_shipping_event.write.mode("overwrite").parquet(f"{output_base}/FactShippingEvent")

# write dq checks report to etl/metadata
dq_df = pd.DataFrame(dq_results)
if os.path.exists(metadata_path):
    existing_df = pd.read_csv(metadata_path)
    combined_df = pd.concat([existing_df, dq_df], ignore_index=True)
else:
    combined_df = dq_df

combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
combined_df = combined_df.sort_values(by='timestamp', ascending=False)

# metadata csv write
combined_df.to_csv(metadata_path, index=False)

print(f"Job ran! Data Quality report in {metadata_path}")

# stop spark
spark.stop()