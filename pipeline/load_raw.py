from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import IntegerType, BooleanType, TimestampType, StringType

# Start Spark session
spark = SparkSession.builder.appName("FlatFactTable").getOrCreate()

# Step 1: Read input CSV
df = spark.read.option("header", True) \
               .option("escape", '"') \
               .option("multiLine", True) \
               .csv("/workspaces/inpost_analytics/input/Dane_zadanie_rekrutacyjne.csv")

# Step 2: Infer JSON schema from one sample
json_sample = df.select("event").filter(col("event").isNotNull()).limit(1).collect()[0][0]
inferred_schema = spark.read.json(spark.sparkContext.parallelize([json_sample])).schema

# Step 3: Parse JSON
df_parsed = df.withColumn("event_parsed", from_json(col("event"), inferred_schema))

# Step 4: Flatten into a wide fact table
fact = df_parsed.select(
    col("event_parsed.entry_date").cast(TimestampType()).alias("entry_date"),
    col("event_parsed.event_code").cast(StringType()).alias("event_code"),
    col("event_parsed.event_date").cast(TimestampType()).alias("event_date"),
    col("event_parsed.event_nature").cast(StringType()).alias("event_nature"),
    col("event_parsed.event_sub_code").cast(StringType()).alias("event_sub_code"),

    col("event_parsed.shipping.sign_code").cast(StringType()).alias("shipping_sign_code"),
    col("event_parsed.shipping.brand_code_alpha").cast(StringType()).alias("shipping_brand_code_alpha"),

    col("event_parsed.shipping.collection.prestation_code").cast(StringType()).alias("shipping_collection_prestation_code"),
    col("event_parsed.shipping.collection.round.codeAgence").cast(IntegerType()).alias("shipping_collection_round_codeAgence"),
    col("event_parsed.shipping.collection.round.pays").cast(StringType()).alias("shipping_collection_round_pays"),

    col("event_parsed.shipping.sav_folder").cast(BooleanType()).alias("shipping_sav_folder"),
    col("event_parsed.shipping.parcel_number").cast(IntegerType()).alias("shipping_parcel_number"),
    col("event_parsed.shipping.parcel_sequence").cast(IntegerType()).alias("shipping_parcel_sequence"),
    col("event_parsed.shipping.is_replaced").cast(BooleanType()).alias("shipping_is_replaced"),
    col("event_parsed.shipping.shipping_id").cast(StringType()).alias("shipping_shipping_id"),
    col("event_parsed.shipping.shipping_number").cast(StringType()).alias("shipping_shipping_number"),

    col("event_parsed.shipping.delivery.prestation_code").cast(StringType()).alias("shipping_delivery_prestation_code"),
    col("event_parsed.shipping.delivery.round.codeAgence").cast(IntegerType()).alias("shipping_delivery_round_codeAgence"),
    col("event_parsed.shipping.delivery.round.pays").cast(StringType()).alias("shipping_delivery_round_pays"),

    col("event_parsed.shipping.state.code").cast(StringType()).alias("shipping_state_code"),
    col("event_parsed.shipping.state.date").cast(TimestampType()).alias("shipping_state_date"),
    col("event_parsed.shipping.state.nature").cast(StringType()).alias("shipping_state_nature"),
    col("event_parsed.shipping.state.sousCode").cast(StringType()).alias("shipping_state_sousCode"),

    to_date(col("event_parsed.event_date")).alias("event_date_only")
)

# Check 1: Count total rows
total_rows = fact.count()
print(f"Total rows after flattening: {total_rows}")

# Check 2: Count rows with any NULL critical columns (shipping_shipping_id, event_date)
null_critical = fact.filter(
    (col("shipping_shipping_id").isNull()) | 
    (col("event_date").isNull())
).count()
print(f"Rows with NULL in shipping_shipping_id or event_date: {null_critical}")

# Check 3: Check unique shipping IDs (should be >0 and reasonable)
unique_ship_ids = fact.select("shipping_shipping_id").distinct().count()
print(f"Unique shipping IDs: {unique_ship_ids}")

# Check 4: Check for duplicates by shipping_shipping_id + event_date + event_code (example composite key)
dup_count = fact.groupBy("shipping_shipping_id", "event_date", "event_code") \
    .count() \
    .filter("count > 1") \
    .count()
print(f"Duplicate event records based on shipping ID + event_date + event_code: {dup_count}")

# Check 5: Optional: Validate data ranges for some numeric columns (e.g. parcel_number > 0)
invalid_parcel_numbers = fact.filter((col("shipping_parcel_number").isNotNull()) & (col("shipping_parcel_number") <= 0)).count()
print(f"Rows with invalid parcel_number (<=0): {invalid_parcel_numbers}")

# Raise error or warning if any of these checks fail (optional)
if null_critical > 0:
    print("⚠️ Warning: Found NULLs in critical columns.")
if dup_count > 0:
    print("⚠️ Warning: Found duplicate event records.")
if invalid_parcel_numbers > 0:
    print("⚠️ Warning: Found invalid parcel numbers.")

# Step 5: Save to CSV
fact.coalesce(1).write.mode("overwrite").csv("/workspaces/inpost_analytics/pipeline/parsed_source.csv", header=True)

print("✅ Flat fact table written to 'pipeline/parsed_source.csv'")