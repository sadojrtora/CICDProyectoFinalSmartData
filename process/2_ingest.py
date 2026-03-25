# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 2 · Ingest — Volume CSV → Bronze (E-Commerce)
# MAGIC
# MAGIC Reads each source CSV from a **Unity Catalog Volume** and writes it into
# MAGIC the corresponding **bronze** Delta table managed by Unity Catalog.
# MAGIC
# MAGIC ## Where to upload your CSV files
# MAGIC Run notebook 1 first, then upload via the Databricks UI:
# MAGIC **Catalog → catalog_ecommerce → landing → raw → Upload to this volume**
# MAGIC
# MAGIC Expected Volume paths:
# MAGIC ```
# MAGIC /Volumes/catalog_ecommerce/landing/raw/customers.csv
# MAGIC /Volumes/catalog_ecommerce/landing/raw/orders.csv
# MAGIC /Volumes/catalog_ecommerce/landing/raw/products.csv
# MAGIC /Volumes/catalog_ecommerce/landing/raw/product_reviews.csv
# MAGIC ```
# MAGIC
# MAGIC **Layer rules:**
# MAGIC - Zero business logic — data arrives exactly as the source sent it
# MAGIC - All value columns stored as StringType to prevent parse failures
# MAGIC - One derived column added per row: `ingestion_date = current_timestamp()`
# MAGIC - Write mode: `overwrite` (full refresh each run)

# COMMAND ----------

# ── Step 1: Remove any widgets left from a previous run ───────────────────────
dbutils.widgets.removeAll()

# COMMAND ----------

# ── Step 2: Import all required libraries ─────────────────────────────────────
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# ── Step 3: Declare notebook widgets ──────────────────────────────────────────
# volume_path : Unity Catalog Volume path where the CSVs were uploaded
# catalogo    : Unity Catalog catalog name
# esquema     : target schema (always 'bronze' for this notebook)

dbutils.widgets.text("volume_path", "/Volumes/catalog_ecommerce/landing/raw")
dbutils.widgets.text("catalogo",    "catalog_ecommerce")
dbutils.widgets.text("esquema",     "bronze")

# COMMAND ----------

# ── Step 4: Read widget values into Python variables ──────────────────────────
volume_path = dbutils.widgets.get("volume_path")
catalogo    = dbutils.widgets.get("catalogo")
esquema     = dbutils.widgets.get("esquema")

print(f"Volume source path : {volume_path}")
print(f"Target             : {catalogo}.{esquema}")

# COMMAND ----------

# ── Step 5: Verify CSV files exist in the Volume before reading ───────────────
# dbutils.fs.ls() lists all files at the given Volume path.
# If a file is missing you will see an error here before any writes happen.
print("Files found in Volume:")
for f in dbutils.fs.ls(volume_path):
    print(f"  {f.path}  ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1 — customers
# MAGIC Source columns: `customer_id, name, email, gender, signup_date, country`

# COMMAND ----------

# ── Step 6a: Build the full Volume path for customers.csv ─────────────────────
ruta_customers = f"{volume_path}/customers.csv"
print(f"Reading: {ruta_customers}")

# COMMAND ----------

# ── Step 6b: Define explicit schema — all value columns as StringType ──────────
# Actual columns confirmed from CSV inspection.
# All values are StringType at bronze — types are cast in the silver notebook.
customers_schema = StructType(fields=[
    StructField("customer_id", StringType(), False),  # PK — integer in source
    StructField("name",        StringType(), True),   # single full name field
    StructField("email",       StringType(), True),
    StructField("gender",      StringType(), True),   # Male / Female / Other
    StructField("signup_date", StringType(), True),   # cast to Date in silver
    StructField("country",     StringType(), True),
])

# COMMAND ----------

# ── Step 6c: Read the CSV using the explicit schema ────────────────────────────
df_customers = (spark.read
    .option("header",      True)
    .option("inferSchema", False)
    .option("escape",      '"')
    .schema(customers_schema)
    .csv(ruta_customers))

# COMMAND ----------

# ── Step 6d: Add ingestion_date audit column and write to bronze ───────────────
df_customers_final = df_customers.withColumn("ingestion_date", current_timestamp())

print(f"Customers rows read: {df_customers_final.count():,}")
df_customers_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.customers")
print(f"✓ {catalogo}.{esquema}.customers written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2 — orders
# MAGIC Source columns: `order_id, customer_id, order_date, total_amount, payment_method, shipping_country`
# MAGIC Note: no `status` column in this dataset.

# COMMAND ----------

# ── Step 7a: Build source path ────────────────────────────────────────────────
ruta_orders = f"{volume_path}/orders.csv"
print(f"Reading: {ruta_orders}")

# COMMAND ----------

# ── Step 7b: Define schema ────────────────────────────────────────────────────
orders_schema = StructType(fields=[
    StructField("order_id",         StringType(), False),  # PK
    StructField("customer_id",      StringType(), False),  # FK → customers
    StructField("order_date",       StringType(), True),   # cast to Date in silver
    StructField("total_amount",     StringType(), True),   # cast to Double in silver
    StructField("payment_method",   StringType(), True),   # enum validated in silver
    StructField("shipping_country", StringType(), True),
])

# COMMAND ----------

# ── Step 7c: Read, add audit column, write ────────────────────────────────────
df_orders = (spark.read
    .option("header",      True)
    .option("inferSchema", False)
    .option("escape",      '"')
    .schema(orders_schema)
    .csv(ruta_orders))

df_orders_final = df_orders.withColumn("ingestion_date", current_timestamp())

print(f"Orders rows read: {df_orders_final.count():,}")
df_orders_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.orders")
print(f"✓ {catalogo}.{esquema}.orders written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3 — products
# MAGIC Source columns: `product_id, product_name, category, price, stock_quantity, brand`
# MAGIC Note: no `seller_id`, `category_id`, `description` or `created_at` in this dataset.

# COMMAND ----------

# ── Step 8a: Build source path ────────────────────────────────────────────────
ruta_products = f"{volume_path}/products.csv"
print(f"Reading: {ruta_products}")

# COMMAND ----------

# ── Step 8b: Define schema ────────────────────────────────────────────────────
products_schema = StructType(fields=[
    StructField("product_id",     StringType(), False),  # PK
    StructField("product_name",   StringType(), True),
    StructField("category",       StringType(), True),   # Books / Clothing / Electronics / etc.
    StructField("price",          StringType(), True),   # cast to Double in silver
    StructField("stock_quantity", StringType(), True),   # cast to Int in silver
    StructField("brand",          StringType(), True),   # BrandA / BrandB / BrandC / BrandD
])

# COMMAND ----------

# ── Step 8c: Read, add audit column, write ────────────────────────────────────
df_products = (spark.read
    .option("header",      True)
    .option("inferSchema", False)
    .option("escape",      '"')
    .schema(products_schema)
    .csv(ruta_products))

df_products_final = df_products.withColumn("ingestion_date", current_timestamp())

print(f"Products rows read: {df_products_final.count():,}")
df_products_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.products")
print(f"✓ {catalogo}.{esquema}.products written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4 — product_reviews
# MAGIC Source columns: `review_id, product_id, customer_id, rating, review_text, review_date`

# COMMAND ----------

# ── Step 9a: Build source path ────────────────────────────────────────────────
ruta_reviews = f"{volume_path}/product_reviews.csv"
print(f"Reading: {ruta_reviews}")

# COMMAND ----------

# ── Step 9b: Define schema ────────────────────────────────────────────────────
reviews_schema = StructType(fields=[
    StructField("review_id",   StringType(), False),  # PK
    StructField("product_id",  StringType(), False),  # FK → products
    StructField("customer_id", StringType(), False),  # FK → customers
    StructField("rating",      StringType(), True),   # cast to Int (1–5) in silver
    StructField("review_text", StringType(), True),   # free text
    StructField("review_date", StringType(), True),   # cast to Date in silver
])

# COMMAND ----------

# ── Step 9c: Read, add audit column, write ────────────────────────────────────
df_reviews = (spark.read
    .option("header",      True)
    .option("inferSchema", False)
    .option("escape",      '"')
    .option("multiLine",   True)   # review_text may contain embedded newlines
    .schema(reviews_schema)
    .csv(ruta_reviews))

df_reviews_final = df_reviews.withColumn("ingestion_date", current_timestamp())

print(f"Reviews rows read: {df_reviews_final.count():,}")
df_reviews_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.product_reviews")
print(f"✓ {catalogo}.{esquema}.product_reviews written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# ── Step 10: Row-count audit across all four bronze tables ────────────────────
print("\n" + "=" * 55)
print("BRONZE INGESTION — Summary")
print("=" * 55)
for tbl in ["customers", "orders", "products", "product_reviews"]:
    cnt = spark.table(f"{catalogo}.{esquema}.{tbl}").count()
    print(f"  {catalogo}.{esquema}.{tbl:<20}  {cnt:>8,} rows")
print("=" * 55)
print("Bronze ingestion complete ✓")
