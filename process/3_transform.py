# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 3 · Transform — Bronze → Silver (E-Commerce)
# MAGIC
# MAGIC Reads from **bronze**, applies data quality rules and type casts,
# MAGIC derives business columns, and writes clean tables to **silver**.
# MAGIC
# MAGIC All transformations are based on the **confirmed actual CSV schema**:
# MAGIC
# MAGIC | Table | Actual columns |
# MAGIC |---|---|
# MAGIC | customers | customer_id, name, email, gender, signup_date, country |
# MAGIC | orders | order_id, customer_id, order_date, total_amount, payment_method, shipping_country |
# MAGIC | products | product_id, product_name, category, price, stock_quantity, brand |
# MAGIC | product_reviews | review_id, product_id, customer_id, rating, review_text, review_date |
# MAGIC
# MAGIC **Layer rules:**
# MAGIC - Cast every column to its correct type (Double, Date, Int)
# MAGIC - Validate data quality: nulls, value ranges, enum sets
# MAGIC - Deduplicate — keep the latest record per primary key
# MAGIC - Derive business columns (price_category, in_stock, sentiment_bucket, etc.)
# MAGIC - Write mode: `overwrite` into pre-declared silver DDL tables
# MAGIC - NO cross-table joins — each silver table is built independently

# COMMAND ----------

# ── Step 1: Remove stale widgets ──────────────────────────────────────────────
dbutils.widgets.removeAll()

# COMMAND ----------

# ── Step 2: Import libraries ──────────────────────────────────────────────────
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# ── Step 3: Declare widgets ───────────────────────────────────────────────────
# esquema_source = schema we read from  (bronze)
# esquema_sink   = schema we write into (silver)

dbutils.widgets.text("catalogo",       "catalog_ecommerce")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink",   "silver")

# COMMAND ----------

# ── Step 4: Read widget values ────────────────────────────────────────────────
catalogo       = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink   = dbutils.widgets.get("esquema_sink")

print(f"Source : {catalogo}.{esquema_source}")
print(f"Sink   : {catalogo}.{esquema_sink}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared UDFs
# MAGIC
# MAGIC Declared once here and reused across all four table transformations.
# MAGIC UDFs encapsulate business rules that cannot be expressed cleanly
# MAGIC with built-in Spark functions.

# COMMAND ----------

# ── Step 5a: UDF — price_category ─────────────────────────────────────────────
# Confirmed price range from data: 5.02 → 499.94 (no negatives, no symbols)
# Buckets the numeric price into a human-readable tier for BI filtering.

def precio_categoria(price):
    """Classify a product price into a business-friendly tier."""
    if price is None:
        return "Unknown"
    elif price < 50:
        return "Low"       # $0 – $49.99
    elif price < 200:
        return "Medium"    # $50 – $199.99
    else:
        return "High"      # $200+

precio_categoria_udf = udf(precio_categoria, StringType())

# COMMAND ----------

# ── Step 5b: UDF — sentiment_bucket ───────────────────────────────────────────
# Confirmed rating values from data: integers 1, 2, 3, 4, 5 — evenly distributed.
# Maps numeric rating to a sentiment label used in golden aggregations.

def sentimiento_bucket(rating):
    """Map a numeric star rating (1–5) to a sentiment label."""
    if rating is None:
        return None
    elif rating >= 4:
        return "positive"   # 4 or 5 stars
    elif rating == 3:
        return "neutral"    # 3 stars
    else:
        return "negative"   # 1 or 2 stars

sentimiento_bucket_udf = udf(sentimiento_bucket, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1 — dim_customers

# COMMAND ----------

# ── Step 6a: Read from bronze ──────────────────────────────────────────────────
# Confirmed columns: customer_id, name, email, gender, signup_date, country
# No nulls observed in sample — all 100k rows complete.
df_customers = spark.table(f"{catalogo}.{esquema_source}.customers")

# COMMAND ----------

# ── Step 6b: Deduplicate by primary key ───────────────────────────────────────
# ROW_NUMBER() over a window keeps only the most recently ingested row
# per customer_id in case the same CSV is ingested more than once.
window_customers = Window.partitionBy("customer_id").orderBy(col("ingestion_date").desc())

df_customers = (df_customers
    .withColumn("_rn", row_number().over(window_customers))
    .filter(col("_rn") == 1)
    .drop("_rn"))

# COMMAND ----------

# ── Step 6c: Data quality — drop rows missing critical fields ──────────────────
# customer_id and email are required for downstream joins and gold aggregations.
total_before = df_customers.count()
df_customers = (df_customers
    .dropna(how="all")
    .filter(col("customer_id").isNotNull() & col("email").isNotNull()))
print(f"  [DQ] customers: {total_before - df_customers.count():,} rows dropped (null PK or email)")

# COMMAND ----------

# ── Step 6d: Apply all transformations ────────────────────────────────────────
# NOTE: name is a single column in this dataset (not first_name + last_name).
#       signup_date is the actual column name (not registration_date).
#       No phone, address or city columns exist in this dataset.

df_customers_silver = (df_customers

    # Trim whitespace and title-case the full name
    .withColumn("name", initcap(trim(col("name"))))

    # Standardise email: lowercase + trim for consistent downstream joins
    .withColumn("email", lower(trim(col("email"))))

    # Standardise gender to title-case for consistency (Male / Female / Other)
    .withColumn("gender", when(col("gender").isin("M", "m"), "Male")
                         .when(col("gender").isin("F", "f"), "Female")
                         .otherwise(col("gender")))
    .withColumn("gender", initcap(trim(col("gender"))))

    # Cast signup_date from String → Date
    # Format confirmed from data: yyyy-MM-dd
    # Rows with an unparseable value become NULL — the pipeline does not crash
    .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))

    # Standardise country to title-case (e.g. "cape verde" → "Cape Verde")
    .withColumn("country", initcap(trim(col("country"))))

    # Derived flag: basic syntactic email validation via REGEX
    .withColumn("is_valid_email",
        col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))

    # Audit timestamp: when was this row processed into silver?
    .withColumn("silver_ts", current_timestamp())

    .select(
        "customer_id", "name", "email", "gender",
        "signup_date", "country",
        "is_valid_email",
        "ingestion_date", "silver_ts"
    )
)

# COMMAND ----------

# ── Step 6e: Write to silver ──────────────────────────────────────────────────
df_customers_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.dim_customers")
print(f"✓ {catalogo}.{esquema_sink}.dim_customers — {df_customers_silver.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2 — dim_products

# COMMAND ----------

# ── Step 7a: Read from bronze ─────────────────────────────────────────────────
# Confirmed columns: product_id, product_name, category, price, stock_quantity, brand
# No nulls in sample. Price range: 5.02–499.94. Stock: 0–1000.
df_products = spark.table(f"{catalogo}.{esquema_source}.products")

# COMMAND ----------

# ── Step 7b: Deduplicate by product_id ───────────────────────────────────────
window_products = Window.partitionBy("product_id").orderBy(col("ingestion_date").desc())

df_products = (df_products
    .withColumn("_rn", row_number().over(window_products))
    .filter(col("_rn") == 1)
    .drop("_rn"))

# COMMAND ----------

# ── Step 7c: DQ — product_id must be present ─────────────────────────────────
total_before = df_products.count()
df_products = (df_products
    .dropna(how="all")
    .filter(col("product_id").isNotNull()))
print(f"  [DQ] products: {total_before - df_products.count():,} rows dropped (null PK)")

# COMMAND ----------

# ── Step 7d: Apply transformations ────────────────────────────────────────────
# Confirmed categories: Books / Clothing / Electronics / Beauty / Toys / Home
# Confirmed brands    : BrandA / BrandB / BrandC / BrandD
# Price range confirmed clean — no currency symbols, no negatives
VALID_CATEGORIES = ["books", "clothing", "electronics", "beauty", "toys", "home"]
VALID_BRANDS     = ["branda", "brandb", "brandc", "brandd"]

df_products_silver = (df_products

    # Trim and title-case product name
    .withColumn("product_name", initcap(trim(col("product_name"))))

    # Standardise category to title-case (Books / Clothing / etc.)
    .withColumn("category", initcap(trim(col("category"))))

    # Standardise brand to title-case
    .withColumn("brand", initcap(trim(col("brand"))))

    # Cast price from String → Double
    # Confirmed clean decimal format in source — no symbols or commas
    .withColumn("price", col("price").cast(DoubleType()))
    # Safety guard: reject any negative prices that may appear in future loads
    .withColumn("price",
        when(col("price") >= 0, col("price")).otherwise(lit(None)))

    # Cast stock_quantity from String → Int
    .withColumn("stock_quantity", col("stock_quantity").cast(IntegerType()))
    # Safety guard: reject any negative stock values
    .withColumn("stock_quantity",
        when(col("stock_quantity") >= 0, col("stock_quantity")).otherwise(lit(None)))

    # Derived: UDF classifies price into Low ($0–$49) / Medium ($50–$199) / High ($200+)
    .withColumn("price_category", precio_categoria_udf(col("price")))

    # Derived flag: is the product currently in stock?
    # stock_quantity = 0 means out of stock (confirmed minimum value in data)
    # NULL stock is treated as out-of-stock (conservative assumption)
    .withColumn("in_stock", coalesce(col("stock_quantity"), lit(0)) > 0)

    .withColumn("silver_ts", current_timestamp())

    # Column order must exactly match the DDL in notebook 1.
    # insertInto() writes by position — wrong order = silently corrupted data.
    # DDL order: product_id, product_name, category, price, stock_quantity, brand, ...
    .select(
        "product_id", "product_name", "category",
        "price", "stock_quantity", "brand",
        "price_category", "in_stock",
        "ingestion_date", "silver_ts"
    )
)

# COMMAND ----------

# ── Step 7e: Write to silver ──────────────────────────────────────────────────
df_products_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.dim_products")
print(f"✓ {catalogo}.{esquema_sink}.dim_products — {df_products_silver.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3 — fact_orders

# COMMAND ----------

# ── Step 8a: Read from bronze ─────────────────────────────────────────────────
# Confirmed columns: order_id, customer_id, order_date, total_amount,
#                    payment_method, shipping_country
# IMPORTANT: there is NO status column in this dataset.
# total_amount is a clean decimal — no currency symbols, no negatives (min=0.0).
df_orders = spark.table(f"{catalogo}.{esquema_source}.orders")

# COMMAND ----------

# ── Step 8b: Deduplicate by order_id ─────────────────────────────────────────
window_orders = Window.partitionBy("order_id").orderBy(col("ingestion_date").desc())

df_orders = (df_orders
    .withColumn("_rn", row_number().over(window_orders))
    .filter(col("_rn") == 1)
    .drop("_rn"))

# COMMAND ----------

# ── Step 8c: DQ — order_id and customer_id must be present ───────────────────
total_before = df_orders.count()
df_orders = (df_orders
    .dropna(how="all")
    .filter(col("order_id").isNotNull() & col("customer_id").isNotNull()))
print(f"  [DQ] orders: {total_before - df_orders.count():,} rows dropped (null PK or FK)")

# COMMAND ----------

# ── Step 8d: Define valid payment method enum ─────────────────────────────────
# Confirmed from data: exactly four values, all with title-case formatting.
VALID_PAYMENT_METHODS = ["bank transfer", "paypal", "cash", "credit card"]

# COMMAND ----------

# ── Step 8e: Apply transformations ────────────────────────────────────────────
df_orders_silver = (df_orders

    # Cast order_date from String → Date
    # Format confirmed: yyyy-MM-dd (date only, no time component)
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

    # Derive integer date key (YYYYMMDD) — useful for date-dimension joins in golden
    .withColumn("order_date_key",
        date_format(col("order_date"), "yyyyMMdd").cast(IntegerType()))

    # Cast total_amount from String → Double
    # Confirmed clean: no currency symbols, no commas, min=0.0
    .withColumn("total_amount", col("total_amount").cast(DoubleType()))
    # Safety guard for future loads
    .withColumn("total_amount",
        when(col("total_amount") >= 0, col("total_amount")).otherwise(lit(None)))

    # Standardise payment_method to lowercase for consistent validation
    .withColumn("payment_method", lower(trim(col("payment_method"))))

    # Derived flag: is this a known payment method value?
    .withColumn("is_valid_payment",
        col("payment_method").isin(VALID_PAYMENT_METHODS))

    # Standardise shipping_country to title-case
    .withColumn("shipping_country", initcap(trim(col("shipping_country"))))

    .withColumn("silver_ts", current_timestamp())

    .select(
        "order_id", "customer_id",
        "order_date", "order_date_key",
        "total_amount",
        "payment_method", "is_valid_payment",
        "shipping_country",
        "ingestion_date", "silver_ts"
    )
)

# COMMAND ----------

# ── Step 8f: Write to silver ──────────────────────────────────────────────────
df_orders_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.fact_orders")
print(f"✓ {catalogo}.{esquema_sink}.fact_orders — {df_orders_silver.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4 — fact_reviews

# COMMAND ----------

# ── Step 9a: Read from bronze ─────────────────────────────────────────────────
# Confirmed columns: review_id, product_id, customer_id, rating, review_text, review_date
# Ratings confirmed as integers 1–5, evenly distributed (~1000 per rating value).
# No nulls observed in sample.
df_reviews = spark.table(f"{catalogo}.{esquema_source}.product_reviews")

# COMMAND ----------

# ── Step 9b: Deduplicate by review_id ────────────────────────────────────────
window_reviews = Window.partitionBy("review_id").orderBy(col("ingestion_date").desc())

df_reviews = (df_reviews
    .withColumn("_rn", row_number().over(window_reviews))
    .filter(col("_rn") == 1)
    .drop("_rn"))

# COMMAND ----------

# ── Step 9c: DQ — all three identifier columns must be present ────────────────
total_before = df_reviews.count()
df_reviews = (df_reviews
    .dropna(how="all")
    .filter(
        col("review_id").isNotNull() &
        col("product_id").isNotNull() &
        col("customer_id").isNotNull()))
print(f"  [DQ] product_reviews: {total_before - df_reviews.count():,} rows dropped (null PK/FK)")

# COMMAND ----------

# ── Step 9d: Apply transformations ────────────────────────────────────────────
df_reviews_silver = (df_reviews

    # Cast rating from String → Int
    # Confirmed values: 1, 2, 3, 4, 5 (integers, no decimals like 4.0)
    .withColumn("rating", col("rating").cast(IntegerType()))

    # Business rule: valid ratings are strictly 1–5
    # Values outside this range become NULL (flagged for investigation)
    .withColumn("rating",
        when((col("rating") >= 1) & (col("rating") <= 5), col("rating"))
        .otherwise(lit(None)))

    # Cast review_date from String → Date
    # Format confirmed: yyyy-MM-dd
    .withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))

    # Derived: character count of the review text
    # Useful for filtering low-effort reviews in analytical queries
    .withColumn("review_length", length(col("review_text")))

    # Derived: UDF maps rating to positive (4–5) / neutral (3) / negative (1–2)
    # NULL rating produces NULL sentiment (not forced to a default)
    .withColumn("sentiment_bucket", sentimiento_bucket_udf(col("rating")))

    .withColumn("silver_ts", current_timestamp())

    .select(
        "review_id", "product_id", "customer_id",
        "rating", "review_text", "review_length",
        "review_date", "sentiment_bucket",
        "ingestion_date", "silver_ts"
    )
)

# COMMAND ----------

# ── Step 9e: Write to silver ──────────────────────────────────────────────────
df_reviews_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.fact_reviews")
print(f"✓ {catalogo}.{esquema_sink}.fact_reviews — {df_reviews_silver.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# ── Step 10: Final row-count audit ────────────────────────────────────────────
print("\n" + "=" * 55)
print("SILVER TRANSFORMATION — Summary")
print("=" * 55)
for tbl in ["dim_customers", "dim_products", "fact_orders", "fact_reviews"]:
    cnt = spark.table(f"{catalogo}.{esquema_sink}.{tbl}").count()
    print(f"  {catalogo}.{esquema_sink}.{tbl:<20}  {cnt:>8,} rows")
print("=" * 55)
print("Silver transformation complete ✓")
