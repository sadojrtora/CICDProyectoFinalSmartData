# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 4 · Load — Silver → Golden (E-Commerce)
# MAGIC
# MAGIC Joins and aggregates the silver tables into four BI-ready golden tables.
# MAGIC All tables are Unity Catalog managed — no ADLS paths required.
# MAGIC
# MAGIC **Layer rules:**
# MAGIC - Cross-table joins happen HERE for the first time (not in silver)
# MAGIC - Business KPIs and RFM segments are computed in this layer
# MAGIC - Write mode: `overwrite` — golden tables are fully refreshed each run
# MAGIC - All tables are pre-declared in notebook 1_preparacion_ambiente (insertInto requires DDL)
# MAGIC
# MAGIC **Tables produced:**
# MAGIC `catalog_ecommerce.golden.agg_sales_summary`       — daily revenue by country
# MAGIC `catalog_ecommerce.golden.agg_product_performance` — ratings + sentiment per product
# MAGIC `catalog_ecommerce.golden.agg_customer_360`        — RFM profile per customer
# MAGIC `catalog_ecommerce.golden.agg_review_sentiment`    — monthly sentiment trends

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
dbutils.widgets.text("catalogo",       "catalog_ecommerce")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink",   "golden")

# COMMAND ----------

# ── Step 4: Read widget values ────────────────────────────────────────────────
catalogo       = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink   = dbutils.widgets.get("esquema_sink")

print(f"Source : {catalogo}.{esquema_source}")
print(f"Sink   : {catalogo}.{esquema_sink}")

# COMMAND ----------

# ── Step 5: Load all four silver tables ───────────────────────────────────────
# spark.table() reads directly from Unity Catalog — no path configuration needed.
# These references are lazy and reused across all four aggregates below.

df_customers = spark.table(f"{catalogo}.{esquema_source}.dim_customers")
df_products  = spark.table(f"{catalogo}.{esquema_source}.dim_products")
df_orders    = spark.table(f"{catalogo}.{esquema_source}.fact_orders")
df_reviews   = spark.table(f"{catalogo}.{esquema_source}.fact_reviews")

print("Silver tables loaded ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate 1 — agg_sales_summary
# MAGIC
# MAGIC **Grain:** One row per calendar date × shipping country
# MAGIC **Answers:** "How much did we sell, where, and with which payment methods?"
# MAGIC **Consumers:** Executive dashboards, daily revenue alerts

# COMMAND ----------

# ── Step 6a: Aggregate orders by date and shipping country ────────────────────
# order_date is a Date in silver — no need to truncate a timestamp.
# total_amount is a clean Double — confirmed min=0.0, max=12,092.51.

df_sales_summary = (df_orders

    .groupBy("order_date", "shipping_country")

    .agg(
        # Total orders placed on this date in this country
        count("order_id").alias("total_orders"),

        # Unique customers who ordered — one customer may place multiple orders
        countDistinct("customer_id").alias("unique_customers"),

        # Core revenue KPIs
        round(sum("total_amount"),  2).alias("gross_revenue"),
        round(avg("total_amount"),  2).alias("avg_order_value"),
        round(min("total_amount"),  2).alias("min_order_value"),
        round(max("total_amount"),  2).alias("max_order_value"),
    )

    # Rename to match the golden DDL column name
    .withColumnRenamed("order_date", "order_date_dt")

    .orderBy("order_date_dt", "shipping_country")
)

# COMMAND ----------

## Preview the information on Step 6a
## df_sales_summary.display()

# COMMAND ----------

# ── Step 6b: Write to golden ──────────────────────────────────────────────────
df_sales_summary.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.agg_sales_summary")
print(f"✓ {catalogo}.{esquema_sink}.agg_sales_summary — {df_sales_summary.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate 2 — agg_product_performance
# MAGIC
# MAGIC **Grain:** One row per product
# MAGIC **Answers:** "Which products have the best/worst ratings, and how are they priced?"
# MAGIC **Consumers:** Merchandising team, recommendation models

# COMMAND ----------

# ── Step 7a: Aggregate review metrics per product ─────────────────────────────
df_review_agg = (df_reviews
    .groupBy("product_id")
    .agg(
        count("review_id").alias("review_count"),
        round(avg("rating"), 2).alias("avg_rating"),
        sum(when(col("sentiment_bucket") == "positive", 1).otherwise(0)).alias("positive_reviews"),
        sum(when(col("sentiment_bucket") == "neutral",  1).otherwise(0)).alias("neutral_reviews"),
        sum(when(col("sentiment_bucket") == "negative", 1).otherwise(0)).alias("negative_reviews"),
    )
)

# COMMAND ----------

# ── Step 7b: Join products with review aggregation ────────────────────────────
# LEFT JOIN preserves products that have zero reviews yet.
# fillna() replaces JOIN-introduced NULLs with sensible zero defaults.
df_product_performance = (df_products

    .join(df_review_agg, on="product_id", how="left")

    .fillna({"review_count":     0,
             "positive_reviews": 0,
             "neutral_reviews":  0,
             "negative_reviews": 0})

    # Global rank by avg_rating — highest rated products rank first
    # desc_nulls_last() pushes unreviewed products to the bottom
    .withColumn("rating_rank",
        rank().over(Window.orderBy(col("avg_rating").desc_nulls_last())))

    # Flag: in-stock product with poor rating AND enough reviews to be meaningful
    # Helps merchandisers identify products that need quality attention
    .withColumn("needs_review_flag",
        (col("in_stock") == True) &
        (col("avg_rating") < 2.5) &
        (col("review_count") >= 5))

    .select(
        "product_id", "product_name", "category", "brand",
        "price", "price_category", "in_stock",
        "review_count", "avg_rating",
        "positive_reviews", "neutral_reviews", "negative_reviews",
        "rating_rank", "needs_review_flag"
    )
    .orderBy("rating_rank")
)

# COMMAND ----------

## Preview the information on Step 7b
## df_product_performance.display()


# COMMAND ----------

# ── Step 7c: Write to golden ──────────────────────────────────────────────────
df_product_performance.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.agg_product_performance")
print(f"✓ {catalogo}.{esquema_sink}.agg_product_performance — {df_product_performance.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate 3 — agg_customer_360
# MAGIC
# MAGIC **Grain:** One row per customer
# MAGIC **Answers:** "Who are our most valuable customers and how do we segment them?"
# MAGIC **Consumers:** CRM, marketing automation, churn-prediction models
# MAGIC
# MAGIC ### RFM Scoring Reference (each dimension scored 1–5, composite range 3–15)
# MAGIC | Score | Recency (days since order) | Frequency (total orders) | Monetary (lifetime spend) |
# MAGIC |-------|---------------------------|--------------------------|--------------------------|
# MAGIC | 5 | ≤ 30  | ≥ 20 | ≥ $5,000 |
# MAGIC | 4 | ≤ 60  | ≥ 10 | ≥ $1,000 |
# MAGIC | 3 | ≤ 90  | ≥ 5  | ≥ $500   |
# MAGIC | 2 | ≤ 180 | ≥ 2  | ≥ $100   |
# MAGIC | 1 | > 180 | 1    | < $100   |
# MAGIC
# MAGIC ### Segment Labels
# MAGIC | Composite RFM | Segment            |
# MAGIC |---------------|--------------------|
# MAGIC | 13 – 15       | Champions          |
# MAGIC | 10 – 12       | Loyal              |
# MAGIC | 7 – 9         | Potential Loyalist  |
# MAGIC | 5 – 6         | At Risk            |
# MAGIC | 3 – 4         | Hibernating        |

# COMMAND ----------

# ── Step 8a: Aggregate order behaviour per customer ───────────────────────────
df_customer_orders = (df_orders
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("total_amount"), 2).alias("lifetime_value"),
        round(avg("total_amount"), 2).alias("avg_order_value"),
        max("order_date").alias("last_order_date"),
        min("order_date").alias("first_order_date"),
    )
)

# COMMAND ----------

# ── Step 8b: Derive the preferred payment method per customer ──────────────────
# Find the payment method each customer used most frequently.
# This is a useful CRM signal for targeted payment promotions.

# Count uses per customer + method, then rank to find the top method per customer
window_payment = Window.partitionBy("customer_id").orderBy(col("method_count").desc())

df_preferred_payment = (df_orders
    .groupBy("customer_id", "payment_method")
    .agg(count("order_id").alias("method_count"))
    .withColumn("_rn", row_number().over(window_payment))
    .filter(col("_rn") == 1)
    .drop("_rn", "method_count")
    .withColumnRenamed("payment_method", "preferred_payment")
)

# COMMAND ----------

# ── Step 8c: Aggregate review behaviour per customer ──────────────────────────
df_customer_reviews = (df_reviews
    .groupBy("customer_id")
    .agg(
        count("review_id").alias("reviews_written"),
        round(avg("rating"), 2).alias("avg_review_given"),
    )
)

# COMMAND ----------

# ── Step 8d: Join customers + order stats + preferred payment + review stats ───
# LEFT JOINs so customers who have never ordered still appear in the table
# (new sign-ups may have no orders yet).
df_customer_360 = (df_customers

    .join(df_customer_orders,    on="customer_id", how="left")
    .join(df_preferred_payment,  on="customer_id", how="left")
    .join(df_customer_reviews,   on="customer_id", how="left")

    # Recency: days since the customer's last order
    .withColumn("days_since_last_order",
        datediff(current_date(), col("last_order_date")))

    # Tenure: days since the customer signed up
    .withColumn("tenure_days",
        datediff(current_date(), col("signup_date")))

    # ── RFM dimension scores (1 = lowest, 5 = highest) ───────────────────────

    # RECENCY — lower days_since_last_order is better
    .withColumn("recency_score",
        when(col("days_since_last_order") <= 30,  lit(5))
        .when(col("days_since_last_order") <= 60,  lit(4))
        .when(col("days_since_last_order") <= 90,  lit(3))
        .when(col("days_since_last_order") <= 180, lit(2))
        .otherwise(lit(1)))

    # FREQUENCY — higher total_orders is better
    .withColumn("frequency_score",
        when(col("total_orders") >= 20, lit(5))
        .when(col("total_orders") >= 10, lit(4))
        .when(col("total_orders") >= 5,  lit(3))
        .when(col("total_orders") >= 2,  lit(2))
        .otherwise(lit(1)))

    # MONETARY — higher lifetime_value is better
    .withColumn("monetary_score",
        when(col("lifetime_value") >= 5000, lit(5))
        .when(col("lifetime_value") >= 1000, lit(4))
        .when(col("lifetime_value") >= 500,  lit(3))
        .when(col("lifetime_value") >= 100,  lit(2))
        .otherwise(lit(1)))

    # Composite RFM score: sum of all three dimensions (range 3–15)
    .withColumn("rfm_score",
        col("recency_score") + col("frequency_score") + col("monetary_score"))

    # Segment label — used directly in CRM and marketing automation tools
    .withColumn("customer_segment",
        when(col("rfm_score") >= 13, lit("Champions"))
        .when(col("rfm_score") >= 10, lit("Loyal"))
        .when(col("rfm_score") >= 7,  lit("Potential Loyalist"))
        .when(col("rfm_score") >= 5,  lit("At Risk"))
        .otherwise(lit("Hibernating")))

    .select(
        "customer_id", "name", "email", "gender", "country",
        "signup_date", "tenure_days",
        "total_orders", "lifetime_value", "avg_order_value",
        "last_order_date", "first_order_date", "days_since_last_order",
        "reviews_written", "avg_review_given",
        "preferred_payment",
        "recency_score", "frequency_score", "monetary_score",
        "rfm_score", "customer_segment"
    )
)

# COMMAND ----------

# ── Step 8e: Write to golden ──────────────────────────────────────────────────
df_customer_360.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.agg_customer_360")
print(f"✓ {catalogo}.{esquema_sink}.agg_customer_360 — {df_customer_360.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate 4 — agg_review_sentiment
# MAGIC
# MAGIC **Grain:** One row per calendar month × product
# MAGIC **Answers:** "How is customer sentiment trending for each product over time?"
# MAGIC **Consumers:** Product quality team, NPS dashboards

# COMMAND ----------

# ── Step 9a: Join reviews with products to enrich with name, category and brand ─
# LEFT JOIN preserves reviews for products that may no longer exist in the catalog.
df_reviews_enriched = (df_reviews
    .join(
        df_products.select("product_id", "product_name", "category", "brand"),
        on="product_id",
        how="left"
    )
)

# COMMAND ----------

# ── Step 9b: Add a yyyy-MM month column for time-series grouping ──────────────
# date_format on a Date column returns a sortable string: "2024-03"
df_reviews_monthly = df_reviews_enriched.withColumn(
    "review_month", date_format(col("review_date"), "yyyy-MM")
)

# COMMAND ----------

# ── Step 9c: Aggregate by month × product ─────────────────────────────────────
df_review_sentiment = (df_reviews_monthly

    .groupBy("review_month", "product_id", "product_name", "category", "brand")

    .agg(
        count("review_id").alias("total_reviews"),
        round(avg("rating"), 2).alias("avg_rating"),
        sum(when(col("sentiment_bucket") == "positive", 1).otherwise(0)).alias("positive_count"),
        sum(when(col("sentiment_bucket") == "neutral",  1).otherwise(0)).alias("neutral_count"),
        sum(when(col("sentiment_bucket") == "negative", 1).otherwise(0)).alias("negative_count"),
    )

    # Percentage splits — useful for stacked bar charts in Power BI
    .withColumn("positive_pct",
        round(col("positive_count") / col("total_reviews") * 100, 1))
    .withColumn("negative_pct",
        round(col("negative_count") / col("total_reviews") * 100, 1))

    # Month-over-month rating change per product
    # LAG() looks back one row in the window ordered chronologically
    .withColumn("prev_month_avg_rating",
        lag("avg_rating", 1).over(
            Window.partitionBy("product_id").orderBy("review_month")
        ))
    .withColumn("rating_mom_change",
        round(col("avg_rating") - col("prev_month_avg_rating"), 2))

    .orderBy("product_id", "review_month")
)

# COMMAND ----------

## Preview Step 9c
display(df_review_sentiment.limit(10))

# COMMAND ----------

# ── Step 9d: Write to golden ──────────────────────────────────────────────────
df_review_sentiment.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.agg_review_sentiment")
print(f"✓ {catalogo}.{esquema_sink}.agg_review_sentiment — {df_review_sentiment.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# ── Step 10: Final row-count audit across all golden tables ───────────────────
GOLDEN_TABLES = [
    "agg_sales_summary",
    "agg_product_performance",
    "agg_customer_360",
    "agg_review_sentiment",
]

print("\n" + "=" * 60)
print("GOLDEN LOAD — Summary")
print("=" * 60)
print(f"  {'Table':<35} {'Rows':>10}")
print("  " + "-" * 45)
for tbl in GOLDEN_TABLES:
    cnt = spark.table(f"{catalogo}.{esquema_sink}.{tbl}").count()
    print(f"  {tbl:<35} {cnt:>10,}")
print("=" * 60)
print("Golden load complete ✓  — Tables ready for Power BI / Synapse")
