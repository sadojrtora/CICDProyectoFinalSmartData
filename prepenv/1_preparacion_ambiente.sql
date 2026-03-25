-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1 · Preparación del Ambiente — E-Commerce Medallion
-- MAGIC ## Unity Catalog Managed Storage with Volumes (no ADLS required)
-- MAGIC
-- MAGIC This notebook creates all infrastructure objects in order:
-- MAGIC   1. Unity Catalog catalog
-- MAGIC   2. Schemas: bronze / silver / golden
-- MAGIC   3. A Unity Catalog **Volume** as the CSV landing zone
-- MAGIC   4. DDL table definitions per layer
-- MAGIC
-- MAGIC **No External Locations or Storage Credentials are needed.**
-- MAGIC Unity Catalog manages all physical storage automatically. (Limitations due to account ban)
-- MAGIC
-- MAGIC **Run this once** before executing any ingest, transform or load notebook.
-- MAGIC To reset the environment, run the DROP block at the bottom.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Catalog & Schemas

-- COMMAND ----------

-- ── Step 1: Create the Unity Catalog catalog ──────────────────────────────────
-- Without a MANAGED LOCATION clause, Databricks uses the workspace's default
-- metastore root storage. Unity Catalog handles all physical paths internally.

CREATE CATALOG IF NOT EXISTS catalog_ecommerce
COMMENT 'E-Commerce Medallion Architecture — Unity Catalog managed storage';

-- COMMAND ----------

-- ── Step 2: Create one schema per Medallion layer ─────────────────────────────
-- Each schema is a logical namespace inside the catalog.
-- Unity Catalog automatically assigns a managed storage path to each schema.

CREATE SCHEMA IF NOT EXISTS catalog_ecommerce.bronze
COMMENT 'Bronze layer — raw ingested data, append-only';

CREATE SCHEMA IF NOT EXISTS catalog_ecommerce.silver
COMMENT 'Silver layer — cleaned, typed and validated data';

CREATE SCHEMA IF NOT EXISTS catalog_ecommerce.golden
COMMENT 'Golden layer — BI-ready aggregated tables';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Landing Volume
-- MAGIC
-- MAGIC A Unity Catalog **Volume** is a managed storage location governed by
-- MAGIC Unity Catalog like any other object. It replaces DBFS `/FileStore/`
-- MAGIC as the recommended place to store files in a modern Databricks workspace.
-- MAGIC
-- MAGIC **How to upload your CSVs after running this notebook:**
-- MAGIC   1. In the Databricks UI go to **Catalog → catalog_ecommerce → landing → raw**
-- MAGIC   2. Click **Upload to this volume**
-- MAGIC   3. Upload your four CSV files:
-- MAGIC      - `customers.csv`
-- MAGIC      - `orders.csv`
-- MAGIC      - `products.csv`
-- MAGIC      - `product_reviews.csv`
-- MAGIC
-- MAGIC Files will be accessible at:
-- MAGIC `/Volumes/catalog_ecommerce/landing/raw/<file>.csv`

-- COMMAND ----------

-- ── Step 3: Create a dedicated schema for the landing Volume ──────────────────
-- Keeping the landing Volume in its own schema gives it independent
-- access controls from the bronze/silver/golden data schemas.

CREATE SCHEMA IF NOT EXISTS catalog_ecommerce.landing
COMMENT 'Landing zone schema — holds the raw CSV Volume';

-- COMMAND ----------

-- ── Step 4: Create the managed Volume inside the landing schema ───────────────
-- MANAGED means Unity Catalog controls the storage path automatically.
-- Accessible at: /Volumes/catalog_ecommerce/landing/raw/

CREATE VOLUME IF NOT EXISTS catalog_ecommerce.landing.raw
COMMENT 'Landing volume — upload raw CSV files here before running ingest';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Tables
-- MAGIC
-- MAGIC Bronze tables are **schema-on-write, append-only**.
-- MAGIC All value columns are stored as STRING exactly as the source sent them.
-- MAGIC The only derived column added at this layer is `ingestion_date`.
-- MAGIC
-- MAGIC Actual source schemas confirmed from CSV inspection:
-- MAGIC   - customers  : customer_id, name, email, gender, signup_date, country
-- MAGIC   - orders     : order_id, customer_id, order_date, total_amount, payment_method, shipping_country
-- MAGIC   - products   : product_id, product_name, category, price, stock_quantity, brand
-- MAGIC   - reviews    : review_id, product_id, customer_id, rating, review_text, review_date

-- COMMAND ----------

-- ── Step 5a: Bronze — customers ────────────────────────────────────────────────
-- Source columns: customer_id, name, email, gender, signup_date, country

CREATE TABLE IF NOT EXISTS catalog_ecommerce.bronze.customers (
  customer_id    STRING  NOT NULL,  -- primary key (integer in source, stored as string)
  name           STRING,            -- full name as a single field
  email          STRING,
  gender         STRING,            -- values: Male / Female / Other
  signup_date    STRING,            -- kept as string; cast to Date in silver
  country        STRING,
  ingestion_date TIMESTAMP          -- added during ingest (current_timestamp)
)
USING DELTA
COMMENT 'Bronze — raw customers, all strings, append-only';

-- COMMAND ----------

-- ── Step 5b: Bronze — orders ───────────────────────────────────────────────────
-- Source columns: order_id, customer_id, order_date, total_amount, payment_method, shipping_country
-- Note: no status column exists in this dataset

CREATE TABLE IF NOT EXISTS catalog_ecommerce.bronze.orders (
  order_id         STRING  NOT NULL,  -- primary key
  customer_id      STRING  NOT NULL,  -- foreign key → customers
  order_date       STRING,            -- cast to Date in silver (format: yyyy-MM-dd)
  total_amount     STRING,            -- cast to Double in silver (clean decimal, no symbols)
  payment_method   STRING,            -- enum: Bank Transfer / PayPal / Cash / Credit Card
  shipping_country STRING,
  ingestion_date   TIMESTAMP
)
USING DELTA
COMMENT 'Bronze — raw orders, all strings, append-only';

-- COMMAND ----------

-- ── Step 5c: Bronze — products ─────────────────────────────────────────────────
-- Source columns: product_id, product_name, category, price, stock_quantity, brand
-- Note: no seller_id, category_id, description or created_at in this dataset

CREATE TABLE IF NOT EXISTS catalog_ecommerce.bronze.products (
  product_id     STRING  NOT NULL,  -- primary key
  product_name   STRING,
  category       STRING,            -- plain string: Books / Clothing / Electronics / etc.
  price          STRING,            -- cast to Double in silver (clean decimal)
  stock_quantity STRING,            -- cast to Int in silver
  brand          STRING,            -- values: BrandA / BrandB / BrandC / BrandD
  ingestion_date TIMESTAMP
)
USING DELTA
COMMENT 'Bronze — raw products, all strings, append-only';

-- COMMAND ----------

-- ── Step 5d: Bronze — product_reviews ─────────────────────────────────────────
-- Source columns: review_id, product_id, customer_id, rating, review_text, review_date

CREATE TABLE IF NOT EXISTS catalog_ecommerce.bronze.product_reviews (
  review_id      STRING  NOT NULL,  -- primary key
  product_id     STRING  NOT NULL,  -- foreign key → products
  customer_id    STRING  NOT NULL,  -- foreign key → customers
  rating         STRING,            -- cast to Int in silver; values: 1 / 2 / 3 / 4 / 5
  review_text    STRING,
  review_date    STRING,            -- cast to Date in silver (format: yyyy-MM-dd)
  ingestion_date TIMESTAMP
)
USING DELTA
COMMENT 'Bronze — raw product reviews, all strings, append-only';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Tables
-- MAGIC
-- MAGIC Silver tables are **typed, deduplicated and DQ-validated**.
-- MAGIC Dates are cast to proper types, values are validated, and derived
-- MAGIC business columns are added at this layer.

-- COMMAND ----------

-- ── Step 6a: Silver — dim_customers ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS catalog_ecommerce.silver.dim_customers (
  customer_id    STRING    NOT NULL,
  name           STRING,            -- trimmed and title-cased
  email          STRING,            -- lowercased and trimmed
  gender         STRING,            -- validated: Male / Female / Other
  signup_date    DATE,              -- cast from string (yyyy-MM-dd)
  country        STRING,            -- title-cased
  is_valid_email BOOLEAN,           -- regex validation flag
  ingestion_date TIMESTAMP,         -- carried from bronze for lineage
  silver_ts      TIMESTAMP          -- when this row was processed into silver
)
USING DELTA
COMMENT 'Silver — cleaned customers dimension';

-- COMMAND ----------

-- ── Step 6b: Silver — dim_products ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS catalog_ecommerce.silver.dim_products (
  product_id     STRING    NOT NULL,
  product_name   STRING,            -- trimmed and title-cased
  category       STRING,            -- title-cased
  price          DOUBLE,            -- cast and validated (>= 0)
  stock_quantity INT,               -- cast and validated (>= 0)
  brand          STRING,            -- title-cased
  price_category STRING,            -- derived: Low / Medium / High
  in_stock       BOOLEAN,           -- derived: stock_quantity > 0
  ingestion_date TIMESTAMP,
  silver_ts      TIMESTAMP
)
USING DELTA
COMMENT 'Silver — cleaned products dimension';

-- COMMAND ----------

-- ── Step 6c: Silver — fact_orders ─────────────────────────────────────────────
-- Note: no status column in the source — removed from this table

CREATE TABLE IF NOT EXISTS catalog_ecommerce.silver.fact_orders (
  order_id            STRING    NOT NULL,
  customer_id         STRING    NOT NULL,
  order_date          DATE,              -- cast from string (yyyy-MM-dd)
  order_date_key      INT,               -- derived: YYYYMMDD integer for date joins
  total_amount        DOUBLE,            -- cast and validated (>= 0)
  payment_method      STRING,            -- lowercased and trimmed
  is_valid_payment    BOOLEAN,           -- validated against known enum
  shipping_country    STRING,            -- title-cased
  ingestion_date      TIMESTAMP,
  silver_ts           TIMESTAMP
)
USING DELTA
COMMENT 'Silver — cleaned orders fact table';

-- COMMAND ----------

-- ── Step 6d: Silver — fact_reviews ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS catalog_ecommerce.silver.fact_reviews (
  review_id        STRING    NOT NULL,
  product_id       STRING    NOT NULL,
  customer_id      STRING    NOT NULL,
  rating           INT,               -- cast and validated (1–5)
  review_text      STRING,
  review_length    INT,               -- derived: character count of review_text
  review_date      DATE,              -- cast from string (yyyy-MM-dd)
  sentiment_bucket STRING,            -- derived: positive / neutral / negative
  ingestion_date   TIMESTAMP,
  silver_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Silver — cleaned product reviews fact table';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Golden Tables
-- MAGIC
-- MAGIC Golden tables are **fully aggregated and overwritten on each run**.
-- MAGIC Optimised for direct consumption by Power BI or Azure Synapse.

-- COMMAND ----------

-- ── Step 7a: Golden — agg_sales_summary ───────────────────────────────────────
-- Grain: one row per calendar date × shipping country

CREATE TABLE IF NOT EXISTS catalog_ecommerce.golden.agg_sales_summary (
  order_date_dt    DATE,
  shipping_country STRING,
  total_orders     LONG,
  unique_customers LONG,
  gross_revenue    DOUBLE,
  avg_order_value  DOUBLE,
  min_order_value  DOUBLE,
  max_order_value  DOUBLE
)
USING DELTA
COMMENT 'Golden — daily sales KPIs by country';

-- COMMAND ----------

-- ── Step 7b: Golden — agg_product_performance ─────────────────────────────────
-- Grain: one row per product

CREATE TABLE IF NOT EXISTS catalog_ecommerce.golden.agg_product_performance (
  product_id        STRING,
  product_name      STRING,
  category          STRING,
  brand             STRING,
  price             DOUBLE,
  price_category    STRING,
  in_stock          BOOLEAN,
  review_count      LONG,
  avg_rating        DOUBLE,
  positive_reviews  LONG,
  neutral_reviews   LONG,
  negative_reviews  LONG,
  rating_rank       INT,
  needs_review_flag BOOLEAN
)
USING DELTA
COMMENT 'Golden — product quality and sentiment performance';

-- COMMAND ----------

-- ── Step 7c: Golden — agg_customer_360 ────────────────────────────────────────
-- Grain: one row per customer (full RFM profile)

CREATE TABLE IF NOT EXISTS catalog_ecommerce.golden.agg_customer_360 (
  customer_id           STRING,
  name                  STRING,
  email                 STRING,
  gender                STRING,
  country               STRING,
  signup_date           DATE,
  tenure_days           INT,
  total_orders          LONG,
  lifetime_value        DOUBLE,
  avg_order_value       DOUBLE,
  last_order_date       DATE,
  first_order_date      DATE,
  days_since_last_order INT,
  reviews_written       LONG,
  avg_review_given      DOUBLE,
  preferred_payment     STRING,   -- most frequently used payment method
  recency_score         INT,
  frequency_score       INT,
  monetary_score        INT,
  rfm_score             INT,
  customer_segment      STRING
)
USING DELTA
COMMENT 'Golden — full customer 360 with RFM segmentation';

-- COMMAND ----------

-- ── Step 7d: Golden — agg_review_sentiment ────────────────────────────────────
-- Grain: one row per calendar month × product

CREATE TABLE IF NOT EXISTS catalog_ecommerce.golden.agg_review_sentiment (
  review_month          STRING,   -- yyyy-MM format
  product_id            STRING,
  product_name          STRING,
  category              STRING,
  brand                 STRING,
  total_reviews         LONG,
  avg_rating            DOUBLE,
  positive_count        LONG,
  neutral_count         LONG,
  negative_count        LONG,
  positive_pct          DOUBLE,
  negative_pct          DOUBLE,
  prev_month_avg_rating DOUBLE,
  rating_mom_change     DOUBLE    -- month-over-month rating change
)
USING DELTA
COMMENT 'Golden — monthly review sentiment trends per product';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Setup

-- COMMAND ----------

-- ── Step 8: Confirm all schemas, tables and the Volume were created ────────────
SHOW TABLES IN catalog_ecommerce.bronze;

-- COMMAND ----------

SHOW TABLES IN catalog_ecommerce.silver;

-- COMMAND ----------

SHOW TABLES IN catalog_ecommerce.golden;

-- COMMAND ----------

SHOW VOLUMES IN catalog_ecommerce.landing;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reset Block *(run only to wipe the entire environment)*
-- MAGIC ⚠️ Destructive — only run in dev/test when you need a clean slate.
-- MAGIC Dropping the catalog CASCADE removes all schemas, tables, and volumes.
-- MAGIC You can also just remove parts as needed

-- COMMAND ----------

-- DROP CATALOG IF EXISTS catalog_ecommerce CASCADE;
-- DROP SCHEMA IF EXISTS catalog_ecommerce.bronze CASCADE;
-- DROP SCHEMA IF EXISTS catalog_ecommerce.silver CASCADE;
-- DROP SCHEMA IF EXISTS catalog_ecommerce.golden CASCADE;
-- DROP VOLUME IF EXISTS catalog_ecommerce.landing CASCADE;
