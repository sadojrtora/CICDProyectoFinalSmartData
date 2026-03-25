-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 5 · Drop & Reset — E-Commerce Medallion
-- MAGIC
-- MAGIC Drops all Delta tables across every schema in `catalog_ecommerce`
-- MAGIC and optionally removes the schemas and catalog itself.
-- MAGIC
-- MAGIC **⚠️  This notebook is destructive. Run only in dev/test environments.**
-- MAGIC
-- MAGIC > **Note on managed tables:** Our tables were created **without** a `LOCATION`
-- MAGIC > clause, so they are Unity Catalog **managed** tables. Unlike external tables,
-- MAGIC > `DROP TABLE` automatically removes both the metadata AND the physical Delta
-- MAGIC > files. No `dbutils.fs.rm()` calls are needed.

-- COMMAND ----------

-- ── Step 1: Remove any widgets left from a previous run ───────────────────────
%python
dbutils.widgets.removeAll()

-- COMMAND ----------

-- ── Step 2: Declare widgets ───────────────────────────────────────────────────
%python
dbutils.widgets.text("catalogo", "catalog_ecommerce")

-- COMMAND ----------

-- ── Step 3: Read widget value ─────────────────────────────────────────────────
%python
catalogo = dbutils.widgets.get("catalogo")
print(f"Target catalog : {catalogo}")
print("⚠️  All tables in this catalog will be dropped.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deletion Bronze Tables

-- COMMAND ----------

-- Drop all four bronze tables
-- Unity Catalog removes both the table metadata and physical Delta files.

DROP TABLE IF EXISTS ${catalogo}.bronze.customers;
DROP TABLE IF EXISTS ${catalogo}.bronze.orders;
DROP TABLE IF EXISTS ${catalogo}.bronze.products;
DROP TABLE IF EXISTS ${catalogo}.bronze.product_reviews;

-- COMMAND ----------

-- Confirm bronze tables are gone
%python
remaining = spark.sql(f"SHOW TABLES IN {catalogo}.bronze").collect()
if remaining:
    print(f"⚠️  Tables still present in bronze: {[r.tableName for r in remaining]}")
else:
    print("✓ bronze — all tables dropped")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deletion Silver Tables

-- COMMAND ----------

-- Drop all four silver tables

DROP TABLE IF EXISTS ${catalogo}.silver.dim_customers;
DROP TABLE IF EXISTS ${catalogo}.silver.dim_products;
DROP TABLE IF EXISTS ${catalogo}.silver.fact_orders;
DROP TABLE IF EXISTS ${catalogo}.silver.fact_reviews;

-- COMMAND ----------

-- Confirm silver tables are gone
%python
remaining = spark.sql(f"SHOW TABLES IN {catalogo}.silver").collect()
if remaining:
    print(f"⚠️  Tables still present in silver: {[r.tableName for r in remaining]}")
else:
    print("✓ silver — all tables dropped")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deletion Golden Tables

-- COMMAND ----------

-- Drop all four golden tables

DROP TABLE IF EXISTS ${catalogo}.golden.agg_sales_summary;
DROP TABLE IF EXISTS ${catalogo}.golden.agg_product_performance;
DROP TABLE IF EXISTS ${catalogo}.golden.agg_customer_360;
DROP TABLE IF EXISTS ${catalogo}.golden.agg_review_sentiment;

-- COMMAND ----------

-- Confirm golden tables are gone
%python
remaining = spark.sql(f"SHOW TABLES IN {catalogo}.golden").collect()
if remaining:
    print(f"⚠️  Tables still present in golden: {[r.tableName for r in remaining]}")
else:
    print("✓ golden — all tables dropped")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Landing Volume Deletion

-- COMMAND ----------

-- Drop the Volume where CSVs were uploaded
-- This removes the volume registration but NOT the files inside it.
-- Files uploaded to the volume must be manually removed via the Databricks UI
-- if you want to free up the underlying storage.

DROP VOLUME IF EXISTS ${catalogo}.landing.raw;

-- COMMAND ----------

-- Confirm volume is gone
%python
remaining = spark.sql(f"SHOW VOLUMES IN {catalogo}.landing").collect()
if remaining:
    print(f"⚠️  Volumes still present in landing: {[r.volume_name for r in remaining]}")
else:
    print("✓ landing volume dropped")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema Deletion

-- COMMAND ----------

-- Drop all schemas (will fail if any tables remain — run the sections above first)

DROP SCHEMA IF EXISTS ${catalogo}.bronze;
DROP SCHEMA IF EXISTS ${catalogo}.silver;
DROP SCHEMA IF EXISTS ${catalogo}.golden;
DROP SCHEMA IF EXISTS ${catalogo}.landing;

-- COMMAND ----------

-- Confirm all schemas are gone
%python
remaining = spark.sql(f"SHOW SCHEMAS IN {catalogo}").collect()
schema_names = [r.databaseName for r in remaining if r.databaseName != "information_schema"]
if schema_names:
    print(f"⚠️  Schemas still present: {schema_names}")
else:
    print("✓ All schemas dropped")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Catalog Deletion
-- MAGIC
-- MAGIC ⚠️ **Only run this cell if you want to remove the entire catalog.**
-- MAGIC Using `CASCADE` drops all remaining schemas and tables in one shot
-- MAGIC if the section-by-section drops above were skipped.

-- COMMAND ----------

-- Uncomment to drop the entire catalog in one shot
-- DROP CATALOG IF EXISTS ${catalogo} CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Final Summary

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("\n" + "=" * 55)
-- MAGIC print("DROP & RESET — Complete")
-- MAGIC print("=" * 55)
-- MAGIC print(f"  Catalog  : {catalogo}")
-- MAGIC print("  Status   : All tables, schemas and volumes removed ✓")
-- MAGIC print()
-- MAGIC print("  To rebuild the environment run notebooks in order:")
-- MAGIC print("    1_preparacion_ambiente.sql")
-- MAGIC print("    2_ingest.py")
-- MAGIC print("    3_transform.py")
-- MAGIC print("    4_load.py")
-- MAGIC print("=" * 55)
