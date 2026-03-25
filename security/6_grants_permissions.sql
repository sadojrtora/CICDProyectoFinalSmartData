-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 6 · Grants & Permissions — E-Commerce Medallion
-- MAGIC
-- MAGIC Manages access control for `catalog_ecommerce` using Unity Catalog groups.
-- MAGIC
-- MAGIC ## Groups created
-- MAGIC
-- MAGIC | Group | Access level | Schemas |
-- MAGIC |---|---|---|
-- MAGIC | `grp_data_engineers` | Full access — read, write, create, modify | bronze, silver, golden, landing |
-- MAGIC | `grp_data_analysts`  | Read-only — SELECT only | golden only |
-- MAGIC
-- MAGIC ## Structure
-- MAGIC 1. Create groups
-- MAGIC 2. Add users to groups
-- MAGIC 3. Grant catalog-level access
-- MAGIC 4. Grant schema-level access per group
-- MAGIC 5. Grant table-level access per group
-- MAGIC 6. Verify grants with SHOW GRANTS
-- MAGIC 7. Revoke section (commented — run as needed)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Groups

-- COMMAND ----------

-- ── Step 1: Create the two groups ─────────────────────────────────────────────
-- Groups are workspace-level objects in Unity Catalog.
-- If a group already exists this statement raises an error — comment it out
-- on subsequent runs or use the Databricks UI to check existing groups.

CREATE GROUP `grp_data_engineers`;

-- COMMAND ----------

CREATE GROUP `grp_data_analysts`;

-- COMMAND ----------

-- ── Step 2: Add users to each group ───────────────────────────────────────────
-- Replace the email addresses below with the actual workspace users.
-- Run one ALTER GROUP per user, or list multiple ADD USER clauses.

ALTER GROUP `grp_data_engineers`
ADD USER `dodimadriz@gmail.com`;

-- COMMAND ----------

ALTER GROUP `grp_data_engineers`
ADD USER `data_engineer_2@yourdomain.com`;

-- COMMAND ----------

ALTER GROUP `grp_data_analysts`
ADD USER `ahuaccachi28@gmail.com`;

-- COMMAND ----------

ALTER GROUP `grp_data_analysts`
ADD USER `data_analyst_2@yourdomain.com`;

-- COMMAND ----------

-- Verify users in each group
SHOW USERS IN GROUP `grp_data_engineers`;

-- COMMAND ----------

Show USERS

-- COMMAND ----------

SHOW USERS IN GROUP `grp_data_analysts`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grants — grp_data_engineers
-- MAGIC
-- MAGIC Data Engineers need full access to all layers so they can:
-- MAGIC - Read raw CSVs from the landing Volume
-- MAGIC - Write to bronze, silver and golden tables
-- MAGIC - Create and modify tables during development
-- MAGIC - Run ingestion, transformation and load notebooks

-- COMMAND ----------

-- ── Step 3a: Catalog-level access for data engineers ─────────────────────────
-- USE CATALOG is required before any schema or table access can be granted.

GRANT USE CATALOG
ON CATALOG catalog_ecommerce
TO `grp_data_engineers`;

-- COMMAND ----------

-- ── Step 3b: Schema-level access for data engineers — bronze ──────────────────

GRANT USE SCHEMA
ON SCHEMA catalog_ecommerce.bronze
TO `grp_data_engineers`;

GRANT CREATE TABLE
ON SCHEMA catalog_ecommerce.bronze
TO `grp_data_engineers`;

-- COMMAND ----------

-- ── Step 3c: Schema-level access for data engineers — silver ──────────────────

GRANT USE SCHEMA
ON SCHEMA catalog_ecommerce.silver
TO `grp_data_engineers`;

GRANT CREATE TABLE
ON SCHEMA catalog_ecommerce.silver
TO `grp_data_engineers`;

-- COMMAND ----------

-- ── Step 3d: Schema-level access for data engineers — golden ──────────────────

GRANT USE SCHEMA
ON SCHEMA catalog_ecommerce.golden
TO `grp_data_engineers`;

GRANT CREATE TABLE
ON SCHEMA catalog_ecommerce.golden
TO `grp_data_engineers`;

-- COMMAND ----------

-- ── Step 3e: Schema-level access for data engineers — landing (Volume) ─────────

GRANT USE SCHEMA
ON SCHEMA catalog_ecommerce.landing
TO `grp_data_engineers`;

GRANT READ VOLUME
ON VOLUME catalog_ecommerce.landing.raw
TO `grp_data_engineers`;

GRANT WRITE VOLUME
ON VOLUME catalog_ecommerce.landing.raw
TO `grp_data_engineers`;

-- COMMAND ----------

-- ── Step 3f: Table-level access for data engineers — bronze ───────────────────
-- ALL PRIVILEGES = SELECT, INSERT, UPDATE, DELETE, MODIFY, REFRESH

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.bronze.customers
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.bronze.orders
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.bronze.products
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.bronze.product_reviews
TO `grp_data_engineers`;

-- COMMAND ----------

-- ── Step 3g: Table-level access for data engineers — silver ───────────────────

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.silver.dim_customers
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.silver.dim_products
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.silver.fact_orders
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.silver.fact_reviews
TO `grp_data_engineers`;

-- COMMAND ----------

-- ── Step 3h: Table-level access for data engineers — golden ───────────────────

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.golden.agg_sales_summary
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.golden.agg_product_performance
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.golden.agg_customer_360
TO `grp_data_engineers`;

GRANT ALL PRIVILEGES
ON TABLE catalog_ecommerce.golden.agg_review_sentiment
TO `grp_data_engineers`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grants — grp_data_analysts
-- MAGIC
-- MAGIC Data Analysts only need read access to the **golden layer** — the BI-ready
-- MAGIC aggregated tables. They should not be able to see raw or intermediate data
-- MAGIC in bronze or silver, and must not be able to modify any table.

-- COMMAND ----------

-- ── Step 4a: Catalog-level access for data analysts ───────────────────────────
-- USE CATALOG is required before any schema or table grant can be applied.
-- We grant this but do NOT grant USE SCHEMA on bronze or silver.

GRANT USE CATALOG
ON CATALOG catalog_ecommerce
TO `grp_data_analysts`;

-- COMMAND ----------

-- ── Step 4b: Schema-level access for data analysts — golden only ──────────────
-- Only golden is exposed. bronze, silver and landing are not accessible.

GRANT USE SCHEMA
ON SCHEMA catalog_ecommerce.golden
TO `grp_data_analysts`;

-- COMMAND ----------

-- ── Step 4c: Table-level SELECT access for data analysts — golden only ─────────
-- SELECT is the only privilege granted — no INSERT, UPDATE, DELETE or MODIFY.

GRANT SELECT
ON TABLE catalog_ecommerce.golden.agg_sales_summary
TO `grp_data_analysts`;

-- COMMAND ----------

GRANT SELECT
ON TABLE catalog_ecommerce.golden.agg_product_performance
TO `grp_data_analysts`;

-- COMMAND ----------

GRANT SELECT
ON TABLE catalog_ecommerce.golden.agg_customer_360
TO `grp_data_analysts`;

-- COMMAND ----------

GRANT SELECT
ON TABLE catalog_ecommerce.golden.agg_review_sentiment
TO `grp_data_analysts`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Grants

-- COMMAND ----------

-- ── Step 5a: Show grants at the catalog level ─────────────────────────────────
SHOW GRANTS ON CATALOG catalog_ecommerce;

-- COMMAND ----------

-- ── Step 5b: Show grants on each schema ───────────────────────────────────────
SHOW GRANTS ON SCHEMA catalog_ecommerce.bronze;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA catalog_ecommerce.silver;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA catalog_ecommerce.golden;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA catalog_ecommerce.landing;

-- COMMAND ----------

-- ── Step 5c: Show grants on bronze tables ─────────────────────────────────────
SHOW GRANTS ON TABLE catalog_ecommerce.bronze.customers;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.bronze.orders;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.bronze.products;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.bronze.product_reviews;

-- COMMAND ----------

-- ── Step 5d: Show grants on silver tables ─────────────────────────────────────
SHOW GRANTS ON TABLE catalog_ecommerce.silver.dim_customers;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.silver.dim_products;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.silver.fact_orders;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.silver.fact_reviews;

-- COMMAND ----------

-- ── Step 5e: Show grants on golden tables — both groups should appear here ─────
SHOW GRANTS ON TABLE catalog_ecommerce.golden.agg_sales_summary;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.golden.agg_product_performance;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.golden.agg_customer_360;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_ecommerce.golden.agg_review_sentiment;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Revoke Section
-- MAGIC
-- MAGIC Run these cells individually as needed to remove access.
-- MAGIC All cells are commented out by default to prevent accidental execution.

-- COMMAND ----------

-- ── Revoke all privileges from grp_data_engineers ────────────────────────────
-- Uncomment and run one block at a time

-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.bronze.customers       FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.bronze.orders           FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.bronze.products         FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.bronze.product_reviews  FROM `grp_data_engineers`;

-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.silver.dim_customers    FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.silver.dim_products     FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.silver.fact_orders      FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.silver.fact_reviews     FROM `grp_data_engineers`;

-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.golden.agg_sales_summary         FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.golden.agg_product_performance   FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.golden.agg_customer_360          FROM `grp_data_engineers`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_ecommerce.golden.agg_review_sentiment      FROM `grp_data_engineers`;

-- REVOKE READ VOLUME  ON VOLUME catalog_ecommerce.landing.raw FROM `grp_data_engineers`;
-- REVOKE WRITE VOLUME ON VOLUME catalog_ecommerce.landing.raw FROM `grp_data_engineers`;

-- REVOKE CREATE TABLE ON SCHEMA catalog_ecommerce.bronze  FROM `grp_data_engineers`;
-- REVOKE CREATE TABLE ON SCHEMA catalog_ecommerce.silver  FROM `grp_data_engineers`;
-- REVOKE CREATE TABLE ON SCHEMA catalog_ecommerce.golden  FROM `grp_data_engineers`;
-- REVOKE USE SCHEMA   ON SCHEMA catalog_ecommerce.bronze  FROM `grp_data_engineers`;
-- REVOKE USE SCHEMA   ON SCHEMA catalog_ecommerce.silver  FROM `grp_data_engineers`;
-- REVOKE USE SCHEMA   ON SCHEMA catalog_ecommerce.golden  FROM `grp_data_engineers`;
-- REVOKE USE SCHEMA   ON SCHEMA catalog_ecommerce.landing FROM `grp_data_engineers`;
-- REVOKE USE CATALOG  ON CATALOG catalog_ecommerce        FROM `grp_data_engineers`;

-- COMMAND ----------

-- ── Revoke all privileges from grp_data_analysts ──────────────────────────────

-- REVOKE SELECT ON TABLE catalog_ecommerce.golden.agg_sales_summary        FROM `grp_data_analysts`;
-- REVOKE SELECT ON TABLE catalog_ecommerce.golden.agg_product_performance  FROM `grp_data_analysts`;
-- REVOKE SELECT ON TABLE catalog_ecommerce.golden.agg_customer_360         FROM `grp_data_analysts`;
-- REVOKE SELECT ON TABLE catalog_ecommerce.golden.agg_review_sentiment     FROM `grp_data_analysts`;

-- REVOKE USE SCHEMA  ON SCHEMA  catalog_ecommerce.golden FROM `grp_data_analysts`;
-- REVOKE USE CATALOG ON CATALOG catalog_ecommerce        FROM `grp_data_analysts`;

-- COMMAND ----------

-- ── Verify revocations took effect ────────────────────────────────────────────
-- SHOW GRANTS ON TABLE catalog_ecommerce.golden.agg_sales_summary;
-- SHOW GRANTS ON CATALOG catalog_ecommerce;
