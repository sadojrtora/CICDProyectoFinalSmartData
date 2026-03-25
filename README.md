# 🛒 E-Commerce Medallion Architecture
### Azure Databricks · Unity Catalog · Delta Lake · PySpark

---

## 📌 Overview

A production-grade **Medallion Architecture** pipeline built on **Azure Databricks** and **Unity Catalog** for a synthetic e-commerce dataset. Raw CSV files are ingested from a Unity Catalog Volume, progressively cleaned and enriched through Bronze → Silver → Golden layers, and surfaced as BI-ready aggregated tables for Power BI or Azure Synapse Analytics.

> All storage is **Unity Catalog managed** — no ADLS Gen2 external locations or storage credentials are required. Due to limitations with the Azure account.

---

## 🏛️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Unity Catalog                                │
│                    catalog_ecommerce                                │
│                                                                     │
│  ┌──────────────┐                                                   │
│  │   landing    │  /Volumes/catalog_ecommerce/landing/raw/          │
│  │   (Volume)   │  customers.csv  orders.csv                        │
│  │              │  products.csv   product_reviews.csv               │
│  └──────┬───────┘                                                   │
│         │  spark.read.csv()                                         │
│         ▼                                                           │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  🥉  BRONZE  — Raw ingestion, all StringType, append-only    │  │
│  │                                                              │  │
│  │   customers        orders          products   product_reviews│  │
│  └───────────────────────────┬──────────────────────────────────┘  │
│                              │  Type casts · DQ · Dedup · UDFs     │
│                              ▼                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  🥈  SILVER  — Cleaned, typed, validated                     │  │
│  │                                                              │  │
│  │   dim_customers    dim_products    fact_orders  fact_reviews  │  │
│  └───────────────────────────┬──────────────────────────────────┘  │
│                              │  Joins · Aggregations · RFM         │
│                              ▼                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  🥇  GOLDEN  — BI-ready aggregated tables                    │  │
│  │                                                              │  │
│  │  agg_sales_summary         agg_product_performance           │  │
│  │  agg_customer_360          agg_review_sentiment              │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────┐
              │  Power BI / Azure Synapse  │
              └───────────────────────────┘
```

---

## 📂 Project Structure

```
ecommerce_medallion/
│
├── 1_preparacion_ambiente.sql    # Environment setup — catalog, schemas, Volume, DDL
├── 2_ingest.py                   # Bronze — CSV → Delta (PySpark)
├── 3_transform.py                # Silver — Cleanse, cast, validate, derive (PySpark)
├── 4_load.py                     # Golden — Join, aggregate, RFM score (PySpark)
├── 5_drop_reset.sql              # Utility — Drop all tables for full reset
├── 6_grants_permissions.sql      # Security — Group creation and access control
└── README.md                     # This file
```

---

## 🗃️ Data Sources

| File | Rows | Description |
|---|---|---|
| `customers.csv` | ~100,000 | Customer profiles — name, email, gender, country |
| `orders.csv` | ~100,000 | Orders — date, amount, payment method, shipping country |
| `products.csv` | ~20,000 | Product catalogue — name, category, price, stock, brand |
| `product_reviews.csv` | ~100,000 | Customer reviews — 1–5 star ratings with free text |

> **Source:** [Synthetic E-Commerce Relational Dataset — Kaggle](https://www.kaggle.com/datasets/naelaqel/synthetic-e-commerce-relational-dataset)

---

## 🥉 Bronze Layer — `catalog_ecommerce.bronze`

Raw ingestion with zero business logic. Every value column is stored as `STRING` to prevent parse failures. One audit column is added per row.

| Table | Primary Key | Source File |
|---|---|---|
| `customers` | `customer_id` | customers.csv |
| `orders` | `order_id` | orders.csv |
| `products` | `product_id` | products.csv |
| `product_reviews` | `review_id` | product_reviews.csv |

**Added at this layer:**
- `ingestion_date TIMESTAMP` — `current_timestamp()` of the load run

---

## 🥈 Silver Layer — `catalog_ecommerce.silver`

Cleaned, typed and validated. Each table is deduplicated by primary key before writing.

### `dim_customers`
| Column | Type | Transformation |
|---|---|---|
| `customer_id` | STRING | PK — dedup window |
| `name` | STRING | `initcap(trim())` |
| `email` | STRING | `lower(trim())` |
| `gender` | STRING | `initcap(trim())` |
| `signup_date` | DATE | `to_date(..., "yyyy-MM-dd")` |
| `country` | STRING | `initcap(trim())` |
| `is_valid_email` | BOOLEAN | Regex flag `^[^@\s]+@[^@\s]+\.[^@\s]+$` |
| `silver_ts` | TIMESTAMP | Processing timestamp |

### `dim_products`
| Column | Type | Transformation |
|---|---|---|
| `product_id` | STRING | PK — dedup window |
| `product_name` | STRING | `initcap(trim())` |
| `category` | STRING | `initcap(trim())` |
| `price` | DOUBLE | Cast + negative guard |
| `stock_quantity` | INT | Cast + negative guard |
| `brand` | STRING | `initcap(trim())` |
| `price_category` | STRING | UDF: Low / Medium / High |
| `in_stock` | BOOLEAN | `stock_quantity > 0` |

### `fact_orders`
| Column | Type | Transformation |
|---|---|---|
| `order_id` | STRING | PK — dedup window |
| `customer_id` | STRING | FK |
| `order_date` | DATE | `to_date(..., "yyyy-MM-dd")` |
| `order_date_key` | INT | `YYYYMMDD` integer key |
| `total_amount` | DOUBLE | Cast + negative guard |
| `payment_method` | STRING | `lower(trim())` |
| `is_valid_payment` | BOOLEAN | Enum check: bank transfer / paypal / cash / credit card |
| `shipping_country` | STRING | `initcap(trim())` |

### `fact_reviews`
| Column | Type | Transformation |
|---|---|---|
| `review_id` | STRING | PK — dedup window |
| `product_id` | STRING | FK |
| `customer_id` | STRING | FK |
| `rating` | INT | Cast + range check (1–5) |
| `review_text` | STRING | Preserved as-is |
| `review_length` | INT | `length(review_text)` |
| `review_date` | DATE | `to_date(..., "yyyy-MM-dd")` |
| `sentiment_bucket` | STRING | UDF: positive (4–5) / neutral (3) / negative (1–2) |

---

## 🥇 Golden Layer — `catalog_ecommerce.golden`

Business-ready aggregated tables. Fully overwritten on each run.

### `agg_sales_summary`
**Grain:** One row per calendar date × shipping country
> Daily revenue KPIs — total orders, unique customers, gross revenue, AOV, min/max order value

### `agg_product_performance`
**Grain:** One row per product
> Product quality scorecard — review count, avg rating, sentiment split, rating rank, needs_review flag

### `agg_customer_360`
**Grain:** One row per customer — full RFM profile

| RFM Score | Segment |
|---|---|
| 13 – 15 | 🏆 Champions |
| 10 – 12 | 💛 Loyal |
| 7 – 9 | 🌱 Potential Loyalist |
| 5 – 6 | ⚠️ At Risk |
| 3 – 4 | 💤 Hibernating |

> Includes: lifetime value, preferred payment method, days since last order, tenure, reviews written

### `agg_review_sentiment`
**Grain:** One row per calendar month × product
> Monthly sentiment trends — avg rating, positive/negative %, month-over-month rating change (LAG)

---

## ⚙️ Notebooks

| # | Notebook | Type | Description |
|---|---|---|---|
| 1 | `1_preparacion_ambiente.sql` | SQL | Creates catalog, schemas, Volume and all table DDLs |
| 2 | `2_ingest.py` | PySpark | Reads CSVs from Volume → writes to bronze |
| 3 | `3_transform.py` | PySpark | Bronze → silver (DQ, casts, dedup, UDFs) |
| 4 | `4_load.py` | PySpark | Silver → golden (joins, aggregations, RFM) |
| 5 | `5_drop_reset.sql` | SQL | Drops all tables, schemas and Volume for full reset |
| 6 | `6_grants_permissions.sql` | SQL | Group creation, grants and revokes |

### Execution Order
```
1_preparacion_ambiente.sql
        ↓
  [Upload CSVs to Volume]
        ↓
2_ingest.py
        ↓
3_transform.py
        ↓
4_load.py
```

---

## ⏰ Databricks Job Schedule

The pipeline is orchestrated as a **Databricks Workflow Job** with the following configuration:

| Setting | Value |
|---|---|
| Job name | `ecommerce_medallion_pipeline` |
| Trigger type | Scheduled (Cron) |
| Schedule | **Every Monday at 00:05 AM** |
| Cron expression | `5 0 * * 1` |
| Timezone | UTC |
| Tasks | 4 sequential tasks (notebooks 1 → 2 → 3 → 4) |
| On failure | Email alert to pipeline owner |

### Job DAG
```
┌─────────────────────────┐
│  1_preparacion_ambiente  │  (SQL notebook)
└────────────┬────────────┘
             ↓
┌────────────────────────┐
│       2_ingest          │  (PySpark notebook)
└────────────┬───────────┘
             ↓
┌────────────────────────┐
│      3_transform        │  (PySpark notebook)
└────────────┬───────────┘
             ↓
┌────────────────────────┐
│        4_load           │  (PySpark notebook)
└────────────────────────┘
```

> **Note:** Notebooks 5 and 6 are **not** part of the scheduled job. They are run manually on demand — notebook 5 for environment resets and notebook 6 for access control changes.

---

## 🔐 Access Control

Two Unity Catalog groups manage access to the pipeline data.

### `grp_data_engineers`
```
Catalog  : USE CATALOG
Schemas  : USE SCHEMA + CREATE TABLE  →  bronze, silver, golden, landing
Tables   : ALL PRIVILEGES             →  all 12 tables
Volume   : READ VOLUME + WRITE VOLUME →  landing.raw
```

### `grp_data_analysts`
```
Catalog  : USE CATALOG
Schemas  : USE SCHEMA                 →  golden only
Tables   : SELECT only                →  agg_sales_summary
                                         agg_product_performance
                                         agg_customer_360
                                         agg_review_sentiment
```

> Bronze, silver and the landing volume are **invisible** to `grp_data_analysts`.

---

## 🚀 Setup Guide

### Prerequisites
- Azure Databricks workspace with Unity Catalog enabled
- Databricks Runtime 14.x LTS or higher
- Workspace admin rights to create catalogs and groups

### Step 1 — Run environment setup
Open and run `1_preparacion_ambiente.sql` in Databricks. This creates the catalog, all schemas, the landing Volume and all table DDLs.

### Step 2 — Upload CSV files
Navigate to **Catalog → catalog_ecommerce → landing → raw** in the Databricks UI and click **Upload to this volume**. Upload the four CSV files:
```
customers.csv
orders.csv
products.csv
product_reviews.csv
```

### Step 3 — Run the pipeline
Execute notebooks 2, 3 and 4 in order, or trigger the Databricks Job manually for the first run.

### Step 4 — Configure access
Open `6_grants_permissions.sql`, replace the placeholder email addresses with your actual workspace users, then run the notebook to create groups and apply grants.

### Step 5 — Schedule (already configured)
The Databricks Job runs automatically every **Monday at 00:05 AM UTC**. No manual action is needed after the first run.

---

## 🔄 Resetting the Environment

To drop all tables and start fresh, run `5_drop_reset.sql`. The notebook drops tables layer by layer with confirmation checks at each step. Use the final `DROP CATALOG CASCADE` cell only when you want to remove everything including the catalog itself.

> Since all tables are **Unity Catalog managed**, `DROP TABLE` automatically removes both the metadata and the physical Delta files. No manual file cleanup is required.

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Cloud platform | Microsoft Azure |
| Compute | Azure Databricks |
| Storage | Unity Catalog Managed Storage |
| File landing | Unity Catalog Volume |
| Table format | Delta Lake |
| Pipeline language | PySpark + SQL |
| Orchestration | Databricks Workflows |
| BI consumption | Power BI / Azure Synapse Analytics |

---

## 📊 Dashboard section 
— Executive Sales Command Center
Source: `agg_sales_summary`
The go-to dashboard for leadership. Centers on revenue performance over time with geographic breakdown.
Key visuals:

📈 Weekly gross revenue trend line with a Monday-to-Monday comparison (aligns perfectly with your job schedule)
🗺️ World map with bubble size = gross revenue per shipping country
KPI cards — Total Orders · Gross Revenue · AOV · Unique Customers (current week vs previous week)
Bar chart — Top 10 countries by gross revenue
Line chart — Average order value trend over time
Stacked bar — Order volume by day of week to spot purchasing patterns

**Story it tells:** "Where is our revenue coming from, is it growing, and which markets are our strongest?"

— Product Performance & Sentiment Tracker
Source: `agg_product_performance` + `agg_review_sentiment`
Combines the product scorecard with the monthly sentiment trends for a complete product health view.
Key visuals:

Scatter plot — Price vs Avg Rating (colored by category, sized by review count) — immediately shows which price tiers get the best reviews
Bar chart — Top 10 and Bottom 10 products by avg rating
Heatmap — Category × Brand average rating grid
Line chart — Month-over-month avg rating trend per category (using rating_mom_change)
Donut chart — Sentiment split (positive / neutral / negative) overall and filterable by category
Table — Products flagged with needs_review_flag = true (in-stock but rating < 2.5)

**Story it tells:** "Which products are delighting customers, which ones are at risk, and is sentiment improving or declining?"

— Customer 360 & RFM Segmentation
Source: `agg_customer_360`
The CRM and marketing team's most valuable dashboard. Built around the RFM scoring model.
Key visuals:

Treemap or donut — Customer count by segment (Champions / Loyal / Potential Loyalist / At Risk / Hibernating)
KPI cards — Total customers · Avg lifetime value · Avg tenure days · % Champions
Bar chart — Average lifetime value per segment (shows the revenue gap between segments)
Bar chart — Top 10 countries by number of Champions
Stacked bar — Preferred payment method breakdown per customer segment
Scatter plot — Recency score vs Monetary score colored by segment
Table — At Risk and Hibernating customers sorted by lifetime value (highest value customers to re-engage first)

**Story it tells:** "Who are our most valuable customers, where are they at risk of churning, and how do we prioritise re-engagement campaigns?"

— Operations & Pipeline Health Monitor
Source: All four golden tables
A more technical dashboard aimed at the data team and operations managers. Monitors data freshness, pipeline output quality, and cross-table consistency.
Key visuals:

KPI cards — Row counts per golden table (refreshed every Monday — confirms the job ran successfully)
Line chart — Weekly order volume vs weekly review volume (are customers reviewing at the same rate as ordering?)
Bar chart — is_valid_payment rate over time (should always be ~100% — any drop signals upstream data issues)
Gauge — % of customers with at least one order (measures catalogue activation rate)
Bar chart — Stock status breakdown by category (in_stock vs out_of_stock count from agg_product_performance)
Table — Products with avg_rating IS NULL (products with zero reviews — marketing opportunity)
KPI card — Last pipeline run date derived from silver_ts (confirms data freshness every Monday)

**Story it tells:** "Did this week's pipeline run successfully, is the data quality healthy, and are there any anomalies that need investigation?"


---
## 📎 Notes

- All IDs (`customer_id`, `order_id`, etc.) are stored as `STRING` in bronze and silver even though they are integers in the source. This prevents type-widening issues on future schema changes.
- `insertInto()` writes by **column position**, not by name. The `.select()` column order in every PySpark notebook exactly matches the DDL declared in notebook 1.
- Golden tables use `mode("overwrite")` — they are fully recomputed on every run. This is intentional: aggregates are cheap to recompute and avoids partial-update complexity.
- The `sentiment_bucket` UDF is a rule-based proxy (rating ≥ 4 = positive, etc.). Replace with an MLflow NLP model for production-quality text sentiment.
- The dashboards presented are only a sample of the described above.
