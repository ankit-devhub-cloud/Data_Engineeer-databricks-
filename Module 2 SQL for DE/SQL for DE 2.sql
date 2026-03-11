-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Essential SQL Concepts for Freshers

-- COMMAND ----------

USE CATALOG thebricklearning;
USE SCHEMA aianalyticsengineer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aliasing (AS)
-- MAGIC
-- MAGIC - Makes column names readable in result sets.
-- MAGIC - Essential for clarity, especially in joins and aggregations.

-- COMMAND ----------

SELECT c.customer_name AS name, o.total_amount AS amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ORDER BY + LIMIT / TOP
-- MAGIC
-- MAGIC - Fetches top-N records (e.g., top 5 customers by spend).
-- MAGIC - Used in dashboards, pagination, and ranking.

-- COMMAND ----------

SELECT * FROM orders
ORDER BY total_amount DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DISTINCT
-- MAGIC Used for deduplication, dimension extraction, or quick exploration.

-- COMMAND ----------

SELECT DISTINCT customer_city FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IN, NOT IN, BETWEEN
-- MAGIC
-- MAGIC - Enables concise filtering logic.
-- MAGIC - Common in BI dashboards and reporting tools.

-- COMMAND ----------

SELECT * FROM orders 
WHERE customer_id IN (1, 2);

-- COMMAND ----------

SELECT * FROM orders 
WHERE total_amount BETWEEN 100 AND 250;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### String Operations on Columns

-- COMMAND ----------

SELECT 
  UPPER(customer_name) AS upper_name,
  LENGTH(customer_name) AS name_length
FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DATE Functions

-- COMMAND ----------

SELECT 
  order_date,
  YEAR(order_date) AS order_year,
  MONTH(order_date) AS order_month
FROM orders;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### NULL Handling – COALESCE, IS NULL

-- COMMAND ----------

SELECT 
  customer_name,
  COALESCE(customer_email, 'N/A') AS email_info
FROM customers;

-- COMMAND ----------

SELECT * FROM orders
WHERE total_amount IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Subqueries

-- COMMAND ----------

SELECT * FROM customers
WHERE customer_id IN (
  SELECT customer_id FROM orders WHERE total_amount > 200
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UNION / UNION ALL

-- COMMAND ----------

SELECT customer_name FROM customers
UNION
SELECT product_name FROM order_items;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  Basic Window Functions

-- COMMAND ----------

SELECT 
  customer_id,
  total_amount,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_rank
FROM orders;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Duplicate removal from a table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In data engineering, removing duplicate rows is a critical task during data cleansing, ETL processing, and data quality enforcement — especially before moving from Bronze ➝ Silver ➝ Gold layers in a medallion architecture (e.g., on Databricks).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Understanding Duplicate Removal from a Table
-- MAGIC We’ll walk through:
-- MAGIC
-- MAGIC - What is a duplicate?
-- MAGIC - How to detect duplicates
-- MAGIC - How to remove duplicates (with and without row IDs)
-- MAGIC - Best practice for deduplication

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. What is a Duplicate?
-- MAGIC
-- MAGIC A duplicate is a row that is identical in one or more selected columns. It can be:
-- MAGIC - Entire row is repeated
-- MAGIC - Or based on a business key (e.g., same customer_id, order_id, etc.)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Detecting Duplicates
-- MAGIC - Example: Identify rows in orders with duplicate order_ids

-- COMMAND ----------

SELECT 
  order_id,
  COUNT(*) AS cnt
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A. Remove Exact Duplicates (All Columns Match)
-- MAGIC
-- MAGIC - Removes rows that are completely identical.
-- MAGIC - Fast and useful for small tables or logs with exact repeats.

-- COMMAND ----------

CREATE TABLE orders_deduped AS
SELECT DISTINCT * FROM orders;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B. Deduplicate Based on Business Key (e.g., order_id) – Keep Latest
-- MAGIC
-- MAGIC - Uses window function ROW_NUMBER() to rank rows per order_id.
-- MAGIC - Keeps only the latest record.
-- MAGIC - Very useful in CDC (Change Data Capture) scenarios.

-- COMMAND ----------

CREATE OR REPLACE TABLE orders_deduped_2 AS
SELECT * FROM (
  SELECT *, 
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
  FROM orders
) t
WHERE rn = 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C. Delete Duplicates In-Place (if table supports DELETE)
-- MAGIC
-- MAGIC - Keeps only the first occurrence of each duplicate order_id.

-- COMMAND ----------

DELETE FROM orders
WHERE rowid NOT IN (
  SELECT MIN(rowid)
  FROM orders
  GROUP BY order_id
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Practice	Why It Matters
-- MAGIC - Define the deduplication logic clearly (by which columns?)	Avoid accidental data loss
-- MAGIC - Use window functions for fine control	Choose which duplicate to retain (latest, earliest)
-- MAGIC - Prefer non-destructive approach	Write deduped data to new table (e.g., Silver layer)
-- MAGIC - Always log or count before deleting	Helps with observability and auditing

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sample_values AS
SELECT * FROM VALUES
  (1, 50),
  (2, 40),
  (3, 40),
  (4, 30),
  (5, 20),
  (6, 20)
AS sample(id, value);

-- COMMAND ----------

SELECT 
    id,
    value,
    RANK() OVER (partition by id ORDER BY value DESC) AS rnk
FROM sample_values;

-- COMMAND ----------

SELECT 
    id,
    value,
    RANK() OVER (ORDER BY value DESC)       AS rnk,
    DENSE_RANK() OVER (ORDER BY value DESC) AS dense_rnk,
    RANK() OVER (PARTITION BY value ORDER BY id) AS rptnk
FROM sample_values;


-- COMMAND ----------

SELECT 
      id,
      value,
      RANK() OVER (PARTITION BY value ORDER BY id) AS rnk
  FROM sample_values

-- COMMAND ----------

WITH ranked AS (
  SELECT 
      id,
      value,
      RANK() OVER (PARTITION BY value ORDER BY id) AS rnk
  FROM sample_values
)
SELECT *
FROM ranked
WHERE rnk = 1;
