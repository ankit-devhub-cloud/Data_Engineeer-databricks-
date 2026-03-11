-- Databricks notebook source
-- MAGIC %md
-- MAGIC # End-to-End Analytical Pipeline using Views & Materialized Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Every data engineer’s goal is the same — to turn raw data into insights that business users can trust.
-- MAGIC Over the past 20 days, we’ve explored the essential SQL concepts that make this possible — from basic CRUD to aggregations, joins, window functions, and data cleaning.
-- MAGIC
-- MAGIC Now it’s time to put it all together.
-- MAGIC  We’ll build an end-to-end analytical pipeline — the kind you’d actually use in a production environment — using Spark SQL.
-- MAGIC
-- MAGIC ### The Business Problem
-- MAGIC Imagine you’re part of a company’s HR analytics team.
-- MAGIC  You’re asked to create a data model that helps answer questions like:
-- MAGIC
-- MAGIC - How many employees are in each department?
-- MAGIC - What’s the total and average salary per department?
-- MAGIC - Who are the top 3 highest-paid employees in every department?
-- MAGIC - Which departments are overspending compared to company average?
-- MAGIC
-- MAGIC The data already exists — but it’s scattered, raw, and not optimized for fast analytics.
-- MAGIC  Your task is to design the pipeline that transforms it into clean, ready-to-query analytical views.
-- MAGIC
-- MAGIC ### Start with Raw Tables
-- MAGIC Let’s assume you have two base tables in your Spark catalog:

-- COMMAND ----------

CREATE OR REPLACE TABLE employees (
  emp_id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  dept_id INT,
  salary DOUBLE,
  hire_date DATE
);

-- COMMAND ----------

CREATE OR REPLACE TABLE departments (
  dept_id INT,
  dept_name STRING,
  location STRING
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You’ve ingested this data from HR systems. It’s clean enough structurally, but not yet aggregated or enriched for reporting.
-- MAGIC
-- MAGIC ### Create the Silver Layer (Cleaned View)
-- MAGIC The silver layer standardizes and enriches the raw data.
-- MAGIC  Here, you can join employees with departments and derive helpful columns like full_name.

-- COMMAND ----------

CREATE OR REPLACE VIEW silver_employee_enriched AS
SELECT
  e.emp_id,
  CONCAT(e.first_name, ' ', e.last_name) AS full_name,
  e.email,
  e.salary,
  e.hire_date,
  d.dept_id,
  d.dept_name,
  d.location
FROM employees e
JOIN departments d
  ON e.dept_id = d.dept_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This view becomes your foundation for analytics — one clean, unified representation of employees with department details.
-- MAGIC
-- MAGIC Whenever you need to query or transform employee data, you’ll use this view instead of hitting raw tables directly.
-- MAGIC
-- MAGIC ### Create the Gold Layer (Analytical Aggregations)
-- MAGIC Now you’ll create summary metrics — the KPIs that drive reporting dashboards.
-- MAGIC
-- MAGIC ### Department-Level Salary Summary

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_department_salary_summary AS
SELECT
  dept_name,
  COUNT(emp_id) AS employee_count,
  ROUND(AVG(salary), 2) AS avg_salary,
  SUM(salary) AS total_salary
FROM silver_employee_enriched
GROUP BY dept_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This view represents a business-ready dataset that can power HR dashboards and department performance metrics.
-- MAGIC
-- MAGIC ### Create Analytical Window View (Top Performers)
-- MAGIC To add more insight, you can use window functions to find the top 3 highest-paid employees per department.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_top_earners AS
SELECT *
FROM (
  SELECT
    full_name,
    dept_name,
    salary,
    RANK() OVER (PARTITION BY dept_name ORDER BY salary DESC) AS salary_rank
  FROM silver_employee_enriched
)
WHERE salary_rank <= 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This gives each department’s top three earners — a view managers love to see.
-- MAGIC
-- MAGIC You can already see the pipeline forming:
-- MAGIC
-- MAGIC ### Raw → Silver → Gold
-- MAGIC
-- MAGIC Each layer more refined and more business-ready.
-- MAGIC
-- MAGIC ### Materialize the Gold Layer for Performance
-- MAGIC Views are powerful, but every time you query them, Spark executes the underlying SQL again.
-- MAGIC  If your data grows large and these aggregations run frequently (say, for dashboards), you can speed things up using materialized views.

-- COMMAND ----------

CREATE MATERIALIZED VIEW gold_department_salary_summary_mv
AS
SELECT
  dept_name,
  COUNT(emp_id) AS employee_count,
  ROUND(AVG(salary), 2) AS avg_salary,
  SUM(salary) AS total_salary
FROM silver_employee_enriched
GROUP BY dept_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This stores precomputed aggregates — perfect for dashboards that refresh every few minutes or hours.
-- MAGIC  When new employee data arrives, you can refresh the view:

-- COMMAND ----------

ALTER MATERIALIZED VIEW gold_department_salary_summary_mv REFRESH;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, querying it is instant — no reprocessing required.
-- MAGIC
-- MAGIC ### Bringing It All Together
-- MAGIC Your final architecture looks like this:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is a classic medallion architecture applied with pure SQL:
-- MAGIC
-- MAGIC - Bronze (Raw): Ingested source tables.
-- MAGIC - Silver (Cleaned): Standardized and enriched data.
-- MAGIC - Gold (Aggregated): Analytical summaries and KPIs.
-- MAGIC
-- MAGIC Every layer is modular, traceable, and optimized for its purpose.
-- MAGIC
-- MAGIC ### Query Like a Pro
-- MAGIC Now, your business users or analysts can simply run:

-- COMMAND ----------

SELECT *
FROM gold_department_salary_summary_mv
ORDER BY total_salary DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC They get instant, consistent answers — no need to know the underlying logic, joins, or data structure.
-- MAGIC
-- MAGIC You’ve effectively built a self-serve analytics layer powered entirely by SQL and Spark.
-- MAGIC
-- MAGIC ### Why This Pipeline Matters
-- MAGIC This is what real data engineering looks like:
-- MAGIC
-- MAGIC - Raw data ingestion
-- MAGIC - Transformation using SQL views
-- MAGIC - Business aggregations and metrics
-- MAGIC - Optimization using materialized views
-- MAGIC
-- MAGIC It’s not just about writing SQL — it’s about designing reliable systems that scale, are easy to maintain, and provide consistent answers.
-- MAGIC
-- MAGIC By separating each layer, you gain:
-- MAGIC
-- MAGIC - Reusability: The same logic serves multiple teams.
-- MAGIC - Transparency: Views document your transformations.
-- MAGIC - Performance: Materialized views precompute heavy queries.
-- MAGIC
-- MAGIC Trust: Everyone sees the same metrics — a single source of truth.
-- MAGIC
-- MAGIC # Closing Thought
-- MAGIC This 21-day journey has taken you from the fundamentals of SQL to building a complete, production-ready data pipeline.
-- MAGIC  You’ve learned how to clean data, aggregate it, enrich it, and finally, make it usable for analytics.
-- MAGIC
-- MAGIC What you built here — 
-- MAGIC
-- MAGIC - The raw-to-gold pipeline
-- MAGIC - The layered views,
-- MAGIC - The materialized summaries — is the foundation of modern data engineering on Spark, Databricks, and every cloud platform.
-- MAGIC
-- MAGIC SQL isn’t just a query language. It’s a data lifecycle language — one that turns data into knowledge.
-- MAGIC
-- MAGIC As you continue your journey, remember: Good data engineers don’t just move data. They design systems that make data meaningful.
