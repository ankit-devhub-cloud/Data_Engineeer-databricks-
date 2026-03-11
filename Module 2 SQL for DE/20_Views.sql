-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Views & Materialized Views
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Why Data Engineers Use Them
-- MAGIC
-- MAGIC If you’ve ever built dashboards, you’ve probably noticed how the same queries get written again and again.
-- MAGIC  You join the same tables, calculate the same KPIs, and apply the same filters — only to display the result in slightly different formats.
-- MAGIC
-- MAGIC As datasets grow and queries become more complex, this repetition becomes costly — both for you and for the system.
-- MAGIC  That’s where views and materialized views come in.
-- MAGIC  They help you organize, reuse, and optimize SQL logic — turning repetitive queries into reusable, reliable building blocks.
-- MAGIC
-- MAGIC ### The Story Before Views
-- MAGIC Imagine your manager asks for a report of each department showing:
-- MAGIC
-- MAGIC - The number of employees
-- MAGIC - The total salary spent
-- MAGIC - The average salary per person
-- MAGIC
-- MAGIC You write a query like this:

-- COMMAND ----------

SELECT d.dept_name,
       COUNT(e.emp_id) AS employee_count,
       SUM(e.salary) AS total_salary,
       ROUND(AVG(e.salary), 2) AS avg_salary
FROM employees e
JOIN departments d
  ON e.dept_id = d.dept_id
GROUP BY d.dept_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The query works perfectly.
-- MAGIC  But then, tomorrow, someone from finance asks for the same data in another dashboard.
-- MAGIC  Next week, the HR team wants to add a filter by department location.
-- MAGIC  Every time, you copy this query and tweak it.
-- MAGIC  Soon you have multiple versions scattered across dashboards, notebooks, and pipelines — each doing the same work slightly differently.
-- MAGIC
-- MAGIC This is the problem views solve.
-- MAGIC
-- MAGIC ###  What is a View?
-- MAGIC A view is like saving a query result as a virtual table.
-- MAGIC  You don’t store the data itself — just the SQL logic behind it.
-- MAGIC  Whenever someone queries the view, Spark runs the underlying query dynamically and returns the latest results.
-- MAGIC
-- MAGIC You can think of a view as a named query, or simply:
-- MAGIC
-- MAGIC “A reusable SQL statement that behaves like a table.”
-- MAGIC
-- MAGIC Here’s how to create one in Spark SQL:

-- COMMAND ----------

CREATE OR REPLACE VIEW dept_salary_summary AS
SELECT d.dept_name,
       COUNT(e.emp_id) AS employee_count,
       SUM(e.salary) AS total_salary,
       ROUND(AVG(e.salary), 2) AS avg_salary
FROM employees e
JOIN departments d
  ON e.dept_id = d.dept_id
GROUP BY d.dept_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, instead of rewriting that entire query every time, you can just do:

-- COMMAND ----------

SELECT * FROM dept_salary_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The result is the same — but the logic is centralized, versioned, and easy to maintain.
-- MAGIC
-- MAGIC ### Why Views Are Useful
-- MAGIC - Reusability — Once a view is defined, everyone can use it.
-- MAGIC - Consistency — All teams rely on the same definition of metrics.
-- MAGIC - Security — You can expose a view to users instead of the raw table (masking sensitive columns).
-- MAGIC - Simplicity — Dashboards and reports become lightweight — no need for complex embedded SQL.
-- MAGIC
-- MAGIC For a data engineer, views represent a semantic layer — they bridge raw data and analytics in a controlled, reliable way.
-- MAGIC
-- MAGIC ### What is a Materialized View?
-- MAGIC A materialized view looks similar but behaves differently.
-- MAGIC  While a regular view runs its query every time it’s accessed, a materialized view stores the results.
-- MAGIC
-- MAGIC Think of it as a snapshot of the query output, saved as a table, which can be refreshed periodically.
-- MAGIC
-- MAGIC In Spark SQL (and Databricks), you can create a materialized view like this:

-- COMMAND ----------

CREATE MATERIALIZED VIEW dept_salary_summary_mv
AS
SELECT d.dept_name,
       COUNT(e.emp_id) AS employee_count,
       SUM(e.salary) AS total_salary,
       ROUND(AVG(e.salary), 2) AS avg_salary
FROM employees e
JOIN departments d
  ON e.dept_id = d.dept_id
GROUP BY d.dept_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, Spark doesn’t re-run the entire query each time.
-- MAGIC  It reads directly from stored results, which makes repeated access faster and cheaper — especially for heavy analytical queries.
-- MAGIC
-- MAGIC When new data comes in, you can refresh the materialized view to update it:

-- COMMAND ----------

ALTER MATERIALIZED VIEW dept_salary_summary_mv REFRESH;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can think of it like this:
-- MAGIC
-- MAGIC A view is a recipe, and a materialized view is a prepared meal based on that recipe.
-- MAGIC
-- MAGIC Why Data Engineers Love Materialized Views
-- MAGIC In modern data pipelines, some queries — like daily KPIs or dashboards — run dozens of times a day.
-- MAGIC  If each run scans millions of rows, that’s expensive and slow.
-- MAGIC
-- MAGIC By materializing those results once, you:
-- MAGIC
-- MAGIC - Save compute cost by not re-running complex joins or aggregations.
-- MAGIC - Reduce latency for dashboards and APIs.
-- MAGIC - Simplify data architecture by pre-staging data in a form ready for analytics.
-- MAGIC
-- MAGIC Materialized views are especially powerful in lakehouse architectures like Databricks, where they integrate seamlessly with Delta tables and can be incrementally refreshed as new data arrives.
-- MAGIC
-- MAGIC Combining the Two in Practice
-- MAGIC A practical data engineer uses both:
-- MAGIC
-- MAGIC - Views for logical organization (business definitions, reusable queries).
-- MAGIC - Materialized views for performance optimization (cached results for analytics).
-- MAGIC
-- MAGIC For example:
-- MAGIC
-- MAGIC - Create a view employee_salary_summary for your team to access.
-- MAGIC - Build a materialized view employee_salary_summary_mv behind it, refreshed daily.
-- MAGIC - Your dashboard queries the view, but the heavy lifting is done by the materialized version.
-- MAGIC
-- MAGIC This pattern keeps logic clean, cost low, and data fresh enough for daily analytics.
-- MAGIC
-- MAGIC # Closing Thought
-- MAGIC Views and materialized views are not just technical tools — they’re architectural patterns.
-- MAGIC  They represent how we separate business logic from data storage, and how we build scalable, maintainable systems.
-- MAGIC
-- MAGIC As a data engineer, your job isn’t just to process data; it’s to make it usable, performant, and trusted.
-- MAGIC  Views bring order. Materialized views bring efficiency.
-- MAGIC  Together, they turn SQL from a collection of queries into a well-structured data ecosystem.
-- MAGIC
-- MAGIC In our final chapter tomorrow, we’ll bring everything together — creating a complete data pipeline that uses everything we’ve learned: tables, joins, aggregations, and materialized views — all working together in a real-world Spark SQL example.
-- MAGIC
