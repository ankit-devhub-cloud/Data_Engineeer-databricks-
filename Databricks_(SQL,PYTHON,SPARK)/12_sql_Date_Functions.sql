-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SQL Functions 
-- MAGIC
-- MAGIC ## Numeric & Date Functions (with Spark SQL Examples)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Numbers and dates are central to most business data. Salaries, employee IDs, and hire dates are not just stored, they are calculated, compared, and analyzed every day. Spark SQL, just like other SQL engines, provides a wide range of numeric and date functions to work with this data efficiently.
-- MAGIC
-- MAGIC Let's walk through them with simple examples on our familiar employees table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  1. ROUND - Rounding Numbers

-- COMMAND ----------

select round(238782.293,2)

-- COMMAND ----------

SELECT emp_id, salary, ROUND(salary, -3) AS rounded_salary
FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. CEIL and FLOOR - Always Up or Down

-- COMMAND ----------

SELECT emp_id, salary,
       CEIL(salary/1000.0) * 1000 AS ceil_salary,
       FLOOR(salary/1000.0) * 1000 AS floor_salary
FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. ABS - Absolute Value

-- COMMAND ----------

SELECT ABS(-5000) AS positive_value;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Arithmetic in Queries

-- COMMAND ----------

SELECT emp_id, salary,
       salary * 1.10 AS new_salary   -- 10% increment
FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Date Functions in Spark SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Current Date

-- COMMAND ----------

SELECT current_date() AS today

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extracting Year, Month, Day

-- COMMAND ----------

SELECT emp_id, hire_date,
       year(hire_date)  AS hire_year,
       month(hire_date) AS hire_month,
       day(hire_date)   AS hire_day
FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Date Difference
-- MAGIC
-- MAGIC To calculate how many days an employee has been in the company:

-- COMMAND ----------

SELECT emp_id, first_name, hire_date,
       datediff(current_date(), hire_date) AS days_in_company
FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Add Days or Months

-- COMMAND ----------

SELECT emp_id, hire_date,
       date_add(hire_date, 90)    AS after_90_days,
       add_months(hire_date, 12) AS after_1_year
FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Truncating Dates (to Month/Year)

-- COMMAND ----------

SELECT emp_id, hire_date,
       trunc(hire_date, 'MM') AS month_start,
       trunc(hire_date, 'YYYY') AS year_start
FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In real-world pipelines, data rarely comes perfectly formatted. You may need to:
-- MAGIC Put salaries into ranges (numeric functions).
-- MAGIC
-- MAGIC - Standardize increments (arithmetic).
-- MAGIC - Calculate tenure or filter by hire dates (date functions).
-- MAGIC - Aggregate over months or years (truncation).
-- MAGIC
-- MAGIC With Spark SQL, these operations are not just quick but also scalable - you can run the same functions on thousands or billions of rows.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Closing Thought

-- COMMAND ----------

-- MAGIC %md
-- MAGIC String functions gave us the power to clean and manipulate text. Numeric and date functions now help us measure, calculate, and track. Together, they allow us to transform raw data into meaningful insights that are ready for analysis.
-- MAGIC
-- MAGIC Next, we'll explore how SQL lets you add logic into queries - the CASE WHEN expression, which acts like "if-else" for data. This is one of the most powerful ways to shape your results directly in SQL.
