-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### DISTINCT vs ROW_NUMBER in Spark SQL
-- MAGIC In a perfect world, every record in your table would be unique.
-- MAGIC  In reality? Duplicates creep in — sometimes because of bad upstream data, other times due to incorrect joins or multiple data sources merging.
-- MAGIC
-- MAGIC For a data engineer, knowing how to identify and remove duplicates is a critical skill. Spark SQL gives us two powerful ways to do this:
-- MAGIC
-- MAGIC - Using DISTINCT — for simple cases.
-- MAGIC - Using ROW_NUMBER() — for advanced control.
-- MAGIC
-- MAGIC Let’s understand both clearly.
-- MAGIC
-- MAGIC ### The Setup
-- MAGIC Imagine your employees table has accidentally received duplicate entries for some employees due to an ingestion issue.
-- MAGIC
-- MAGIC Employees
-- MAGIC
-- MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/employees_duplicates.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Clearly, Bob and Diana have duplicate rows. Let’s fix that.
-- MAGIC
-- MAGIC ### Method 1: Using DISTINCT
-- MAGIC DISTINCT removes duplicates by returning only unique combinations of columns.
-- MAGIC
-- MAGIC ### Example 1: Removing complete row duplicates

-- COMMAND ----------

SELECT DISTINCT *
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This returns only unique rows — any duplicate rows that are exactly identical across all columns will be eliminated.
-- MAGIC
-- MAGIC ### Example 2: Unique employees based on name and email

-- COMMAND ----------

SELECT DISTINCT first_name, last_name, email
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is useful when you don’t care about other columns like salary or department — you just want unique combinations of these attributes.
-- MAGIC
-- MAGIC Best for: Quick cleanup when duplicates are exact and no tie-breaking logic is needed.Limitation: Doesn’t let you control which duplicate to keep if rows differ slightly (e.g., same employee but different salary).
-- MAGIC
-- MAGIC ### Method 2: Using ROW_NUMBER() — When You Need Control
-- MAGIC In real-world data, duplicates are often not exact. Maybe one row has an older salary, or one was updated later. In such cases, DISTINCT isn’t enough — you need to decide which record to keep.
-- MAGIC
-- MAGIC That’s where the ROW_NUMBER() window function comes in. It assigns a unique sequential number to each row within a partition, based on ordering.
-- MAGIC
-- MAGIC Example 3: Keeping only the latest record per employee

-- COMMAND ----------

SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY hire_date DESC) AS row_num
  FROM employees
)
WHERE row_num = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let’s break it down:
-- MAGIC
-- MAGIC - PARTITION BY emp_id means: treat all rows for the same employee together.
-- MAGIC - ORDER BY hire_date DESC means: give priority to the most recent record.
-- MAGIC - ROW_NUMBER() assigns 1 to the latest, 2 to the next, and so on.
-- MAGIC - The outer query keeps only row_num = 1.
-- MAGIC
-- MAGIC Best for: Datasets where duplicates have variations (different timestamps, salaries, etc.).
-- MAGIC  Control: You decide which record wins — the newest, the oldest, or by any other logic.
-- MAGIC
-- MAGIC ### Combining Approaches in Practice
-- MAGIC Sometimes, you’ll first use DISTINCT to quickly clean the exact duplicates, and then apply ROW_NUMBER() for fine-grained deduplication logic — especially in production pipelines where data comes from multiple sources or ingestion streams.
-- MAGIC
-- MAGIC ### Real-World Example for Data Engineers
-- MAGIC Imagine you’re building a pipeline that merges daily employee data from multiple HR systems.
-- MAGIC
-- MAGIC - Some systems send duplicate rows for the same employee.
-- MAGIC - Others send slightly different versions (maybe salary updated).
-- MAGIC
-- MAGIC In your Spark SQL ETL job, you’d:
-- MAGIC
-- MAGIC Use DISTINCT to remove exact duplicates early.
-- MAGIC
-- MAGIC Use ROW_NUMBER() to retain only the latest version based on last_updated timestamp.
-- MAGIC
-- MAGIC This ensures clean, reliable, and up-to-date data — a core responsibility of any data engineer.
-- MAGIC
-- MAGIC # Closing Thought
-- MAGIC Both DISTINCT and ROW_NUMBER() are tools to fight the same problem — data duplication — but they work at different levels of sophistication.
-- MAGIC
-- MAGIC - Use DISTINCT when duplicates are exact and simple.
-- MAGIC - Use ROW_NUMBER() when you need to choose which duplicate to keep.
-- MAGIC
-- MAGIC Clean data doesn’t just make queries faster — it makes insights trustworthy.
-- MAGIC  For a data engineer, mastering deduplication means mastering data integrity.
-- MAGIC
-- MAGIC In our next session, we’ll move into one of the most powerful parts of SQL analytics: Aggregations and GROUP BY, where you’ll learn how to summarize and measure your data.
