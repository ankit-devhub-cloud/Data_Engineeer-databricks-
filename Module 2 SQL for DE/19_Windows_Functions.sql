-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Window Functions for Analytics - ROW_NUMBER, RANK, PARTITION BY

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If GROUP BY helps us summarize data, window functions help us analyze it without losing detail.
-- MAGIC
-- MAGIC In the previous lesson, when we grouped by department, each group collapsed into one row — one result per department.
-- MAGIC  But what if we want to calculate something for each employee, within their department, without collapsing everything?
-- MAGIC  For example:
-- MAGIC
-- MAGIC - Who is the highest-paid employee in each department?
-- MAGIC - How does each salary compare to their department’s average?
-- MAGIC - Which employee joined first in every team?
-- MAGIC
-- MAGIC These kinds of questions require contextual comparisons — comparing a row against others in the same group — and that’s exactly what window functions do.
-- MAGIC
-- MAGIC ### Understanding the “Window”
-- MAGIC Think of a window as a view or frame of rows that SQL looks at for each calculation.
-- MAGIC  When you apply a window function, SQL doesn’t shrink your data like GROUP BY; it adds new insights to every row — like adding an extra column that shows rank, average, or count within a certain scope.
-- MAGIC
-- MAGIC In Spark SQL, we define this “scope” using the OVER() clause, often combined with PARTITION BY to divide the dataset into logical groups.
-- MAGIC
-- MAGIC ### Example Dataset
-- MAGIC We’ll work with this familiar table:
-- MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/employees.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. ROW_NUMBER() — Giving Each Row a Unique Sequence
-- MAGIC
-- MAGIC Let’s start simple.
-- MAGIC  If you want to number all employees in order of their salary (highest to lowest), you can use ROW_NUMBER().

-- COMMAND ----------

SELECT first_name, last_name, dept_id, salary,
       ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This assigns 1 to the highest-paid employee overall, 2 to the next, and so on. Unlike a plain ORDER BY, this doesn’t just sort — it labels every row with its position in the ranking.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. PARTITION BY — Restarting the Window per Group
-- MAGIC
-- MAGIC Now suppose you want to rank employees within their departments, not across the whole company.
-- MAGIC  That’s where PARTITION BY comes in.

-- COMMAND ----------

SELECT first_name, last_name, dept_id, salary,
       ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rank
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This query tells Spark SQL:
-- MAGIC
-- MAGIC “For each department (partition), order employees by salary, and number them from 1 onward.”

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Notice how the ranking restarts for every department.
-- MAGIC That’s the power of PARTITION BY — it resets your window for each logical group.
-- MAGIC
-- MAGIC ### 3. RANK() — Handling Ties Gracefully
-- MAGIC Sometimes, two employees might have the same salary.
-- MAGIC  With ROW_NUMBER(), they still get different numbers — 1 and 2.
-- MAGIC  But RANK() treats equal values the same and “skips” the next rank number.
-- MAGIC
-- MAGIC Let’s illustrate with an example. Imagine we add another employee to IT with the same salary as Charlie (50,000):

-- COMMAND ----------

SELECT dept_id, first_name, salary,
       RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rank_in_dept
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If two employees in IT have the same salary, they both get rank 2 — and the next rank will be 4, not 3.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This small difference matters a lot when you want to reflect true position based on shared values.
-- MAGIC
-- MAGIC ### 4. DENSE_RANK() — Keeping Sequence Continuous
-- MAGIC If you prefer not to skip ranks when there are ties, you can use DENSE_RANK().

-- COMMAND ----------

SELECT dept_id, first_name, salary,
       DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dense_rank_in_dept
FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here, both Charlie and Jason (50,000) get rank 2 — but the next employee would get rank 3 (not 4).
-- MAGIC
-- MAGIC This function is very common in leaderboards, sales performance tracking, or performance reviews where rank gaps don’t make sense.
-- MAGIC
-- MAGIC ### 5. Combining Window Functions with Aggregates
-- MAGIC Window functions can also compute aggregates per group while still showing individual rows.
-- MAGIC  For example, suppose you want to show each employee’s salary alongside the average salary of their department:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This type of comparison is nearly impossible with simple GROUP BY — you’d lose the employee-level detail.
-- MAGIC
-- MAGIC Why Window Functions Matter for Data Engineers
-- MAGIC Window functions are the bridge between row-level data and aggregated insight.
-- MAGIC  They allow you to:
-- MAGIC
-- MAGIC - Rank and sort within groups.
-- MAGIC - Compare individuals to group metrics.
-- MAGIC - Build analytical models directly in SQL (no need for separate joins).
-- MAGIC
-- MAGIC In real data engineering workflows — whether in HR analytics, finance reporting, or customer segmentation — you’ll use window functions to calculate rolling sums, find top-N per group, and generate time-based comparisons in streaming datasets.
-- MAGIC
-- MAGIC Spark SQL supports all of this natively and scales it across massive distributed data — meaning you can compute rankings and averages across billions of rows as easily as six.
-- MAGIC
-- MAGIC # Closing Thought
-- MAGIC GROUP BY summarizes data by collapsing rows.
-- MAGIC  Window functions expand data by adding context to each row.
-- MAGIC
-- MAGIC They help you answer questions like:
-- MAGIC
-- MAGIC - “How does this employee compare to others in their team?”
-- MAGIC - “Who are the top performers in each region?”
-- MAGIC - “What’s the trend over time for each product?”
-- MAGIC - 
-- MAGIC Learning to think in “windows” is like learning a new dimension of SQL — you’re no longer reading the table row by row, but through a moving lens that compares each row to its surroundings.
-- MAGIC
-- MAGIC In our next session, we’ll bring all these ideas together and learn how to build analytical queries that blend aggregates, joins, and windows — the kind of queries every modern data engineer writes daily.
