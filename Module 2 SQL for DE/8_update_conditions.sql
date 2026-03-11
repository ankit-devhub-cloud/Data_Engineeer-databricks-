-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UPDATE with Conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Databases are living systems. Employees join, leave, or get promoted. Departments change names or locations. Salaries get revised. If all we ever did was insert data, the database would quickly become outdated. That's why SQL gives us the UPDATE statement - the way to modify existing records without creating duplicates.
-- MAGIC
-- MAGIC But with this power comes responsibility. An update can be precise and surgical, or it can be destructive if done carelessly. The key is learning how to update safely, with conditions.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # The Role of UPDATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unlike INSERT, which adds new rows, UPDATE changes values in rows that already exist. But here's the important detail: if you run an UPDATE without conditions, it will change every row in the table.
-- MAGIC
-- MAGIC That's why conditions are so critical. They tell the database which specific rows should be updated.

-- COMMAND ----------

select * from thedataengineering_00.company.employees WHERE emp_id = 102;

-- COMMAND ----------

UPDATE thedataengineering_00.company.employees
SET salary = 85000
WHERE emp_id = 102;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Updating Based on Conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC You can also update multiple rows at once by setting conditions.

-- COMMAND ----------

select * from thedataengineering_00.company.employees where dept_id = 3

-- COMMAND ----------

UPDATE thedataengineering_00.company.employees
SET salary = salary * 1.05
WHERE dept_id = 3;

-- COMMAND ----------

select * from thedataengineering_00.company.employees where dept_id = 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Updating Multiple Columns

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sometimes you need to modify more than one piece of information at once.

-- COMMAND ----------

select * from thedataengineering_00.company.employees where emp_id = 105

-- COMMAND ----------

UPDATE thedataengineering_00.company.employees
SET last_name = 'Williams1',
    salary = 50000
WHERE emp_id = 105;

-- COMMAND ----------

select * from thedataengineering_00.company.employees where emp_id = 105

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Avoiding Mistakes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The danger with UPDATE is forgetting the WHERE clause or writing it incorrectly. Always remember:
-- MAGIC
-- MAGIC - Without WHERE, every row is updated.
-- MAGIC - With the wrong condition, you might update the wrong set of rows.
-- MAGIC
-- MAGIC
-- MAGIC A safe practice is to first run a SELECT with the same condition to see which rows will be affected.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For data engineers, UPDATE is more than just fixing typos or changing salaries. It's part of keeping data fresh, accurate, and trustworthy. You might:
-- MAGIC
-- MAGIC - Correct errors in ingested data.
-- MAGIC - Adjust values when upstream systems change.
-- MAGIC - Apply transformation logic directly in staging tables.
-- MAGIC
-- MAGIC
-- MAGIC But because updates can alter large amounts of data in seconds, they must be done with discipline. Always double-check the conditions, and in production systems, updates are often logged or run within transactions to ensure safety.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Closing Thought

-- COMMAND ----------

-- MAGIC %md
-- MAGIC INSERT introduced life into your tables. SELECT gave you the power to ask questions. Now, UPDATE gives you the ability to evolve your data over time. It's a reminder that databases are not static - they change as the real world changes.
-- MAGIC
-- MAGIC Handled with care, UPDATE is your ally in keeping data aligned with reality. Handled recklessly, it can wipe out accuracy in seconds. As a data engineer, that's why you must always treat UPDATE with respect.
-- MAGIC
-- MAGIC In our next lesson, we'll look at the other side of change: deleting data. Sometimes rows need to be removed entirely, and SQL gives us the DELETE operation to do just that.
