-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Reading Data in Different Ways

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By now, our departments and employees tables are no longer empty blueprints. They have records - names, salaries, departments, hire dates. But a database filled with data is useless until we can read it back in meaningful ways. That's what the SELECT statement is for.
-- MAGIC
-- MAGIC If INSERT is about writing a story into the database, SELECT is about reading it back. This is the SQL command you will use most often, not just as a beginner but throughout your career.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # The Simplest Form

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The most basic way to use SELECT is to return everything from a table.

-- COMMAND ----------

SELECT * FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Selecting with column names

-- COMMAND ----------

SELECT emp_id,salary FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Filtering with WHERE

-- COMMAND ----------

SELECT first_name, last_name 
FROM thedataengineering_00.company.employees
WHERE dept_id = 2;

-- COMMAND ----------

SELECT first_name, last_name, salary
FROM thedataengineering_00.company.employees
WHERE salary > 70000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Filtering with Ranges

-- COMMAND ----------

SELECT first_name, last_name, hire_date
FROM thedataengineering_00.company.employees
WHERE hire_date BETWEEN '2019-01-01' AND '2021-12-31';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Filtering with Lists

-- COMMAND ----------

SELECT first_name, last_name, dept_id
FROM thedataengineering_00.company.employees
WHERE dept_id IN (1, 3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Combining Conditions

-- COMMAND ----------

SELECT first_name, last_name, salary
FROM thedataengineering_00.company.employees
WHERE dept_id = 3 AND salary > 60000;

-- COMMAND ----------

SELECT first_name, last_name, dept_id, salary
FROM thedataengineering_00.company.employees
WHERE dept_id = 1 OR salary > 90000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Sorting Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Databases don't guarantee any natural order of rows. If you want results sorted, you use ORDER BY.

-- COMMAND ----------

SELECT first_name, last_name, salary
FROM thedataengineering_00.company.employees
ORDER BY salary DESC;

-- COMMAND ----------

SELECT first_name, last_name, hire_date
FROM thedataengineering_00.company.employees
ORDER BY hire_date ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Aliases - Friendlier Output

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sometimes column names are too technical. You can rename them in the output using AS.

-- COMMAND ----------

SELECT 
first_name AS name,
last_name  AS surname,
salary     AS compensation
FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Closing Thought

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By mastering SELECT, you've unlocked the ability to interrogate data. Instead of scrolling through endless rows like in Excel, you ask precise questions - "show me employees hired between these dates," "list everyone earning above this amount," "give me the top earners in Finance."
-- MAGIC
-- MAGIC This is the moment SQL starts to feel magical: with just a few lines, you can sift through thousands or millions of rows and instantly see only what matters.
-- MAGIC
-- MAGIC In our next lesson, we'll go beyond reading - we'll learn how to update existing data, changing salaries, correcting mistakes, and keeping records fresh.
