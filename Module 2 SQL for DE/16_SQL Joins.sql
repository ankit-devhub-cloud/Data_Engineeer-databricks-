-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL Joins — INNER, LEFT, RIGHT, FULL Explained Visually
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the real world, data rarely lives in one single table. Employees belong to departments. Orders belong to customers. Transactions link to products. To get meaningful insights, you often need to combine tables.
-- MAGIC
-- MAGIC That’s what JOINs are for. They allow you to connect rows from two tables based on a shared column. In our case, the shared column is dept_id — it links employees to departments.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The Setup
-- MAGIC Let’s remind ourselves of our two tables:
-- MAGIC
-- MAGIC ### Department Table
-- MAGIC
-- MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/department.png)
-- MAGIC
-- MAGIC ### Employees Table
-- MAGIC
-- MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/employees.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is the perfect playground to learn JOINs.
-- MAGIC
-- MAGIC ### 1. INNER JOIN
-- MAGIC Visual: Only the overlap (like the intersection in a Venn diagram).
-- MAGIC  It returns rows where the dept_id exists in both tables.

-- COMMAND ----------

SELECT e.first_name, e.last_name, d.dept_name
FROM thedataengineering_00.company.employees e
INNER JOIN thedataengineering_00.company.department d
  ON e.dept_id = d.dept_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  Result: Every employee with a valid department.
-- MAGIC  
-- MAGIC  Excludes departments with no employees.
-- MAGIC
-- MAGIC So if we had a department 5 with no employees, it would not appear here.
-- MAGIC
-- MAGIC ### 2. LEFT JOIN
-- MAGIC Visual: All rows from the left table (employees), plus matches from the right table (departments).
-- MAGIC  If no match, you still keep the employee, but department info will be NULL.

-- COMMAND ----------

SELECT e.first_name, e.last_name, d.dept_name
FROM employees e
LEFT JOIN departments d
  ON e.dept_id = d.dept_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Result: Every employee is shown.
-- MAGIC If an employee belongs to a department that doesn’t exist in departments, the department name will just show as NULL.
-- MAGIC
-- MAGIC This is useful when you care about “all employees, even if their department is missing.”
-- MAGIC
-- MAGIC ### 3. RIGHT JOIN
-- MAGIC Visual: The opposite of LEFT. All rows from the right table (departments), plus matching employees.

-- COMMAND ----------

SELECT e.first_name, e.last_name, d.dept_name
FROM employees e
RIGHT JOIN departments d
  ON e.dept_id = d.dept_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Result: Every department is shown, even if it has no employees.
-- MAGIC For example, if Marketing (dept_id = 4) had no employees, it would still appear, but employee columns would be NULL.
-- MAGIC
-- MAGIC This is useful when you care about “all departments, even if no one is assigned yet.”
-- MAGIC
-- MAGIC ### 4. FULL OUTER JOIN
-- MAGIC Visual: Everything from both sides.
-- MAGIC  It combines LEFT and RIGHT — all employees, all departments, matched where possible, and NULLs where not.

-- COMMAND ----------

SELECT e.first_name, e.last_name, d.dept_name
FROM employees e
FULL OUTER JOIN departments d
  ON e.dept_id = d.dept_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Result:
-- MAGIC
-- MAGIC Employees with valid departments.
-- MAGIC
-- MAGIC Employees without valid departments (still listed, dept = NULL).
-- MAGIC
-- MAGIC Departments with no employees (still listed, employee = NULL).
-- MAGIC
-- MAGIC This join is the most inclusive and ensures no data is left out.
-- MAGIC
-- MAGIC ### Why Joins Matter for Data Engineers
-- MAGIC As a data engineer, JOINs are one of your most powerful tools. They allow you to:
-- MAGIC
-- MAGIC Combine transactional data with reference data (e.g., orders + products).
-- MAGIC
-- MAGIC Enrich raw logs with lookup tables.
-- MAGIC
-- MAGIC Build dimensional models for analytics.
-- MAGIC
-- MAGIC But they also require care — large JOINs can be expensive in distributed systems, and the wrong type of JOIN can change your results dramatically.
-- MAGIC
-- MAGIC # Closing Thought
-- MAGIC
-- MAGIC - INNER JOIN → Only matching rows.
-- MAGIC - LEFT JOIN → All from left, matches from right.
-- MAGIC - RIGHT JOIN → All from right, matches from left.
-- MAGIC - FULL OUTER JOIN → Everything from both sides.
-- MAGIC
-- MAGIC Think of JOINs as the way to connect dots across tables. Once you master them, you’ll unlock the ability to answer much more complex questions with SQL.
-- MAGIC
-- MAGIC Next, we’ll move to something equally practical: how to remove duplicates from your data — a task every data engineer faces regularly.
