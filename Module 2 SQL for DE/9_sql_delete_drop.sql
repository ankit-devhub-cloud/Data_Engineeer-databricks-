-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DELETE from Table vs DROP Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By now, we've learned how to add data with INSERT, explore it with SELECT, and change it with UPDATE. But what happens when data is no longer needed? Maybe an employee has left the company. Maybe a department was mistakenly added. Or maybe, in rare cases, you don't just want to remove rows - you want to remove the entire table itself.
-- MAGIC
-- MAGIC This is where we need to understand the difference between DELETE and DROP. Both sound like they remove data, but they operate at very different levels.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DELETE - Removing Rows

-- COMMAND ----------

select * from thedataengineering_00.company.employees WHERE emp_id = 104;

-- COMMAND ----------

DELETE FROM thedataengineering_00.company.employees WHERE emp_id = 104;

-- COMMAND ----------

select * from thedataengineering_00.company.employees WHERE emp_id = 104;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Remove all employees in a department

-- COMMAND ----------

select * from thedataengineering_00.company.employees
-- WHERE dept_id = 4

-- COMMAND ----------

DELETE FROM thedataengineering_00.company.employees WHERE dept_id = 4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delete all the employees

-- COMMAND ----------

DELETE FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DROP - Removing the Entire Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While DELETE removes rows, DROP goes a step further: it deletes the entire table structure. Once dropped, the table is gone - its data, its columns, its constraints, everything.

-- COMMAND ----------

DROP TABLE thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. DELETE → removes specific rows.
-- MAGIC 2. TRUNCATE → removes all rows, keeps the table.
-- MAGIC 3. DROP → removes the table itself.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Closing Thought

-- COMMAND ----------

-- MAGIC %md
-- MAGIC DELETE is like removing people from an office but keeping the building.
-- MAGIC
-- MAGIC TRUNCATE is like clearing the office of everyone at once, leaving it empty but standing.
-- MAGIC
-- MAGIC DROP is like demolishing the building itself.
-- MAGIC
-- MAGIC Each command has its place. As a data engineer, your responsibility is to choose the right tool for the right job and always double-check before running them on real systems.
