-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL vs DML Explained Simply

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Every data engineer eventually realizes this:
-- MAGIC
-- MAGIC  Not all SQL statements are the same. Some build the structure of the database; others fill that structure with data.
-- MAGIC
-- MAGIC  Understanding this difference is like understanding the blueprint versus the building - one designs, the other lives and breathes.
-- MAGIC
-- MAGIC These two worlds of SQL are called DDL (Data Definition Language) and DML (Data Manipulation Language).
-- MAGIC
-- MAGIC  They look similar at first glance, but their roles in a data engineer's life are very different.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/ddlvsdml.jpg)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The Two Sides of SQL
-- MAGIC
-- MAGIC Think of your database as a house.
-- MAGIC When you lay the foundation, build rooms, and decide how the wiring should run - that's DDL.
-- MAGIC
-- MAGIC When you move in furniture, paint walls, or rearrange things - that's DML.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DDL - Defining the Structure

-- COMMAND ----------

-- MAGIC %md
-- MAGIC DDL stands for Data Definition Language.
-- MAGIC
-- MAGIC  These commands tell Spark (or any SQL engine) what the database should look like.
-- MAGIC
-- MAGIC  You use DDL when you're creating, altering, or removing objects like tables, views, or databases.
-- MAGIC
-- MAGIC Common DDL statements include:
-- MAGIC
-- MAGIC - CREATE – to create new objects
-- MAGIC - ALTER – to modify existing objects
-- MAGIC - DROP – to delete them
-- MAGIC - TRUNCATE – to remove all data but keep the structure
-- MAGIC
-- MAGIC Let's look at some simple Spark SQL examples.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS company_db;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a Table

-- COMMAND ----------

CREATE OR REPLACE TABLE employees (
  emp_id INT,
  first_name STRING,
  last_name STRING,
  dept_id INT,
  salary DOUBLE,
  hire_date DATE
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Altering a Table

-- COMMAND ----------

ALTER TABLE employees ADD COLUMNS (email STRING);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dropping a table

-- COMMAND ----------

DROP TABLE IF EXISTS employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DML - Manipulating the Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once your tables are defined, it's time to work inside them.
-- MAGIC
-- MAGIC  This is where DML (Data Manipulation Language) comes in.
-- MAGIC
-- MAGIC  DML deals with inserting, updating, deleting, and reading records.
-- MAGIC
-- MAGIC Common DML statements include:
-- MAGIC
-- MAGIC - INSERT – to add data
-- MAGIC - UPDATE – to change data
-- MAGIC - DELETE – to remove data
-- MAGIC - SELECT – to retrieve data
-- MAGIC
-- MAGIC Let's see them in action.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

INSERT INTO employees (emp_id, first_name, last_name, dept_id, salary, hire_date)
VALUES (101, 'Alice', 'Johnson', 1, 60000, '2020-01-15');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Data

-- COMMAND ----------

UPDATE employees
SET salary = 65000
WHERE emp_id = 101;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deleting Data

-- COMMAND ----------

DELETE FROM employees
WHERE dept_id = 4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Selecting Data

-- COMMAND ----------

SELECT first_name, last_name, salary
FROM employees
WHERE salary > 70000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For a data engineer, understanding DDL and DML isn't just academic - it's operational.
-- MAGIC
-- MAGIC When building data pipelines or lakehouse architectures in Spark, you constantly switch between the two:
-- MAGIC
-- MAGIC - You use DDL to design your schema - defining how tables, views, and databases connect.
-- MAGIC - You use DML to load, transform, and analyze data flowing through those structures.
-- MAGIC
-- MAGIC And here's the subtle but crucial distinction:
-- MAGIC DDL affects the metadata - it changes your catalog, your definitions, your structure.
-- MAGIC
-- MAGIC DML affects the data itself - it changes what's stored, but not the underlying design.
-- MAGIC
-- MAGIC In a large team environment, these differences also translate into permissions:
-- MAGIC
-- MAGIC Data engineers or admins often have DDL privileges (to create or alter objects).
-- MAGIC
-- MAGIC Analysts and dashboard users usually get DML privileges (to query or modify data)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Closing Thought

-- COMMAND ----------

-- MAGIC %md
-- MAGIC DDL and DML are the two languages every data engineer must be fluent in.
-- MAGIC DDL builds the bones of your data system.
-- MAGIC DML fills it with life.
-- MAGIC
-- MAGIC Without DDL, there's nowhere to store data.
-- MAGIC  Without DML, there's nothing worth storing.
-- MAGIC Understanding when to use each - and how they complement each other - is what separates someone who "writes queries" from someone who designs data systems.
