-- Databricks notebook source
-- MAGIC %md
-- MAGIC # How to Create Databases & Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Up until now, we've been talking about databases in theory - how they are like Excel sheets, why SQL matters, and how data types give structure. But today we move to the first real act of building: creating a database and designing tables.
-- MAGIC
-- MAGIC This is where things get real. A table is more than just a container for data; it is a disciplined structure that decides how information will be stored, retrieved, and trusted for years to come. 
-- MAGIC
-- MAGIC For a data engineer, this step is as critical as laying the foundation of a building. Get it wrong, and the cracks will appear later in your pipelines, dashboards, and business decisions.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating a Database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Catalog -> Schema -> Tables/Views

-- COMMAND ----------


CREATE CATALOG IF NOT EXISTS thedataengineering_00;
CREATE SCHEMA IF NOT EXISTS company;

USE CATALOG thedataengineering_00;
USE SCHEMA company;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Designing Tables - More Than Columns

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When we design a table, we don't just say "here are the columns." We also define rules that enforce order:
-- MAGIC
-- MAGIC - What kind of data belongs in each column (the data types we learned yesterday).
-- MAGIC - Which column identifies each row uniquely.
-- MAGIC - Which columns should never be duplicated.
-- MAGIC
-- MAGIC
-- MAGIC These rules protect us from chaos. They make sure no two employees accidentally get the same ID, no two departments share the same name, and every employee record remains valid.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Primary Key - The Identity of a Row

-- COMMAND ----------

-- MAGIC %md
-- MAGIC One of the first rules you set is the primary key. Think of it as the fingerprint of a record:
-- MAGIC
-- MAGIC - It must be unique.
-- MAGIC - It can never be empty.
-- MAGIC - It guarantees that each row in the table can always be identified.
-- MAGIC
-- MAGIC In our schema, the emp_id column in the employees table is the perfect primary key. No two employees should share the same ID, and every employee must have one. Without it, the database would struggle to tell Alice Johnson apart from another Alice Johnson.
-- MAGIC
-- MAGIC Similarly, in the departments table, the dept_id is the natural primary key.
-- MAGIC
-- MAGIC Here's how we create the departments table with a primary key:

-- COMMAND ----------


show create table thedataengineering_00.company.employees

-- COMMAND ----------

CREATE TABLE if not exists thedataengineering_00.company.employees(
  emp_id BIGINT,
  first_name STRING,
  last_name STRING,
  age BIGINT,
  gender STRING,
  salary BIGINT,
  dept_id BIGINT,
  hire_date DATE,
  CONSTRAINT pk_empid1 PRIMARY KEY (emp_id))
USING delta

-- COMMAND ----------


show create table thedataengineering_00.company.employees_test

-- COMMAND ----------



drop table thedataengineering_00.company.employees_test;

CREATE TABLE if not exists thedataengineering_00.company.employees_test (
  emp_id BIGINT,
  first_name STRING,
  last_name STRING,
  age BIGINT,
  gender STRING,
  salary BIGINT,
  dept_id BIGINT,
  hire_date DATE,
  CONSTRAINT pk_empid PRIMARY KEY (emp_id))
USING delta;

-- COMMAND ----------


insert into thedataengineering_00.company.employees
values(1,'John','Smith',30,'Male',50000,1,'2020-01-01');

insert into thedataengineering_00.company.employees
values(2,'Jane','Doe',25,'Female',60000,2,'2020-01-01');

insert into thedataengineering_00.company.employees
values(1,'Bob','Johnson',40,'Male',70000,3,'2020-01-01');

-- COMMAND ----------

insert into thedataengineering_00.company.employees_test
values(1,'Bob','Johnson',40,'Male',70000,3,'2020-01-01');

-- COMMAND ----------


select * from thedataengineering_00.company.employees_test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Uniqueness - Guardrails Against Bad Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  Why This Matters for Data Engineers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC It's tempting to think creating tables is just boilerplate. But this step determines how trustworthy your data will be as it flows through the system. A pipeline is only as good as the quality of the data at its source.
-- MAGIC
-- MAGIC - Without a primary key, you risk duplicate or missing records.
-- MAGIC - Without uniqueness rules, you risk data that doesn't make sense (two employees sharing the same email).
-- MAGIC - Without foreign keys, you risk orphan records (employees pointing to departments that don't exist).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Suppose you forgot to give primary key what is the option?
-- MAGIC ## 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Closing Thought

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Designing tables is not just a technical ritual. It's the act of building discipline into your data. Primary keys give every record an identity. Uniqueness rules prevent mistakes. Foreign keys preserve relationships. Together, they ensure that your data - the lifeblood of any organization - is reliable.
