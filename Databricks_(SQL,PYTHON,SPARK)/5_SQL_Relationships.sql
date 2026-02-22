-- Databricks notebook source
-- MAGIC %md
-- MAGIC we built our first database and defined tables with primary keys and uniqueness rules. That gave each row a clear identity. But in the real world, data never lives in isolation. An employee belongs to a department. A department may manage multiple employees. These kinds of relationships are at the heart of database design.
-- MAGIC
-- MAGIC If primary keys are about identity, then foreign keys are about connection. They ensure that one table can reliably point to another, and together they maintain data integrity.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Why Relationships Matter

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Imagine we have two tables: departments and employees.
-- MAGIC
-- MAGIC - The departments table lists all valid departments in the company.
-- MAGIC - The employees table lists individual employees.
-- MAGIC
-- MAGIC Now, if we store department information directly in the employees table as plain text — say “HR” or “Finance” — we create problems:
-- MAGIC
-- MAGIC - What if one row says “HR” and another says “Hr” or “Human Resources”? 
-- MAGIC - What if someone types “Financee” by mistake? 
-- MAGIC - What if a department is renamed later?
-- MAGIC
-- MAGIC Suddenly, the data becomes inconsistent. Queries like “how many employees are in Finance” start producing unreliable results.
-- MAGIC
-- MAGIC The better way is to store only a reference in the employees table (like dept_id), and let the database enforce that every employee’s department actually exists in the departments table. This reference is the foreign key.

-- COMMAND ----------


select * from thedataengineering_00.company.employees

-- COMMAND ----------


select * from thedataengineering_00.company.department

-- COMMAND ----------


use catalog thedataengineering_00;
create schema relationships;

-- COMMAND ----------


drop table if exists thedataengineering_00.relationships.employees;

CREATE TABLE If not exists thedataengineering_00.relationships.employees (
    emp_id      INT PRIMARY KEY,
    first_name  VARCHAR(50) NOT NULL,
    last_name   VARCHAR(50) NOT NULL,
    email       VARCHAR(100) NOT NULL,
    hire_date   DATE NOT NULL,
    salary      DECIMAL(10,2) NOT NULL,
    dept_id     INT NOT NULL,
    CONSTRAINT fk_employees_dept FOREIGN KEY (dept_id) REFERENCES thedataengineering_00.relationships.departments(dept_id));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert into Employees Table

-- COMMAND ----------

INSERT INTO thedataengineering_00.relationships.employees (emp_id, first_name, last_name, email, hire_date, salary, dept_id) VALUES
(101, 'Alice', 'Johnson', 'alice.johnson@company.com', '2020-01-15', 60000, 1),
(102, 'Bob', 'Smith', 'bob.smith@company.com', '2019-03-10', 75000, 2);

-- COMMAND ----------

INSERT INTO thedataengineering_00.relationships.employees (emp_id, first_name, last_name, email, hire_date, salary, dept_id) VALUES
(103, 'Alice1', 'Johnson1', 'alice.johnson@company.com', '2020-01-15', 60000, 6),
(104, 'Bob2', 'Smith2', 'bob.smith@company.com', '2019-03-10', 75000, 9);

-- COMMAND ----------

drop table thedataengineering_00.relationships.departments;

CREATE TABLE if not exists thedataengineering_00.relationships.departments (
    dept_id     INT PRIMARY KEY,
    dept_name   VARCHAR(100) NOT NULL,
    location    VARCHAR(100)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert into Departments Table

-- COMMAND ----------

INSERT INTO thedataengineering_00.relationships.departments (dept_id, dept_name, location) 
VALUES
(1, 'HR', 'New York'),
(2, 'Finance', 'Chicago'),
(3, 'IT', 'San Francisco');

-- COMMAND ----------


select * from thedataengineering_00.relationships.departments

-- COMMAND ----------


select * from thedataengineering_00.relationships.employees
