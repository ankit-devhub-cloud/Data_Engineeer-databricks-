-- Databricks notebook source
-- MAGIC %md
-- MAGIC Now that we’ve created our tables with primary keys, uniqueness, and foreign keys, it’s time to breathe life into them. Until now, the tables are like empty rooms in a newly built office — perfectly designed, but without people or activity. The moment you start inserting data, those rooms fill with employees, departments, and meaningful information.
-- MAGIC
-- MAGIC In SQL, the operation of adding new data into a table is done with the INSERT statement. This is one of the four fundamental CRUD operations: Create, Read, Update, and Delete. Today, we focus on the “C” — creating records.
-- MAGIC
-- MAGIC ### How INSERT Works
-- MAGIC Think of a table as a form with several fields: Employee ID, First Name, Last Name, Email, Hire Date, Salary, and Department ID. An INSERT statement is simply you filling out that form and handing it over to the database.
-- MAGIC
-- MAGIC For example, when HR hires a new employee, you want to record their details in the employees table.

-- COMMAND ----------


USE CATALOG thedataengineering_00;
USE SCHEMA company;


-- COMMAND ----------

show create table company.employees

-- COMMAND ----------

INSERT INTO employees (emp_id, first_name, last_name, age,gender, salary, dept_id,hire_date)
VALUES (101, 'Alice', 'Johnson',23, 'M',60000,1,'2020-01-15');

-- COMMAND ----------

select * from company.employees

-- COMMAND ----------

INSERT INTO employees (emp_id, first_name, last_name, age,gender, salary, dept_id,hire_date)
VALUES
(105, 'Alice', 'Johnson',23, 'M',60000,1,'2020-01-15'),
(106, 'Alice', 'Johnson',23, 'M',60000,1,'2020-01-15'),
(107, 'Alice', 'Johnson',23, 'M',60000,1,'2020-01-15'),
(109, 'Alice', 'Johnson',23, 'M',60000,1,'2020-01-15');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC With this command, Bob, Charlie, and Diana all enter the system together.
-- MAGIC
-- MAGIC ### Respecting Constraints
-- MAGIC Remember the rules we set while creating the table? The database enforces them during insert operations.
-- MAGIC
-- MAGIC ### Primary Key Rule:
-- MAGIC  If you try to insert another employee with emp_id = 101, the database will reject it. Two employees cannot share the same ID.
-- MAGIC
-- MAGIC ### Unique Constraint:
-- MAGIC  If you accidentally enter two employees with the same email, the database will block the insert. Each email must be unique.
-- MAGIC
-- MAGIC ### Foreign Key Rule:
-- MAGIC  If you try to assign a department that doesn’t exist — say dept_id = 99 — the database will refuse, because every employee must belong to a valid department.
-- MAGIC
-- MAGIC These constraints act like security guards, standing at the door of your table, checking every new entry before it comes in.
-- MAGIC
-- MAGIC ### Inserting into Departments First
-- MAGIC Since employees depend on departments (via the foreign key), you always need to insert department data first. Otherwise, employees won’t have a valid department to point to.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC At first glance, inserting rows might feel like the simplest part of SQL. But for a data engineer, it's the beginning of every pipeline. Data must first be captured and ingested before it can be transformed, cleaned, or analyzed. INSERT statements are like the entry gates to your data system.
-- MAGIC
-- MAGIC Getting inserts right - with the right constraints and validation - ensures that bad data never enters your system. And if bad data never enters, you spend far less time fixing downstream errors later.

-- COMMAND ----------

INSERT INTO departments (dept_id, dept_name, location) VALUES
(1, 'HR', 'New York'),
(2, 'Finance', 'Chicago'),
(3, 'IT', 'San Francisco'),
(4, 'Marketing', 'Boston');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### INSERT Without Specifying Columns
-- MAGIC You can also insert data without listing column names, but this requires providing values in the exact order the columns were defined. For beginners, this is risky and not recommended, because any change in table design can break your inserts. Still, here’s what it looks like:

-- COMMAND ----------

INSERT INTO employees 
VALUES (105, 'Alice', 'Johnson',23, 'M',60000,1,'2020-01-15');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Why INSERT Matters for Data Engineers
-- MAGIC At first glance, inserting rows might feel like the simplest part of SQL. But for a data engineer, it’s the beginning of every pipeline. Data must first be captured and ingested before it can be transformed, cleaned, or analyzed. INSERT statements are like the entry gates to your data system.
-- MAGIC
-- MAGIC Getting inserts right — with the right constraints and validation — ensures that bad data never enters your system. And if bad data never enters, you spend far less time fixing downstream errors later.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Closing Thought

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By mastering the INSERT operation, you've taken the first step toward actively working with your tables. Until now, our employees and departments tables were like empty blueprints. With INSERT, they've begun to fill with life: departments have been established, employees have joined, and relationships are taking shape.
-- MAGIC
-- MAGIC In the next lesson, we'll move from adding data to reading data. That's when you'll see the real power of SQL - turning raw rows into meaningful answers to questions.
