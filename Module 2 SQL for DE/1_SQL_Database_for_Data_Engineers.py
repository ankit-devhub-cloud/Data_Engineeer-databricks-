# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1: What is a Database?

# COMMAND ----------

# MAGIC %md
# MAGIC When students hear the word database for the first time, many of them imagine something mysterious, locked away in giant servers. But in reality, a database is just a smarter and more structured version of something you already know very well: an Excel sheet.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/1_excel.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Think of Excel. You have rows and columns. Each column has a name, such as Name, Age, or Salary. Each row contains the actual data, such as Alice, 30, 60,000. A database table works the same way. The difference is that instead of opening one file at a time on your laptop, databases are designed to store millions (or billions) of rows, keep them organized, and allow many people or applications to use them simultaneously without breaking anything.

# COMMAND ----------

# MAGIC %md
# MAGIC # From Excel to Databases

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Imagine your college placement cell keeps student information in an Excel file. It works fine for a few hundred students. But what happens when you join a company that manages data for millions of customers, or thousands of employees across different offices? Suddenly, Excel starts to break. Files get too large, multiple people can't edit at the same time, and searching becomes painfully slow.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/1_db.png)

# COMMAND ----------

# MAGIC %md
# MAGIC This is where databases come in. A database is a system that:
# MAGIC - Stores data reliably (no matter how big).
# MAGIC - Organizes it into tables (just like structured sheets).
# MAGIC - Provides tools to query and analyze the data efficiently.
# MAGIC - Ensures that multiple people can work with the data without conflict.

# COMMAND ----------

# MAGIC %md
# MAGIC # Introducing SQL

# COMMAND ----------

# MAGIC %md
# MAGIC So how do we talk to a database? That's where SQL (Structured Query Language) comes in.
# MAGIC SQL is like the language of databases. If the database is the library, then SQL is the librarian you talk to:
# MAGIC
# MAGIC "Can you give me a list of all employees in the IT department?"
# MAGIC
# MAGIC "Please update Bob's salary to 80,000."
# MAGIC
# MAGIC "Remove the record of the employee who left last month."
# MAGIC
# MAGIC SQL makes these requests possible. You don't need to know how the database engine actually stores the data internally - SQL abstracts that complexity and gives you a clear, English-like way to communicate.

# COMMAND ----------

# MAGIC %md
# MAGIC # Why SQL for Data Engineers?

# COMMAND ----------

# MAGIC %md
# MAGIC As a future data engineer, you will be the one building pipelines that move data from raw sources into structured, usable formats. You will need to clean messy logs, join information from different systems, and prepare it for analytics or machine learning.
# MAGIC
# MAGIC Almost all of this relies on SQL at some stage. Even if you later learn Spark, Python, or big data frameworks, you'll notice they all embed SQL concepts - grouping, joining, filtering, aggregating. In fact, many large companies still depend on SQL as their backbone for reporting and decision-making.
# MAGIC
# MAGIC SQL is to data what mathematics is to engineering - a foundation you cannot skip.
# MAGIC
# MAGIC Let's take our teaching schema with two simple tables:

# COMMAND ----------

# MAGIC %md
# MAGIC Department Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from thedataengineering_00.company.department

# COMMAND ----------

# MAGIC %md
# MAGIC Employees Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from thedataengineering_00.company.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT first_name, last_name, salary
# MAGIC FROM thedataengineering_00.company.employees
# MAGIC WHERE dept_id = 3;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Today, we just built the bridge between something you know (Excel) and something you will master (databases with SQL). From here on, we'll dive deeper - into data types, creating tables, and actually shaping the data we store.
