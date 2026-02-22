# Databricks notebook source
# MAGIC %md
# MAGIC # What is SQL and Why Every Data Engineer must learn it?
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC On Day 1, we compared databases to Excel sheets and saw how data is stored in tables. But knowing that data lives in tables is just the start. The real question is: how do we talk to it?

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL - The Language of Data

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/1_sql.png)

# COMMAND ----------

# MAGIC %md
# MAGIC SQL, short for Structured Query Language, is the standard language used to communicate with databases. It's not about writing long step-by-step instructions like you would in a programming language. Instead, you simply tell the database what you want, and it figures out how to get it for you.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - For example, imagine your HR team asks:
# MAGIC - "Who are the employees in the Finance department?"
# MAGIC - "Which employees joined this year?"
# MAGIC - "What's the highest salary in IT?"

# COMMAND ----------

# MAGIC %md
# MAGIC # Why SQL Matters for Data Engineers

# COMMAND ----------

# MAGIC %md
# MAGIC As a data engineer, your job revolves around making raw data usable. You'll design pipelines, clean messy datasets, and prepare information so analysts and data scientists can work with it. At every step, SQL shows up.

# COMMAND ----------

# MAGIC %md
# MAGIC - A. When creating new tables, you use SQL to define the structure.
# MAGIC - B. When transforming data, you use SQL to filter, group, and join.
# MAGIC - C. When validating pipelines, you run SQL queries to check results.

# COMMAND ----------

# MAGIC %md
# MAGIC Even advanced platforms like Spark, Databricks, Snowflake, or BigQuery build their engines around SQL. You might later write Python or Scala, but underneath, the logic still feels like SQL - select, join, aggregate. That's why companies expect data engineers to think in SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC # The Power of Simplicity

# COMMAND ----------

# MAGIC %md
# MAGIC Picture yourself scrolling through an Excel file with thousands of rows. If you want to find all employees earning above 70,000, you'd spend minutes filtering and clicking around. With SQL, you ask that in one line and get the answer instantly - whether the table has 1,000 rows or 100 million.
# MAGIC That's the beauty of SQL: simplicity at any scale.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from thedataengineering_00.company.employees
# MAGIC where salary > 70000

# COMMAND ----------

# MAGIC %md
# MAGIC # More Than Just Queries

# COMMAND ----------

# MAGIC %md
# MAGIC It's also worth noting that SQL is not limited to "asking questions." It lets you define rules, insert new data, update existing information, and even control who has access. It manages the full lifecycle of structured data.
# MAGIC So when someone says "SQL is just about querying," that's only half the story. For a data engineer, SQL is about building trust in data.

# COMMAND ----------

# MAGIC %md
# MAGIC # Closing Thought

# COMMAND ----------

# MAGIC %md
# MAGIC If data are the backbone of the modern data world, SQL is the language that keeps them alive. It's how you describe data, how you shape it, and how you deliver it to others.
# MAGIC For a fresher entering data engineering, SQL is not just another skill on the resume. It's the first real tool you'll wield - and once you get comfortable with it, you'll see the entire world of data differently.
