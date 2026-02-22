# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Data Types

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/table_defn.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Volumes/thedataengineering_00/data/sampledata/sampleimages/table_shot.png)

# COMMAND ----------

# MAGIC %md
# MAGIC When you start storing information in a database, it's not enough to just create a table with rows and columns. You also have to decide what kind of information each column will hold. This is where data types come in.
# MAGIC
# MAGIC Think of data types as rules that help the database understand how to treat each piece of information. Just like in real life, you wouldn't add an employee's salary and their joining date together - they're different things. By choosing the right data type, you tell the database:
# MAGIC
# MAGIC - "This column will always contain numbers."
# MAGIC - "This one will hold text."
# MAGIC - "That one will be a proper date."
# MAGIC
# MAGIC Without these rules, data becomes messy and unreliable - something no data engineer can afford.

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table thedataengineering_00.company.employees

# COMMAND ----------

# MAGIC %md
# MAGIC # Numbers: For Salaries and IDs

# COMMAND ----------

# MAGIC %md
# MAGIC In our employees table, emp_id and salary are numeric.
# MAGIC
# MAGIC emp_id is an integer because IDs are whole numbers - 101, 102, 103. You don't expect a half employee with ID 103.5.
# MAGIC
# MAGIC salary is better stored as a decimal because money can have fractions (like 65000.50). Storing salary as text would be a disaster - you wouldn't be able to calculate totals or averages correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC # Text: Names and Departments

# COMMAND ----------

# MAGIC %md
# MAGIC Columns like first_name, last_name, and dept_name are all text (VARCHAR).
# MAGIC
# MAGIC  Why not just use numbers? Because names are words, and words need flexibility.
# MAGIC
# MAGIC If you tried to store "Information Technology" as a number, you'd lose all meaning. But if you store it as text, it remains readable and searchable.

# COMMAND ----------

# MAGIC %md
# MAGIC # Dates: When People Joined

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The column hire_date is best stored as a DATE type. That way, you can ask the database:
# MAGIC
# MAGIC - Who joined before 2020?
# MAGIC - How many employees were hired this year?
# MAGIC
# MAGIC If you stored hire dates as plain text ("2020-01-15"), the database wouldn't understand how to compare them. But with a proper DATE type, you can do time-based calculations easily.

# COMMAND ----------

# MAGIC %md
# MAGIC # Why Choosing the Right Type Matters

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's take two simple examples.
# MAGIC
# MAGIC If you mistakenly store salaries as text, the database might think "90000" is smaller than "10000" because it compares letters, not numbers. That's obviously wrong - and it could mislead your analysis.
# MAGIC
# MAGIC If you store hire dates as text, finding employees who joined after 2019 becomes complicated. But with a DATE type, it's as simple as asking the database to compare dates.
# MAGIC
# MAGIC Good data types = clean logic + efficient queries.

# COMMAND ----------

# MAGIC %md
# MAGIC # The Bigger Picture for Data Engineers

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC For a data engineer, choosing the right type is about scalability and reliability. When your tables grow from hundreds of rows to millions, the database needs to store and process them efficiently. Data types guide how much space each column takes, how fast queries run, and whether future calculations make sense.
# MAGIC
# MAGIC It's like setting strong foundations before building a skyscraper - if your foundation is weak, the whole structure is at risk.

# COMMAND ----------

# MAGIC %md
# MAGIC Practical Experience In Migration of systems
# MAGIC -  Join of columns
# MAGIC -  Size of columns optimization
# MAGIC -  Slow Queries

# COMMAND ----------

# MAGIC %md
# MAGIC # Closing Thought

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC SQL data types may seem basic, but they're the guardrails that keep your data trustworthy. Whether it's an employee's salary, their name, or their hire date, storing it in the right format ensures that tomorrow's queries won't break and tomorrow's insights won't mislead.
