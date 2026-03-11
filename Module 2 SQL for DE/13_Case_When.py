# Databricks notebook source
# MAGIC %md
# MAGIC # CASE WHEN & Conditional Logic in Spark SQL
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC So far, we’ve learned how to store, insert, update, delete, and filter data. But real-world data often demands more than just filtering — it requires logic.
# MAGIC
# MAGIC For example, You may want to group employees into “High Salary” and “Low Salary” buckets. You may want to flag employees hired before 2020 as “Senior” and after 2020 as “Junior.”
# MAGIC
# MAGIC In programming languages, we’d use if-else statements. In SQL, the equivalent is CASE WHEN.
# MAGIC
# MAGIC ### What is CASE WHEN?
# MAGIC
# MAGIC The CASE WHEN expression allows you to apply conditional logic inside a query. You tell the database: if this condition is true, return this value; otherwise, return something else.
# MAGIC
# MAGIC It works seamlessly inside a SELECT statement, making it incredibly useful for data transformations.
# MAGIC
# MAGIC Example 1: Categorizing Salaries
# MAGIC Let’s divide employees into salary bands:

# COMMAND ----------

SELECT emp_id, first_name, last_name, salary,
       CASE 
           WHEN salary >= 80000 THEN 'High Salary'
           WHEN salary BETWEEN 50000 AND 79999 THEN 'Medium Salary'
           ELSE 'Low Salary'
       END AS salary_band
FROM thedataengineering_00.company.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC - Salaries ≥ 80,000 → “High Salary”
# MAGIC - Between 50,000 and 79,999 → “Medium Salary”
# MAGIC - Below 50,000 → “Low Salary”
# MAGIC
# MAGIC This instantly adds a new derived column, salary_band, to your results.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Senior vs Junior Employees
# MAGIC
# MAGIC Suppose HR wants to know who joined before 2020 (senior) vs after (junior):

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, last_name, hire_date,
# MAGIC        CASE 
# MAGIC            WHEN year(hire_date) < 2020 THEN 'Senior'
# MAGIC            ELSE 'Junior'
# MAGIC        END AS employee_level
# MAGIC FROM thedataengineering_00.company.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Department Names Instead of IDs
# MAGIC
# MAGIC Remember that in our employees table, we store only dept_id. Sometimes we want to display friendly department names without a join. CASE WHEN can help (though joins are usually better in production).

# COMMAND ----------


SELECT first_name, last_name, dept_id,
       CASE 
           WHEN dept_id = 1 THEN 'HR'
           WHEN dept_id = 2 THEN 'Finance'
           WHEN dept_id = 3 THEN 'IT'
           WHEN dept_id = 4 THEN 'Marketing'
           ELSE 'Unknown'
       END AS department_name
FROM thedataengineering_00.company.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4: Flagging Special Conditions
# MAGIC
# MAGIC Maybe we want to quickly identify employees with missing email addresses:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     first_name, 
# MAGIC     last_name,
# MAGIC     CASE 
# MAGIC         WHEN last_name IS NULL THEN 'Missing Last Name'
# MAGIC         ELSE 'Valid Last Name'
# MAGIC     END AS email_status
# MAGIC FROM thedataengineering_00.company.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why CASE WHEN Matters for Data Engineers
# MAGIC
# MAGIC For a data engineer, CASE WHEN is not just about pretty labels — it’s about transforming raw data into business-friendly insights.
# MAGIC
# MAGIC It allows you to derive new columns without altering tables.
# MAGIC
# MAGIC It adds categorization and flags directly in SQL, reducing the need for downstream logic.
# MAGIC
# MAGIC It’s often used in data quality checks (e.g., missing values, invalid ranges).
# MAGIC
# MAGIC It’s critical for reporting pipelines, where business teams expect clean categories like “Gold Customer,” “Silver Customer,” “Bronze Customer.”
# MAGIC
# MAGIC In Spark SQL, CASE WHEN scales effortlessly — whether you’re working with 10 rows or 10 billion.

# COMMAND ----------

# MAGIC %md
# MAGIC # Closing Thought
# MAGIC
# MAGIC With CASE WHEN, SQL moves beyond just storing and querying — it starts thinking with you. It lets you embed business logic directly into queries, shaping results into categories and conditions that make sense for decision-making.
# MAGIC
# MAGIC As a data engineer, this is where your queries start reflecting real-world business rules, not just raw data.
# MAGIC
# MAGIC In the next lesson, we’ll tackle one of the most important SQL skills: subqueries — queries inside queries — which give you the ability to answer complex questions step by step.
