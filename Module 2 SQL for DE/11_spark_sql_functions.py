# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Functions - String Operations (Concatenation, Trimming, etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenation - Joining Strings Together

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name || ' ' || last_name AS full_name
# MAGIC FROM thedataengineering_00.company.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT TRIM(first_name) AS clean_first_name
# MAGIC FROM thedataengineering_00.company.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC # UPPER and LOWER - Consistency in Case

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UPPER(dept_name) AS department
# MAGIC FROM thedataengineering_00.company.department;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LOWER(dept_name) AS normalized_email
# MAGIC FROM thedataengineering_00.company.department;

# COMMAND ----------

# MAGIC %md
# MAGIC ## LENGTH - Measuring Text

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, LENGTH(first_name) AS name_length
# MAGIC FROM thedataengineering_00.company.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SUBSTRING - Extracting Parts of Text

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUBSTRING(first_name, 1, 4) AS first_five_chars
# MAGIC FROM thedataengineering_00.company.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT UPPER(CONCAT(TRIM(first_name), ' ', TRIM(last_name))) AS clean_full_name
# MAGIC FROM thedataengineering_00.company.employees;
