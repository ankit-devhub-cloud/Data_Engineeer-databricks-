-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Aggregations & GROUP BY

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Turning Data into Insights
-- MAGIC Up to this point, we’ve been working with individual records.
-- MAGIC  We inserted employees, filtered them, updated their salaries, and even removed duplicates. But in the real world, your job as a data engineer is not just to store data — it’s to help people understand it.
-- MAGIC
-- MAGIC No manager wants to see a thousand individual employee rows; they want to know things like — 
-- MAGIC
-- MAGIC - What’s the average salary per department?
-- MAGIC - Which team has the most people?
-- MAGIC - How much is the company spending on payroll?
-- MAGIC
-- MAGIC These questions don’t deal with single rows — they deal with groups of data.
-- MAGIC  And this is where aggregations and GROUP BY come in.
-- MAGIC
-- MAGIC ### Seeing the Bigger Picture
-- MAGIC When you look at your employees table, you see individuals — each with their own salary, department, and hire date.
-- MAGIC But when you group these employees by dept_id, suddenly the focus shifts from people to departments. Each group now represents one department, and within that group, you can perform calculations — count how many employees it has, sum their salaries, or compute the average pay.
-- MAGIC
-- MAGIC This is what aggregation really means: rolling up detailed data to see the bigger picture.
-- MAGIC
-- MAGIC ### How GROUP BY Works
-- MAGIC Let’s start simple. Suppose you want to know how many employees work in each department.
-- MAGIC  You’d tell SQL: “Group all employees by department, and then count how many there are in each group.”

-- COMMAND ----------

SELECT dept_id, COUNT(*) AS employee_count
FROM employees
GROUP BY dept_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Behind the scenes, Spark SQL looks at all rows with the same dept_id, puts them in one bucket, and applies the function COUNT(*) to count them.
-- MAGIC  What you get is not individual records anymore — but a summary, one row per department.
-- MAGIC
-- MAGIC That’s the mental shift you need when thinking about GROUP BY:
-- MAGIC  You no longer deal with individual rows. You deal with groups of rows, and aggregate functions summarize them.
-- MAGIC
-- MAGIC ### From Counting to Measuring
-- MAGIC Counting employees is just the beginning. Once you start grouping, you can calculate almost anything — totals, averages, minimums, maximums.
-- MAGIC
-- MAGIC Let’s say you want to find out how much each department spends on salaries.

-- COMMAND ----------

SELECT dept_id, SUM(salary) AS total_salary
FROM employees
GROUP BY dept_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, instead of six employee rows, you might get just four — one per department — showing how the total salary differs across them.
-- MAGIC  With just one more function, you’ve turned raw HR data into something management can act on.
-- MAGIC
-- MAGIC Or maybe your HR partner wants to see the average salary for each department, rounded neatly to two decimal places:

-- COMMAND ----------

SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY dept_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This gives a more human-friendly view: IT averages 72,500, Finance 73,500, Marketing 45,000.
-- MAGIC
-- MAGIC With every aggregate, you’re shaping the data into meaning.
-- MAGIC
-- MAGIC ### Adding Context with Joins
-- MAGIC Numbers alone can sometimes feel lifeless — what does dept_id = 3 even mean?
-- MAGIC  This is where your earlier understanding of joins becomes valuable. You can bring the departments table into the picture to make the output readable.

-- COMMAND ----------

SELECT d.dept_name,
       COUNT(e.emp_id) AS employee_count,
       ROUND(AVG(e.salary), 2) AS avg_salary
FROM employees e
JOIN departments d
  ON e.dept_id = d.dept_id
GROUP BY d.dept_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now the query speaks the language of business — “Finance,” “IT,” “Marketing” — instead of just numbers.
-- MAGIC  You’ve bridged the technical world of tables with the human world of insights.
-- MAGIC
-- MAGIC ### Filtering Groups — The HAVING Clause
-- MAGIC Once you start aggregating, a natural question follows:
-- MAGIC  What if I want to see only the departments where the average salary is above a certain threshold?
-- MAGIC
-- MAGIC You might think of using WHERE, but WHERE filters rows before grouping happens.
-- MAGIC  We now need to filter after grouping — once aggregates are calculated.
-- MAGIC  That’s where the HAVING clause comes in.

-- COMMAND ----------

SELECT d.dept_name, ROUND(AVG(e.salary), 2) AS avg_salary
FROM employees e
JOIN departments d
  ON e.dept_id = d.dept_id
GROUP BY d.dept_name
HAVING AVG(e.salary) > 70000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC HAVING is like saying: “Now that we’ve grouped and calculated, show me only those groups where the average salary exceeds 70,000.”
-- MAGIC
-- MAGIC In this query, departments like IT and Finance make the cut, while others quietly drop off the result set.
-- MAGIC
-- MAGIC ### Thinking Like a Data Engineer
-- MAGIC For a data engineer, GROUP BY is not just about writing reports — it’s about transforming data at scale.
-- MAGIC  When Spark SQL runs a GROUP BY across millions of rows, it distributes the computation across the cluster.
-- MAGIC  Each worker node groups and aggregates its share of data, and Spark then combines all those partial results into the final output.
-- MAGIC
-- MAGIC That’s why the same simple syntax you write for 10 rows also works for 10 billion rows.
-- MAGIC  GROUP BY is one of the core building blocks of large-scale data processing.
-- MAGIC
-- MAGIC ### Beyond Numbers
-- MAGIC At its heart, aggregation is about summarizing stories.
-- MAGIC  Each row in your table is a small story — one employee, one department, one transaction. GROUP BY collects all those stories, finds what they have in common, and turns them into something bigger: totals, averages, counts, trends.
-- MAGIC
-- MAGIC And for a data engineer, that’s the difference between data and insight.
-- MAGIC
-- MAGIC # Closing Thought
-- MAGIC When you master GROUP BY, you begin to see data differently.
-- MAGIC  Instead of endless rows, you start to see patterns — the departments growing fastest, the ones paying more, the ones shrinking quietly.
-- MAGIC
-- MAGIC SQL’s aggregate functions are the simplest form of analytics — but also the most universal.
-- MAGIC  They remind us that at the heart of every data system, there’s always one fundamental question:
-- MAGIC  “What does all this data mean?”
-- MAGIC
-- MAGIC In the next session, we’ll take this idea further — learning how to go beyond static summaries and calculate dynamic insights, like rankings, running totals, and comparisons.
-- MAGIC  That’s the world of Window Functions, and it’s where SQL begins to feel truly analytical.
