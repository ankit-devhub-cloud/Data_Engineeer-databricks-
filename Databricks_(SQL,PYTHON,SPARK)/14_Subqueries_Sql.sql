-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Subqueries in Spark
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sometimes, one query isn’t enough. You ask one question, get an answer, and then realize you need to use that answer as part of a bigger question. Instead of running multiple queries manually, SQL allows you to nest one query inside another. This is called a subquery.
-- MAGIC
-- MAGIC Think of it like asking:
-- MAGIC
-- MAGIC “Who earns more than the average salary?”
-- MAGIC  First, you need the average salary → one query.
-- MAGIC  Then, you compare each employee’s salary to that average → another query.
-- MAGIC  A subquery lets you combine both into one.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  What is a Subquery?
-- MAGIC A subquery is simply a query inside parentheses, used inside another query. The outer query depends on the result of the inner query.
-- MAGIC
-- MAGIC Spark SQL supports subqueries in:
-- MAGIC
-- MAGIC - WHERE clause (to filter using another query).
-- MAGIC - FROM clause (to treat the subquery result as a table).
-- MAGIC - SELECT clause (to return calculated values).
-- MAGIC
-- MAGIC Example 1: Employees Earning Above Average
-- MAGIC Step 1: Find the average salary.

-- COMMAND ----------

SELECT AVG(salary) FROM thedataengineering_00.company.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2: Use that in another query to filter employees.

-- COMMAND ----------

SELECT first_name, last_name, salary
FROM thedataengineering_00.company.employees
WHERE salary > (SELECT AVG(salary) FROM thedataengineering_00.company.employees);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This subquery runs once, gets the average salary, and then the outer query finds all employees above it.
-- MAGIC
-- MAGIC ### Example 2: Employees in the Same Department as Bob
-- MAGIC
-- MAGIC Let’s say we want to find all employees who belong to Bob’s department.

-- COMMAND ----------

SELECT first_name, last_name
FROM thedataengineering_00.company.employees
WHERE dept_id = (
    SELECT dept_id FROM thedataengineering_00.company.employees WHERE first_name = 'Bob'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The inner query finds Bob’s department ID, and the outer query retrieves all employees in that department.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Example 3: Using Subquery in FROM (Derived Table)
-- MAGIC
-- MAGIC Sometimes you want to create a temporary result set first, then query it.

-- COMMAND ----------

SELECT dept_id, avg_salary
FROM (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM thedataengineering_00.company.employees
    GROUP BY dept_id
) dept_avg
WHERE avg_salary > 70000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here, the subquery calculates average salaries per department, and the outer query filters only those above 70,000.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Limitations of Subqueries in Spark SQL
-- MAGIC Subqueries are powerful, but they have limitations — especially in Spark SQL, where performance on big data matters:
-- MAGIC
-- MAGIC ### Performance Overhead
-- MAGIC Every subquery may trigger an extra computation. On large datasets, this can be slower than equivalent joins.
-- MAGIC
-- MAGIC Example: Getting department names with a subquery for each row is much slower than a single join.
-- MAGIC
-- MAGIC ### Readability
-- MAGIC Complex nested subqueries are hard to read and maintain. For pipelines, clarity matters more than “clever SQL.”
-- MAGIC
-- MAGIC ### Optimizer Behavior
-- MAGIC Spark’s Catalyst optimizer sometimes rewrites subqueries into joins internally. So while subqueries work, they aren’t always the most efficient.
-- MAGIC
-- MAGIC ### Not Always Flexible
-- MAGIC Spark SQL does not support correlated subqueries in every place. For example, using correlated subqueries inside SELECT or HAVING may fail, forcing you to rewrite as a join.
-- MAGIC
-- MAGIC ### Why Subqueries Matter for Data Engineers
-- MAGIC For freshers, subqueries feel like magic — queries inside queries. They let you break down complex problems into smaller pieces and solve them step by step.
-- MAGIC
-- MAGIC But for data engineers, the real lesson is knowing when to use them and when not to.
-- MAGIC
-- MAGIC - Use subqueries for clarity in learning or when the dataset is small.
-- MAGIC - Prefer joins or CTEs (Common Table Expressions) for performance and readability in production pipelines.
-- MAGIC
-- MAGIC # Closing Thought
-- MAGIC Subqueries are like thinking in layers — answer a small question first, then use it to answer the bigger one. They give SQL its expressive power and flexibility. But in Spark SQL, you also need to balance that flexibility with performance.
-- MAGIC
-- MAGIC As you grow as a data engineer, you’ll learn to use subqueries where they make sense, and rewrite them as joins or CTEs when speed and scale demand it. In our next lesson, we’ll move to one of the most essential SQL skills for data engineers: Joins — the ability to combine data across multiple tables. This is where our employees and departments tables will truly come together.
