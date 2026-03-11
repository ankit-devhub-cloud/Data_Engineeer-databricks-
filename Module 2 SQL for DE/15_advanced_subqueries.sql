-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###  EXISTS, IN, and Correlated Subqueries
-- MAGIC Subqueries open the door to writing more expressive SQL. On Day 14, we saw how to place a query inside another to answer questions like “who earns above the average?”. But SQL offers more advanced ways to use subqueries — powerful tools like EXISTS, IN, and correlated subqueries.
-- MAGIC
-- MAGIC For a data engineer, these techniques matter because they allow you to handle complex filters, validations, and business rules without breaking queries into multiple steps.
-- MAGIC
-- MAGIC ### The IN Subquery
-- MAGIC The IN operator checks whether a value matches any value returned by a subquery.
-- MAGIC
-- MAGIC Example 1: Employees in departments located in Chicago

-- COMMAND ----------

SELECT first_name, last_name
FROM thedataengineering_00.company.employees
WHERE dept_id IN (
    SELECT dept_id
    FROM thedataengineering_00.company.department
    WHERE location = 'Chicago'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here, the inner query finds all department IDs in Chicago. The outer query then selects employees whose department matches one of those IDs.
-- MAGIC
-- MAGIC ### The EXISTS Subquery
-- MAGIC While IN checks for matching values, EXISTS checks for the existence of rows in the subquery. It returns TRUE if the subquery returns at least one row.
-- MAGIC
-- MAGIC ### Example 2: Departments that have at least one employee

-- COMMAND ----------

SELECT dept_name
FROM departments d
WHERE EXISTS (
    SELECT 1
    FROM employees e
    WHERE e.dept_id = d.dept_id
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here, we’re asking: “Does this department have employees?” If yes, it gets included. If no, it doesn’t.
-- MAGIC
-- MAGIC EXISTS is often more efficient than IN when dealing with large data, because the database can stop checking as soon as it finds one match.
-- MAGIC
-- MAGIC ### Correlated Subqueries
-- MAGIC A correlated subquery is a special case: the inner query depends on values from the outer query. This means the subquery runs once for every row in the outer query.
-- MAGIC
-- MAGIC ### Example 3: Employees who earn above the average salary of their department

-- COMMAND ----------

SELECT first_name, last_name, salary, dept_id
FROM employees e
WHERE salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE dept_id = e.dept_id
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Notice that the inner query refers to e.dept_id from the outer query. This makes it correlated.
-- MAGIC
-- MAGIC For each employee, the subquery calculates the average salary of their department, and then compares their salary to that average.
-- MAGIC
-- MAGIC ### Limitations of Advanced Subqueries in Spark SQL
-- MAGIC While these are powerful, Spark SQL comes with some limitations you should be aware of:
-- MAGIC
-- MAGIC ### Performance on Large Data
-- MAGIC
-- MAGIC Correlated subqueries can be expensive since the inner query may execute for every row in the outer query. Spark’s optimizer (Catalyst) sometimes rewrites them into joins, but not always efficiently.
-- MAGIC
-- MAGIC **Readability**
-- MAGIC
-- MAGIC Nested EXISTS and correlated subqueries can be hard to understand, especially for beginners. In production, Common Table Expressions (CTEs) or joins are often preferred for clarity.
-- MAGIC
-- MAGIC Not **All** Contexts Supported
-- MAGIC
-- MAGIC Spark SQL does not fully support correlated subqueries in every clause (e.g., inside SELECT lists or HAVING clauses). You may need to rewrite them as joins or CTEs.
-- MAGIC
-- MAGIC ### Why Advanced Subqueries Matter for Data Engineers
-- MAGIC For a data engineer, advanced subqueries are a way to:
-- MAGIC
-- MAGIC - Check existence of relationships (EXISTS).
-- MAGIC - Filter based on multiple matching values (IN).
-- MAGIC - Compare row-level values to aggregates in their group (correlated).
-- MAGIC
-- MAGIC These patterns are often used in data validation, anomaly detection, and pipeline quality checks. For example:
-- MAGIC
-- MAGIC - Finding employees without a valid department.
-- MAGIC - Checking if a department has at least one active employee.
-- MAGIC - Flagging outliers who earn significantly above peers in their department.
-- MAGIC
-- MAGIC # Closing Thought
-- MAGIC Advanced subqueries extend the power of SQL beyond simple queries. With IN, you can filter by lists. With EXISTS, you can validate relationships. With correlated subqueries, you can compare a row against its peers.
-- MAGIC
-- MAGIC For data engineers, the challenge is not just knowing how to write them, but also when to rewrite them into joins or CTEs for better performance.
-- MAGIC
-- MAGIC In our next lesson, we’ll shift gears into one of the most important SQL concepts for combining data across tables: Joins. This is where our employees and departments tables will truly come together.
