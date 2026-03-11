-- Databricks notebook source
-- MAGIC %md
-- MAGIC customers(customer_id, customer_name, customer_city)
-- MAGIC
-- MAGIC orders(order_id, customer_id, order_date, total_amount)
-- MAGIC
-- MAGIC order_items(order_item_id, order_id, product_name, quantity, price_per_unit)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DDL – Data Definition Language

-- COMMAND ----------

USE Catalog thebricklearning;
USE Schema aianalyticsengineer;

-- COMMAND ----------


CREATE TABLE customers (
  customer_id INT,
  customer_name STRING,
  customer_city STRING
);

CREATE TABLE orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  total_amount DOUBLE
);

CREATE TABLE order_items (
  order_item_id INT,
  order_id INT,
  product_name STRING,
  quantity INT,
  price_per_unit DOUBLE
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Alter Table and Add Column

-- COMMAND ----------

ALTER TABLE customers ADD COLUMNS (customer_email STRING);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop table if exists.

-- COMMAND ----------

DROP TABLE IF EXISTS old_transactions;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  DML – Data Manipulation Language

-- COMMAND ----------

select * from customers

-- COMMAND ----------

INSERT INTO customers VALUES
(1, 'Alice', 'New York', 'alice@example.com'),
(2, 'Bob', 'Chicago','john@example.com'),
(3, 'Charlie', 'Boston','nathan@example.com'),
(4, 'Diana', 'Seattle','smith@example.com');


-- COMMAND ----------

INSERT INTO orders (order_id, customer_id, order_date, total_amount) VALUES
(101, 1, '2025-08-01', 250.00),
(102, 2, '2025-08-02', 120.00),
(103, 3, '2025-08-03', 300.00),
(104, 6, '2025-08-04', 400.00); -- Non-existent customer (to test RIGHT and FULL OUTER JOIN)


-- COMMAND ----------

INSERT INTO order_items (order_item_id, order_id, product_name, quantity, price_per_unit) VALUES
(1001, 101, 'Laptop', 1, 250.00),
(1002, 102, 'Mouse', 2, 30.00),
(1003, 103, 'Monitor', 2, 150.00),
(1004, 103, 'HDMI Cable', 1, 20.00),
(1005, 105, 'Keyboard', 1, 60.00); -- Order ID does not exist in orders


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update Data

-- COMMAND ----------

UPDATE customers 
SET customer_city = 'San Francisco' 
WHERE customer_id = 2;


-- COMMAND ----------

select * from customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  Joins – All 5 Types
-- MAGIC ###  INNER JOIN - Show me the order ID, customer name, and total amount for all orders that have a matching customer in the customers table.

-- COMMAND ----------

SELECT o.order_id, c.customer_name, o.total_amount
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  LEFT JOIN - Give me a list of all customers and the orders they have placed — and also include customers who haven’t placed any orders

-- COMMAND ----------

SELECT c.customer_name, o.order_id
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Show all customers and their orders — and if a customer hasn’t placed any order, still show their name.

-- COMMAND ----------

SELECT o.order_id, o.customer_id, c.customer_name
FROM orders o
RIGHT JOIN customers c ON o.customer_id = c.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Full Outer Join - Give me a list of all customers and all orders, whether or not they are matched.
-- MAGIC ### Show customer details with order info if available, and also include:
-- MAGIC
-- MAGIC - customers who haven’t placed any orders, and
-- MAGIC - orders that don’t have a matching customer.

-- COMMAND ----------

SELECT c.customer_id, c.customer_name, o.order_id, o.total_amount
FROM customers c
FULL OUTER JOIN orders o ON c.customer_id = o.customer_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cross Join
-- MAGIC
-- MAGIC Show every possible combination of customers and products from the order items table — whether or not the customer actually ordered that product

-- COMMAND ----------

SELECT c.customer_name, i.product_name
FROM customers c
CROSS JOIN order_items i;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Conditions – WHERE Clause
-- MAGIC Simple Filter

-- COMMAND ----------

SELECT * FROM orders WHERE total_amount > 150;


-- COMMAND ----------

SELECT * FROM customers 
WHERE customer_city = 'San Francisco' AND customer_id < 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Arithmatic Operator

-- COMMAND ----------

SELECT 
  product_name,
  quantity,
  price_per_unit,
  quantity * price_per_unit AS total_price
FROM order_items;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Discount Logic

-- COMMAND ----------

SELECT 
  order_id,
  total_amount,
  total_amount * 0.9 AS discounted_total
FROM orders;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  CTE – Common Table Expressions
-- MAGIC ###  CTE for Customer Spend

-- COMMAND ----------

WITH customer_spend AS (
  SELECT customer_id, SUM(total_amount) AS total_spent
  FROM orders
  GROUP BY customer_id
)
SELECT c.customer_name, s.total_spent
FROM customer_spend s
JOIN customers c ON s.customer_id = c.customer_id
WHERE total_spent > 150;


-- COMMAND ----------

WITH order_detail AS (
  SELECT 
    o.order_id, 
    o.customer_id,
    i.product_name,
    i.quantity,
    i.price_per_unit
  FROM orders o
  JOIN order_items i ON o.order_id = i.order_id
)
SELECT c.customer_name, od.*
FROM order_detail od
JOIN customers c ON od.customer_id = c.customer_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CASE Statement in SQL – Examples with Explanations
-- MAGIC
-- MAGIC We’ll use the same tables:
-- MAGIC
-- MAGIC - customers
-- MAGIC - orders
-- MAGIC - order_items
-- MAGIC

-- COMMAND ----------

SELECT 
  order_id,
  total_amount,
  CASE 
    WHEN total_amount >= 300 THEN 'High Value'
    WHEN total_amount BETWEEN 150 AND 299 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS order_category
FROM orders;


-- COMMAND ----------

SELECT 
  customer_id,
  customer_name,
  customer_city,
  CASE 
    WHEN customer_city IN ('New York', 'Boston') THEN 'East'
    WHEN customer_city IN ('Chicago', 'Austin') THEN 'Central'
    ELSE 'West'
  END AS region
FROM customers;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CASE inside Aggregation – Count High Value Orders

-- COMMAND ----------

SELECT 
  customer_id,
  COUNT(*) AS total_orders,
  COUNT(CASE WHEN total_amount >= 250 THEN 1 END) AS high_value_orders
FROM orders
GROUP BY customer_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CASE with JOIN – Tag Product Type by Name

-- COMMAND ----------

SELECT 
  o.order_id,
  oi.product_name,
  oi.quantity,
  CASE 
    WHEN LOWER(product_name) LIKE '%laptop%' THEN 'Electronics'
    WHEN LOWER(product_name) LIKE '%mouse%' THEN 'Accessories'
    WHEN LOWER(product_name) LIKE '%monitor%' THEN 'Display'
    ELSE 'Other'
  END AS product_type
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CASE for Null Handling

-- COMMAND ----------

SELECT 
  customer_id,
  customer_name,
  COALESCE(customer_email, 'Email Not Available') AS email_info
FROM customers;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Alternative with case

-- COMMAND ----------

SELECT 
  customer_id,
  customer_name,
  CASE 
    WHEN customer_email IS NULL THEN 'Email Not Available'
    ELSE customer_email
  END AS email_info
FROM customers;
