-- FOOD DELIVERY ASSIGNMENT 3
-- Grafana Dashboard SQL Queries
-- Database: stockdb
-- Table: order_events


-- 1. Total Orders
SELECT
    COUNT(DISTINCT order_id) AS total_orders
FROM order_events;


-- 2. Orders by Cuisine
SELECT
    cuisine,
    COUNT(DISTINCT order_id) AS total_orders
FROM order_events
GROUP BY cuisine
ORDER BY total_orders DESC;


-- 3. Orders by City
SELECT
    city,
    COUNT(DISTINCT order_id) AS orders
FROM order_events
GROUP BY city
ORDER BY orders DESC;


-- 4. Total Order Value by Payment Mode
SELECT
    payment_mode,
    SUM(order_value) AS total_order_value
FROM order_events
GROUP BY payment_mode
ORDER BY total_order_value DESC;


-- 5. Average Order Value by Cuisine
SELECT
    cuisine,
    AVG(order_value) AS average_order_value
FROM order_events
GROUP BY cuisine
ORDER BY average_order_value DESC;


-- 6. Average Restaurant Preparation Time by City
SELECT
    city,
    AVG(restaurant_preparation_time_min) AS average_preparation_time
FROM order_events
GROUP BY city
ORDER BY average_preparation_time DESC;
