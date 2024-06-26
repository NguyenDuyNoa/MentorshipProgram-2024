-- A. Pizza Metrics

-- 1. How many pizzas were ordered?
SELECT COUNT(*) AS total_pizzas_ordered
FROM pizza_runner.customer_orders;

-- 2. How many unique customer orders were made?
SELECT COUNT(DISTINCT order_id) AS unique_customer_orders
FROM pizza_runner.customer_orders;

-- 3. How many successful orders were delivered by each runner?
SELECT runner_id, COUNT(*) AS successful_orders
FROM pizza_runner.runner_orders
WHERE cancellation IS NULL OR cancellation = ''
GROUP BY runner_id;

-- 4. How many of each type of pizza was delivered?
SELECT co.pizza_id, pn.pizza_name, COUNT(*) AS pizzas_delivered
FROM pizza_runner.customer_orders co
JOIN pizza_runner.runner_orders ro ON co.order_id = ro.order_id
JOIN pizza_runner.pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE ro.cancellation IS NULL OR ro.cancellation = ''
GROUP BY co.pizza_id, pn.pizza_name;

-- 5. How many Vegetarian and Meatlovers were ordered by each customer?
SELECT co.customer_id, pn.pizza_name, COUNT(*) AS pizzas_ordered
FROM pizza_runner.customer_orders co
JOIN pizza_runner.pizza_names pn ON co.pizza_id = pn.pizza_id
GROUP BY co.customer_id, pn.pizza_name;

-- 6. What was the maximum number of pizzas delivered in a single order?
SELECT MAX(pizza_count) AS max_pizzas_in_order
FROM (
    SELECT order_id, COUNT(*) AS pizza_count
    FROM pizza_runner.customer_orders
    GROUP BY order_id
) subquery;

-- 7. For each customer, how many delivered pizzas had at least 1 change and how many had no changes?
SELECT co.customer_id,
       COUNT(CASE WHEN (co.exclusions IS NOT NULL AND co.exclusions <> 'null' AND co.exclusions <> '')
                    OR (co.extras IS NOT NULL AND co.extras <> 'null' AND co.extras <> '') THEN 1 END) AS pizzas_with_changes,
       COUNT(CASE WHEN (co.exclusions IS NULL OR co.exclusions = 'null' OR co.exclusions = '')
                    AND (co.extras IS NULL OR co.extras = 'null' OR co.extras = '') THEN 1 END) AS pizzas_without_changes
FROM pizza_runner.customer_orders co
JOIN pizza_runner.runner_orders ro ON co.order_id = ro.order_id
WHERE ro.cancellation IS NULL OR ro.cancellation = ''
GROUP BY co.customer_id;

-- 8. How many pizzas were delivered that had both exclusions and extras?
SELECT COUNT(*) AS pizzas_with_both_changes
FROM pizza_runner.customer_orders co
JOIN pizza_runner.runner_orders ro ON co.order_id = ro.order_id
WHERE (co.exclusions IS NOT NULL AND co.exclusions <> 'null' AND co.exclusions <> '')
  AND (co.extras IS NOT NULL AND co.extras <> 'null' AND co.extras <> '')
  AND (ro.cancellation IS NULL OR ro.cancellation = '');

-- 9. What was the total volume of pizzas ordered for each hour of the day?
SELECT EXTRACT(HOUR FROM order_time) AS order_hour, COUNT(*) AS pizzas_ordered
FROM pizza_runner.customer_orders
GROUP BY order_hour
ORDER BY order_hour;

-- 10. What was the volume of orders for each day of the week?
SELECT TO_CHAR(order_time, 'Day') AS order_day, COUNT(*) AS pizzas_ordered
FROM pizza_runner.customer_orders
GROUP BY order_day
ORDER BY order_day;
