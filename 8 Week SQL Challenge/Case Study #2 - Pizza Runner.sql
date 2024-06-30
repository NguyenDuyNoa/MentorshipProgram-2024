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


-- B. Runner and Customer Experience

-- 1. How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)
SELECT to_char(registration_date, 'ww') as week_start, count(runner_id) as total_runners
FROM pizza_runner.runners
GROUP BY to_char(registration_date, 'ww')
ORDER BY week_start;

-- 2. What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?
WITH pickup_times AS (
  SELECT 
    r.runner_id,
    r.order_id,
    EXTRACT(EPOCH FROM (TO_TIMESTAMP(r.pickup_time, 'YYYY-MM-DD HH24:MI:SS') - c.order_time)) / 60 AS pickup_minutes
  FROM pizza_runner.runner_orders r
  JOIN pizza_runner.customer_orders c ON r.order_id = c.order_id
  WHERE r.pickup_time != 'null' AND r.pickup_time IS NOT NULL
)
SELECT 
  runner_id,
  AVG(pickup_minutes) AS avg_pickup_time
FROM pickup_times
GROUP BY runner_id
ORDER BY runner_id;

-- 3. Is there any relationship between the number of pizzas and how long the order takes to prepare?
WITH order_info AS (
  SELECT 
    r.order_id,
    COUNT(c.pizza_id) AS pizza_count,
    EXTRACT(EPOCH FROM (TO_TIMESTAMP(r.pickup_time, 'YYYY-MM-DD HH24:MI:SS') - c.order_time)) / 60 AS prep_time_minutes
  FROM pizza_runner.runner_orders r
  JOIN pizza_runner.customer_orders c ON r.order_id = c.order_id
  WHERE r.pickup_time != 'null' AND r.pickup_time IS NOT NULL
  GROUP BY r.order_id, r.pickup_time, c.order_time
)
SELECT 
  pizza_count,
  AVG(prep_time_minutes) AS avg_prep_time_minutes
FROM order_info
GROUP BY pizza_count
ORDER BY pizza_count;

-- 4. What was the average distance travelled for each customer?
WITH customer_distances AS (
  SELECT 
    c.customer_id,
    CAST(REGEXP_REPLACE(r.distance, '[^0-9.]', '', 'g') AS NUMERIC) AS distance_km
  FROM pizza_runner.customer_orders c
  JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
  WHERE r.distance != 'null' AND r.distance IS NOT NULL
)
SELECT 
  customer_id,
  ROUND(AVG(distance_km), 2) AS avg_distance_km
FROM customer_distances
GROUP BY customer_id
ORDER BY customer_id;

-- 5. What was the difference between the longest and shortest delivery times for all orders?
WITH delivery_times AS (
  SELECT 
    CASE 
      WHEN duration LIKE '%minutes' THEN REPLACE(duration, 'minutes', '')
      WHEN duration LIKE '%mins' THEN REPLACE(duration, 'mins', '')
      WHEN duration LIKE '%minute' THEN REPLACE(duration, 'minute', '')
      ELSE duration
    END::INTEGER AS duration_minutes
  FROM pizza_runner.runner_orders
  WHERE duration != 'null' AND duration IS NOT NULL
)
SELECT 
  MAX(duration_minutes) - MIN(duration_minutes) AS delivery_time_difference
FROM delivery_times;

-- 6. What was the average speed for each runner for each delivery and do you notice any trend for these values?
WITH speed_calc AS (
  SELECT 
    runner_id,
    order_id,
    CAST(REGEXP_REPLACE(distance, '[^0-9.]', '', 'g') AS NUMERIC) AS distance_km,
    CASE 
      WHEN duration LIKE '%minutes' THEN REPLACE(duration, 'minutes', '')
      WHEN duration LIKE '%mins' THEN REPLACE(duration, 'mins', '')
      WHEN duration LIKE '%minute' THEN REPLACE(duration, 'minute', '')
      ELSE duration
    END::NUMERIC AS duration_minutes
  FROM pizza_runner.runner_orders
  WHERE distance != 'null' AND duration != 'null' AND distance IS NOT NULL AND duration IS NOT NULL
)
SELECT 
  runner_id,
  order_id,
  ROUND(distance_km / (duration_minutes / 60), 2) AS speed_km_per_hour
FROM speed_calc
ORDER BY runner_id, order_id;

-- 7. What is the successful delivery percentage for each runner?
WITH delivery_status AS (
  SELECT 
    runner_id,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN cancellation IS NULL OR cancellation = 'null' OR cancellation = '' THEN 1 ELSE 0 END) AS successful_orders
  FROM pizza_runner.runner_orders
  GROUP BY runner_id
)
SELECT 
  runner_id,
  ROUND(successful_orders::NUMERIC / total_orders * 100, 2) AS success_percentage
FROM delivery_status
ORDER BY runner_id;


-- C. Ingredient Optimisation

-- 1. What are the standard ingredients for each pizza?
SELECT 
  pn.pizza_name,
  STRING_AGG(pt.topping_name, ', ' ORDER BY pt.topping_name) AS standard_ingredients
FROM pizza_runner.pizza_names pn
JOIN pizza_runner.pizza_recipes pr ON pn.pizza_id = pr.pizza_id
CROSS JOIN LATERAL UNNEST(string_to_array(pr.toppings, ', ')) AS t(topping_id)
JOIN pizza_runner.pizza_toppings pt ON pt.topping_id::text = t.topping_id
GROUP BY pn.pizza_name;
-- 2. What was the most commonly added extra?
WITH extras_split AS (
  SELECT 
    UNNEST(string_to_array(NULLIF(extras, 'null'), ', ')) AS extra_id
  FROM pizza_runner.customer_orders
  WHERE extras IS NOT NULL AND extras != 'null' AND extras != ''
)
SELECT 
  pt.topping_name,
  COUNT(*) AS extra_count
FROM extras_split es
JOIN pizza_runner.pizza_toppings pt ON pt.topping_id::text = es.extra_id
GROUP BY pt.topping_name
ORDER BY extra_count DESC
LIMIT 1;
-- 3. What was the most common exclusion?
WITH exclusions_split AS (
  SELECT 
    UNNEST(string_to_array(NULLIF(exclusions, 'null'), ', ')) AS exclusion_id
  FROM pizza_runner.customer_orders
  WHERE exclusions IS NOT NULL AND exclusions != 'null' AND exclusions != ''
)
SELECT 
  pt.topping_name,
  COUNT(*) AS exclusion_count
FROM exclusions_split es
JOIN pizza_runner.pizza_toppings pt ON pt.topping_id::text = es.exclusion_id
GROUP BY pt.topping_name
ORDER BY exclusion_count DESC
LIMIT 1;
-- 4. Generate an order item for each record in the customers_orders table in the format of one of the following:
-- - Meat Lovers
-- - Meat Lovers - Exclude Beef
-- - Meat Lovers - Extra Bacon
-- - Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers

WITH order_details AS (
  SELECT 
    co.order_id,
    co.pizza_id,
    pn.pizza_name,
    STRING_AGG(DISTINCT CASE WHEN et.extra_id IS NOT NULL THEN pt_extra.topping_name ELSE NULL END, ', ') AS extras,
    STRING_AGG(DISTINCT CASE WHEN ex.exclusion_id IS NOT NULL THEN pt_excl.topping_name ELSE NULL END, ', ') AS exclusions
  FROM pizza_runner.customer_orders co
  JOIN pizza_runner.pizza_names pn ON co.pizza_id = pn.pizza_id
  LEFT JOIN LATERAL UNNEST(string_to_array(NULLIF(co.extras, 'null'), ', ')) AS et(extra_id) ON TRUE
  LEFT JOIN LATERAL UNNEST(string_to_array(NULLIF(co.exclusions, 'null'), ', ')) AS ex(exclusion_id) ON TRUE
  LEFT JOIN pizza_runner.pizza_toppings pt_extra ON pt_extra.topping_id::text = et.extra_id
  LEFT JOIN pizza_runner.pizza_toppings pt_excl ON pt_excl.topping_id::text = ex.exclusion_id
  GROUP BY co.order_id, co.pizza_id, pn.pizza_name
)
SELECT
  order_id,
  CASE
    WHEN exclusions IS NULL AND extras IS NULL THEN pizza_name
    WHEN exclusions IS NOT NULL AND extras IS NULL THEN pizza_name || ' - Exclude ' || exclusions
    WHEN exclusions IS NULL AND extras IS NOT NULL THEN pizza_name || ' - Extra ' || extras
    ELSE pizza_name || ' - Exclude ' || exclusions || ' - Extra ' || extras
  END AS order_item
FROM order_details
ORDER BY order_id;
-- 5. Generate an alphabetically ordered comma separated ingredient list for each pizza order from the customer_orders table and add a 2x in front of any relevant ingredients
-- - For example: "Meat Lovers: 2xBacon, Beef, ... , Salami"
WITH pizza_ingredients AS (
  SELECT 
    pn.pizza_id,
    pn.pizza_name,
    UNNEST(string_to_array(pr.toppings, ', ')) AS topping_id
  FROM pizza_runner.pizza_names pn
  JOIN pizza_runner.pizza_recipes pr ON pn.pizza_id = pr.pizza_id
),
order_details AS (
  SELECT 
    co.order_id,
    co.pizza_id,
    COALESCE(string_to_array(NULLIF(co.extras, 'null'), ', '), ARRAY[]::text[]) AS extras,
    COALESCE(string_to_array(NULLIF(co.exclusions, 'null'), ', '), ARRAY[]::text[]) AS exclusions
  FROM pizza_runner.customer_orders co
)
SELECT 
  od.order_id,
  pi.pizza_name || ': ' || STRING_AGG(
    CASE 
      WHEN od.extras @> ARRAY[pi.topping_id] THEN '2x' || pt.topping_name
      WHEN od.exclusions @> ARRAY[pi.topping_id] THEN NULL
      ELSE pt.topping_name
    END,
    ', ' ORDER BY pt.topping_name
  ) AS ingredients_list
FROM order_details od
JOIN pizza_ingredients pi ON od.pizza_id = pi.pizza_id
JOIN pizza_runner.pizza_toppings pt ON pt.topping_id::text = pi.topping_id
WHERE pt.topping_name IS NOT NULL
GROUP BY od.order_id, pi.pizza_name
ORDER BY od.order_id;
-- 6. What is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?
WITH delivered_pizzas AS (
  SELECT co.pizza_id, co.extras, co.exclusions
  FROM pizza_runner.customer_orders co
  JOIN pizza_runner.runner_orders ro ON co.order_id = ro.order_id
  WHERE ro.pickup_time != 'null' AND ro.pickup_time IS NOT NULL
),
pizza_ingredients AS (
  SELECT 
    dp.pizza_id,
    UNNEST(string_to_array(pr.toppings, ', ')) AS topping_id
  FROM delivered_pizzas dp
  JOIN pizza_runner.pizza_recipes pr ON dp.pizza_id = pr.pizza_id
),
all_toppings AS (
  SELECT pi.topping_id
  FROM pizza_ingredients pi
  UNION ALL
  SELECT UNNEST(string_to_array(NULLIF(dp.extras, 'null'), ', ')) AS topping_id
  FROM delivered_pizzas dp
  WHERE dp.extras IS NOT NULL AND dp.extras != 'null'
  EXCEPT ALL
  SELECT UNNEST(string_to_array(NULLIF(dp.exclusions, 'null'), ', ')) AS topping_id
  FROM delivered_pizzas dp
  WHERE dp.exclusions IS NOT NULL AND dp.exclusions != 'null'
)
SELECT 
  pt.topping_name,
  COUNT(*) AS topping_count
FROM all_toppings at
JOIN pizza_runner.pizza_toppings pt ON pt.topping_id::text = at.topping_id
GROUP BY pt.topping_name
ORDER BY topping_count DESC;



-- D. Pricing and Ratings
-- 1. If a Meat Lovers pizza costs $12 and Vegetarian costs $10 and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?
SELECT 
  SUM(CASE 
    WHEN pn.pizza_name = 'Meatlovers' THEN 12
    WHEN pn.pizza_name = 'Vegetarian' THEN 10
    ELSE 0
  END) AS total_revenue
FROM pizza_runner.customer_orders co
JOIN pizza_runner.pizza_names pn ON co.pizza_id = pn.pizza_id
JOIN pizza_runner.runner_orders ro ON co.order_id = ro.order_id
WHERE ro.pickup_time != 'null' AND ro.pickup_time IS NOT NULL;

-- 2. What if there was an additional $1 charge for any pizza extras?
-- - Add cheese is $1 extra
WITH pizza_prices AS (
  SELECT 
    co.order_id,
    co.pizza_id,
    CASE 
      WHEN pn.pizza_name = 'Meatlovers' THEN 12
      WHEN pn.pizza_name = 'Vegetarian' THEN 10
      ELSE 0
    END AS base_price,
    CASE 
      WHEN co.extras IS NULL OR co.extras = 'null' OR co.extras = '' THEN 0
      ELSE CARDINALITY(STRING_TO_ARRAY(co.extras, ', '))
    END AS extra_count
  FROM pizza_runner.customer_orders co
  JOIN pizza_runner.pizza_names pn ON co.pizza_id = pn.pizza_id
  JOIN pizza_runner.runner_orders ro ON co.order_id = ro.order_id
  WHERE ro.pickup_time != 'null' AND ro.pickup_time IS NOT NULL
)
SELECT SUM(base_price + extra_count) AS total_revenue
FROM pizza_prices;

-- 3. The Pizza Runner team now wants to add an additional ratings system that allows customers to rate their runner, how would you design an additional table for this new dataset - generate a schema for this new table and insert your own data for ratings for each successful customer order between 1 to 5.

CREATE TABLE pizza_runner.runner_ratings (
  order_id INTEGER,
  rating INTEGER CHECK (rating BETWEEN 1 AND 5),
  comment TEXT,
  rating_time TIMESTAMP
);

INSERT INTO pizza_runner.runner_ratings (order_id, rating, comment, rating_time)
VALUES
  (1, 5, 'Excellent service!', '2020-01-01 19:10:00'),
  (2, 4, 'Good delivery time', '2020-01-01 20:30:00'),
  (3, 3, 'Average service', '2020-01-03 01:15:00'),
  (4, 5, 'Very prompt delivery', '2020-01-04 14:30:00'),
  (5, 4, 'Pizza was still hot', '2020-01-08 22:00:00'),
  (7, 5, 'Runner was very polite', '2020-01-08 22:15:00'),
  (8, 4, 'Good overall experience', '2020-01-10 01:00:00'),
  (10, 5, 'Arrived earlier than expected', '2020-01-11 19:30:00');
-- 4. Using your newly generated table - can you join all of the information together to form a table which has the following information for successful deliveries?
-- - customer_id
-- - order_id
-- - runner_id
-- - rating
-- - order_time
-- - pickup_time
-- - Time between order and pickup
-- - Delivery duration
-- - Average speed
-- - Total number of pizzas
WITH delivery_info AS (
  SELECT 
    c.customer_id,
    c.order_id,
    r.runner_id,
    rr.rating,
    c.order_time,
    TO_TIMESTAMP(r.pickup_time, 'YYYY-MM-DD HH24:MI:SS') AS pickup_time,
    EXTRACT(EPOCH FROM (TO_TIMESTAMP(r.pickup_time, 'YYYY-MM-DD HH24:MI:SS') - c.order_time)) / 60 AS time_between_order_pickup,
    CAST(REGEXP_REPLACE(r.duration, '[^0-9]', '', 'g') AS INTEGER) AS delivery_duration,
    CAST(REGEXP_REPLACE(r.distance, '[^0-9.]', '', 'g') AS NUMERIC) / (CAST(REGEXP_REPLACE(r.duration, '[^0-9]', '', 'g') AS NUMERIC) / 60) AS avg_speed,
    COUNT(c.pizza_id) AS total_pizzas
  FROM pizza_runner.customer_orders c
  JOIN pizza_runner.runner_orders r ON c.order_id = r.order_id
  LEFT JOIN pizza_runner.runner_ratings rr ON c.order_id = rr.order_id
  WHERE r.pickup_time != 'null' AND r.pickup_time IS NOT NULL
  GROUP BY c.customer_id, c.order_id, r.runner_id, rr.rating, c.order_time, r.pickup_time, r.duration, r.distance
)
SELECT 
  customer_id,
  order_id,
  runner_id,
  rating,
  order_time,
  pickup_time,
  ROUND(time_between_order_pickup::numeric, 2) AS time_between_order_pickup_mins,
  delivery_duration AS delivery_duration_mins,
  ROUND(avg_speed::numeric, 2) AS avg_speed_km_per_hour,
  total_pizzas
FROM delivery_info
ORDER BY order_id;

-- 5. If a Meat Lovers pizza was $12 and Vegetarian $10 fixed prices with no cost for extras and each runner is paid $0.30 per kilometre traveled - how much money does Pizza Runner have left over after these deliveries?
WITH pizza_revenue AS (
  SELECT 
    co.order_id,
    SUM(CASE 
      WHEN pn.pizza_name = 'Meatlovers' THEN 12
      WHEN pn.pizza_name = 'Vegetarian' THEN 10
      ELSE 0
    END) AS order_revenue
  FROM pizza_runner.customer_orders co
  JOIN pizza_runner.pizza_names pn ON co.pizza_id = pn.pizza_id
  GROUP BY co.order_id
),
runner_payment AS (
  SELECT 
    order_id,
    0.30 * CAST(REGEXP_REPLACE(distance, '[^0-9.]', '', 'g') AS NUMERIC) AS runner_payment
  FROM pizza_runner.runner_orders
  WHERE pickup_time != 'null' AND pickup_time IS NOT NULL
)
SELECT 
  SUM(pr.order_revenue) AS total_revenue,
  SUM(rp.runner_payment) AS total_runner_payment,
  SUM(pr.order_revenue) - SUM(rp.runner_payment) AS profit
FROM pizza_revenue pr
JOIN runner_payment rp ON pr.order_id = rp.order_id;
