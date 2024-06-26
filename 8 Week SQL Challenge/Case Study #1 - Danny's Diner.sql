/* --------------------
   Case Study Questions
   --------------------*/

-- 1. What is the total amount each customer spent at the restaurant?
SELECT 
  s.customer_id,
  SUM(m.price) AS total_revenue
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY 
  s.customer_id;
-- 2. How many days has each customer visited the restaurant?
SELECT 
  s.customer_id,
  COUNT(DISTINCT s.order_date) AS visit_days
FROM 
  dannys_diner.sales s
GROUP BY 
  s.customer_id;

-- 3. What was the first item from the menu purchased by each customer?
SELECT 
  s.customer_id,
  s.order_date,
  s.product_id,
  m.product_name
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
WHERE 
  s.order_date = (
    SELECT MIN(s2.order_date)
    FROM dannys_diner.sales s2
    WHERE s2.customer_id = s.customer_id
  )
ORDER BY 
  s.customer_id;

-- 4. What is the most purchased item on the menu and how many times was it purchased by all customers?
SELECT 
  m.product_name,
  COUNT(*) AS purchase_count
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY 
  m.product_name
ORDER BY 
  purchase_count DESC
LIMIT 1;

-- 5. Which item was the most popular for each customer?
SELECT 
  m.product_name,
  COUNT(*) AS purchase_count
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY 
  m.product_name
ORDER BY 
  purchase_count DESC
LIMIT 1;

-- 6. Which item was purchased first by the customer after they became a member?
SELECT 
  s.customer_id,
  m.product_name
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
JOIN 
  dannys_diner.members mb ON s.customer_id = mb.customer_id
WHERE 
  s.order_date = (
    SELECT MIN(s2.order_date)
    FROM dannys_diner.sales s2
    WHERE s2.customer_id = s.customer_id
    AND s2.order_date >= mb.join_date
  )
ORDER BY 
  s.customer_id;

-- 7. Which item was purchased just before the customer became a member?
SELECT 
  s.customer_id,
  m.product_name
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
JOIN 
  dannys_diner.members mb ON s.customer_id = mb.customer_id
WHERE 
  s.order_date = (
    SELECT MAX(s2.order_date)
    FROM dannys_diner.sales s2
    WHERE s2.customer_id = s.customer_id
    AND s2.order_date < mb.join_date
  )
ORDER BY 
  s.customer_id;

-- 8. What is the total items and amount spent for each member before they became a member?
SELECT 
  s.customer_id,
  COUNT(s.product_id) AS total_items,
  SUM(m.price) AS total_spent
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
JOIN 
  dannys_diner.members mb ON s.customer_id = mb.customer_id
WHERE 
  s.order_date < mb.join_date
GROUP BY 
  s.customer_id;

-- 9.  If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
SELECT 
  s.customer_id,
  SUM(
    CASE 
      WHEN m.product_name = 'sushi' THEN m.price * 20
      ELSE m.price * 10
    END
  ) AS total_points
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY 
  s.customer_id;

-- 10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?
SELECT 
  s.customer_id,
  SUM(
    CASE 
      WHEN s.order_date BETWEEN mb.join_date AND (mb.join_date + INTERVAL '6 days') THEN 
        CASE 
          WHEN m.product_name = 'sushi' THEN m.price * 40
          ELSE m.price * 20
        END
      ELSE 
        CASE 
          WHEN m.product_name = 'sushi' THEN m.price * 20
          ELSE m.price * 10
        END
    END
  ) AS total_points
FROM 
  dannys_diner.sales s
JOIN 
  dannys_diner.menu m ON s.product_id = m.product_id
JOIN 
  dannys_diner.members mb ON s.customer_id = mb.customer_id
WHERE 
  s.customer_id IN ('A', 'B')
  AND s.order_date <= '2021-01-31'
GROUP BY 
  s.customer_id;
