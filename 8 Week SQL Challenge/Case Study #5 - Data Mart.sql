-- 1. Data Cleansing Steps
-- In a single query, perform the following operations and generate a new table in the data_mart schema named clean_weekly_sales:
CREATE TABLE data_mart.clean_weekly_sales AS
SELECT
-- - Convert the week_date to a DATE format
  TO_DATE(week_date, 'DD/MM/YY') AS week_date,

-- - Add a week_number as the second column for each week_date value, for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc
  EXTRACT(WEEK FROM TO_DATE(week_date, 'DD/MM/YY')) AS week_number,

-- - Add a month_number with the calendar month for each week_date value as the 3rd column
  EXTRACT(MONTH FROM TO_DATE(week_date, 'DD/MM/YY')) AS month_number,

-- - Add a calendar_year column as the 4th column containing either 2018, 2019 or 2020 values
  EXTRACT(YEAR FROM TO_DATE(week_date, 'DD/MM/YY')) AS calendar_year,

  region,
  platform,
-- - Add a new column called age_band after the original segment column using the following mapping on the number inside the segment value
  CASE 
    WHEN segment = 'null' THEN 'unknown'
    ELSE segment
  END AS segment,
  CASE 
    WHEN segment = 'null' THEN 'unknown'
    WHEN RIGHT(segment, 1) = '1' THEN 'Young Adults'
    WHEN RIGHT(segment, 1) = '2' THEN 'Middle Aged'
    WHEN RIGHT(segment, 1) IN ('3', '4') THEN 'Retirees'
    ELSE 'unknown'
  END AS age_band,
-- - Add a new demographic column using the following mapping for the first letter in the segment values:
-- - Ensure all null string values with an "unknown" string value in the original segment column as well as the new age_band and demographic columns
 CASE
    WHEN segment = 'null' THEN 'unknown'
    WHEN LEFT(segment, 1) = 'C' THEN 'Couples'
    WHEN LEFT(segment, 1) = 'F' THEN 'Families'
    ELSE 'unknown'
  END AS demographic,
  
  customer_type,
  transactions,
  sales,
-- - Generate a new avg_transaction column as the sales value divided by transactions rounded to 2 decimal places for each record
  ROUND(CAST(sales AS DECIMAL) / NULLIF(transactions, 0), 2) AS avg_transaction
FROM data_mart.weekly_sales;



-- 2. Data Exploration

-- 1. What day of the week is used for each week_date value?
SELECT DISTINCT TO_CHAR(week_date, 'Day') AS day_of_week
FROM data_mart.clean_weekly_sales;

-- 2. What range of week numbers are missing from the dataset?
WITH all_weeks AS (
  SELECT generate_series(1, 52) AS week_number
)
SELECT week_number
FROM all_weeks
WHERE week_number NOT IN (SELECT DISTINCT week_number FROM data_mart.clean_weekly_sales)
ORDER BY week_number;

-- 3. How many total transactions were there for each year in the dataset?
SELECT 
  calendar_year,
  SUM(transactions) AS total_transactions
FROM data_mart.clean_weekly_sales
GROUP BY calendar_year
ORDER BY calendar_year;

-- 4. What is the total sales for each region for each month?
SELECT 
  region,
  month_number,
  SUM(sales) AS total_sales
FROM data_mart.clean_weekly_sales
GROUP BY region, month_number
ORDER BY region, month_number;

-- 5. What is the total count of transactions for each platform
SELECT 
  platform,
  SUM(transactions) AS total_transactions
FROM data_mart.clean_weekly_sales
GROUP BY platform;

-- 6. What is the percentage of sales for Retail vs Shopify for each month?
WITH monthly_sales AS (
  SELECT 
    calendar_year,
    month_number,
    platform,
    SUM(sales) AS total_sales
  FROM data_mart.clean_weekly_sales
  GROUP BY calendar_year, month_number, platform
)
SELECT 
  calendar_year,
  month_number,
  ROUND(100.0 * SUM(CASE WHEN platform = 'Retail' THEN total_sales ELSE 0 END) / SUM(total_sales), 2) AS retail_percentage,
  ROUND(100.0 * SUM(CASE WHEN platform = 'Shopify' THEN total_sales ELSE 0 END) / SUM(total_sales), 2) AS shopify_percentage
FROM monthly_sales
GROUP BY calendar_year, month_number
ORDER BY calendar_year, month_number;

-- 7. What is the percentage of sales by demographic for each year in the dataset?
WITH yearly_sales AS (
  SELECT 
    calendar_year,
    demographic,
    SUM(sales) AS total_sales
  FROM data_mart.clean_weekly_sales
  GROUP BY calendar_year, demographic
)
SELECT 
  calendar_year,
  demographic,
  ROUND(100.0 * total_sales / SUM(total_sales) OVER (PARTITION BY calendar_year), 2) AS percentage
FROM yearly_sales
ORDER BY calendar_year, demographic;

-- 8. Which age_band and demographic values contribute the most to Retail sales?
SELECT 
  age_band,
  demographic,
  SUM(sales) AS total_sales,
  ROUND(100.0 * SUM(sales) / SUM(SUM(sales)) OVER (), 2) AS percentage
FROM data_mart.clean_weekly_sales
WHERE platform = 'Retail'
GROUP BY age_band, demographic
ORDER BY total_sales DESC
LIMIT 5;

-- 9. Can we use the avg_transaction column to find the average transaction size for each year for Retail vs Shopify? If not - how would you calculate it instead?
SELECT 
  calendar_year,
  platform,
  ROUND(AVG(avg_transaction), 2) AS avg_transaction_size
FROM data_mart.clean_weekly_sales
GROUP BY calendar_year, platform
ORDER BY calendar_year, platform;