-- Check orders_table columns
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'orders_table';

SELECT MAX(LENGTH(card_number::TEXT)) FROM orders_table;
SELECT MAX(LENGTH(store_code::TEXT)) FROM orders_table;
SELECT MAX(LENGTH(product_code::TEXT)) FROM orders_table;

-- Check dim_users columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';

SELECT MAX(LENGTH(country_code)) AS max_length_country_code 
FROM dim_users;

-- Check dim_store_details columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details';

SELECT *
FROM dim_store_details;

SELECT MAX(LENGTH(store_code)) FROM dim_store_details;
SELECT MAX(LENGTH(country_code)) FROM dim_store_details;

-- Check dim_products columns
SELECT *
FROM dim_products;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_products';

SELECT MAX(LENGTH("EAN")) FROM dim_products;
SELECT MAX(LENGTH(product_code)) FROM dim_products;
SELECT MAX(LENGTH(weight_class)) FROM dim_products;

-- Check dim_date_times columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_date_times';

SELECT *
FROM dim_date_times;

-- Check dim_card_details columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_card_details';

SELECT MAX(LENGTH(card_number)) FROM dim_card_details;
SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details;

-- Confirm primary keys
SELECT DISTINCT
    tc.table_name AS table_name,
    kcu.column_name AS column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name IN (
      'dim_users', 
      'dim_store_details', 
      'dim_products', 
      'dim_date_times', 
      'dim_card_details'
  );

SELECT * FROM dim_users LIMIT 10;
SELECT * FROM dim_store_details LIMIT 10;
SELECT * FROM dim_products LIMIT 10;
SELECT * FROM dim_date_times LIMIT 10;
SELECT * FROM dim_card_details LIMIT 10;

-- Business logic / analysis queries

-- 1) Which countries have physical stores, and how many stores in each?
SELECT 
    country_code AS country,
    COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- 2) Which localities have the most stores?
SELECT 
    locality,
    COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- 3) Which months produced the most sales?
SELECT
    ROUND(SUM(ord.product_quantity * dp.product_price_gbp)::NUMERIC, 2) AS sales,
    ddt.month
FROM orders_table ord
JOIN dim_date_times ddt ON ddt.date_uuid = ord.date_uuid
JOIN dim_products dp ON dp.product_code = ord.product_code
GROUP BY ddt.month
ORDER BY sales DESC
LIMIT 6;

-- 4) How many sales are online vs offline?
SELECT 
    COUNT(store_code) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    'Web' AS location
FROM orders_table
WHERE store_code LIKE 'WEB%'
GROUP BY location

UNION

SELECT 
    COUNT(store_code) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    'Offline' AS location
FROM orders_table
WHERE store_code NOT LIKE 'WEB%'
GROUP BY location;

-- 5) Percentage of total sales by store_type
SELECT 
    s.store_type,
    ROUND(SUM(o.product_quantity * p.product_price_gbp)::NUMERIC, 2) AS total_sales,
    ROUND(
        SUM(o.product_quantity * p.product_price_gbp)::NUMERIC
        / (SELECT SUM(product_quantity * product_price_gbp)::NUMERIC FROM orders_table JOIN dim_products ON orders_table.product_code = dim_products.product_code)
        * 100, 2
    ) AS sales_made_percent
FROM orders_table o
JOIN dim_store_details s ON o.store_code = s.store_code
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY s.store_type
ORDER BY total_sales DESC;

-- 6) Which months in which years produced the highest sales, top 10
WITH monthly_sales AS (
    SELECT 
        EXTRACT(YEAR FROM ddt.datetime)::INT AS year,
        EXTRACT(MONTH FROM ddt.datetime)::INT AS month,
        ROUND(SUM(ot.product_quantity * dp.product_price_gbp)::NUMERIC, 2) AS total_sales
    FROM orders_table ot
    JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
    JOIN dim_products dp ON ot.product_code = dp.product_code
    GROUP BY EXTRACT(YEAR FROM ddt.datetime), EXTRACT(MONTH FROM ddt.datetime)
),
ranked_sales AS (
    SELECT 
        year,
        month,
        total_sales,
        RANK() OVER (PARTITION BY year ORDER BY total_sales DESC) AS rank
    FROM monthly_sales
)
SELECT total_sales, year, month
FROM ranked_sales
WHERE rank = 1
ORDER BY total_sales DESC
LIMIT 10;

-- 7) Staff headcount by country
SELECT 
    country_code,
    SUM(CAST(staff_numbers AS INTEGER)) AS total_staff_numbers
FROM dim_store_details
WHERE staff_numbers ~ '^[0-9]+$'
  AND country_code IS NOT NULL
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- 8) Which German store type is selling the most?
SELECT 
    ROUND(SUM(ot.product_quantity * dp.product_price_gbp)::NUMERIC, 2) AS total_sales,
    sdt.store_type,
    sdt.country_code
FROM orders_table ot
JOIN dim_store_details sdt ON ot.store_code = sdt.store_code
JOIN dim_products dp ON ot.product_code = dp.product_code
WHERE sdt.country_code = 'DE'
GROUP BY sdt.store_type, sdt.country_code
ORDER BY total_sales ASC;

-- 9) Average time between sales, grouped by year
WITH sales_timestamps AS (
    SELECT 
        EXTRACT(YEAR FROM ddt.datetime) AS year,
        ddt.datetime AS current_sale_time,
        LEAD(ddt.datetime) OVER (PARTITION BY EXTRACT(YEAR FROM ddt.datetime)
                                 ORDER BY ddt.datetime) AS next_sale_time
    FROM orders_table ot
    JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
),
time_differences AS (
    SELECT 
        year,
        current_sale_time,
        next_sale_time,
        next_sale_time - current_sale_time AS time_diff
    FROM sales_timestamps
    WHERE next_sale_time IS NOT NULL
),
average_time_per_year AS (
    SELECT 
        year,
        AVG(EXTRACT(EPOCH FROM time_diff)) AS avg_seconds
    FROM time_differences
    GROUP BY year
)
SELECT 
    year,
    json_build_object(
        'hours', FLOOR(avg_seconds / 3600),
        'minutes', FLOOR((avg_seconds % 3600) / 60),
        'seconds', FLOOR(avg_seconds % 60),
        'milliseconds', (avg_seconds - FLOOR(avg_seconds)) * 1000
    ) AS actual_time_taken
FROM average_time_per_year
ORDER BY year;
