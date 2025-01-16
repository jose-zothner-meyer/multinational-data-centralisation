-- Rename the table
ALTER TABLE dim_orders
RENAME TO orders_table;

-- Alter columns in orders_table
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- Alter columns in dim_users
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

-- Merge latitude columns or clean data in dim_store_details as needed
DELETE FROM dim_store_details 
WHERE longitude ~ '[^0-9.-]';

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE NUMERIC USING latitude::NUMERIC,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

-- Add weight_class column in dim_products
ALTER TABLE dim_products
    ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight_kg < 2 THEN 'Light'
    WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
    WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
    WHEN weight_kg >= 140 THEN 'Truck_Required'
    ELSE 'Unknown'
END;

DELETE FROM dim_products
WHERE product_name IS NULL 
  AND product_price_gbp IS NULL 
  AND category IS NULL 
  AND date_added IS NULL;

-- Rename and cast columns in dim_products
ALTER TABLE dim_products
    RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
    ALTER COLUMN product_price_gbp TYPE FLOAT USING product_price_gbp::FLOAT,
    ALTER COLUMN weight_kg TYPE FLOAT USING weight_kg::FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(17) USING "EAN"::VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN "uuid" TYPE UUID USING "uuid"::UUID,
    ALTER COLUMN still_available TYPE BOOLEAN USING CASE WHEN still_available = 'true' THEN true ELSE false END,
    ALTER COLUMN weight_class TYPE VARCHAR(14) USING weight_class::VARCHAR(14);

-- Alter dim_date_times: add year, month, day, time
ALTER TABLE dim_date_times
    ADD COLUMN year INT,
    ADD COLUMN month INT,
    ADD COLUMN day INT,
    ADD COLUMN time VARCHAR(8);

UPDATE dim_date_times
SET year = EXTRACT(YEAR FROM datetime),
    month = EXTRACT(MONTH FROM datetime),
    day = EXTRACT(DAY FROM datetime),
    time = TO_CHAR(datetime, 'HH24:MI:SS');

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Alter dim_card_details
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19),
    ALTER COLUMN expiry_date TYPE VARCHAR(5) USING expiry_date::VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Add primary keys to each dim table
DELETE FROM dim_users
WHERE user_uuid IS NULL;
ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);

DELETE FROM dim_store_details
WHERE store_code IS NULL;
ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);

DELETE FROM dim_products
WHERE product_code IS NULL;
ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);

DELETE FROM dim_card_details
WHERE card_number IS NULL;
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);

-- Add foreign keys to orders_table
ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid
    FOREIGN KEY (user_uuid)
    REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_store_code
    FOREIGN KEY (store_code)
    REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_product_code
    FOREIGN KEY (product_code)
    REFERENCES dim_products(product_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_date_uuid
    FOREIGN KEY (date_uuid)
    REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_number
    FOREIGN KEY (card_number)
    REFERENCES dim_card_details(card_number);
