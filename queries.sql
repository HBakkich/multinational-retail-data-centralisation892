--Milestone 2: Create the database schema


ALTER TABLE orders_table 
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(30),
    ALTER COLUMN store_code TYPE VARCHAR(20),
    ALTER COLUMN product_code TYPE VARCHAR(20),
    ALTER COLUMN product_quantity TYPE SMALLINT;

ALTER TABLE dim_users 
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN join_date TYPE DATE;

ALTER TABLE dim_store_details
    DROP COLUMN lat,
    ALTER COLUMN longitude TYPE FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(20),
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN latitude TYPE FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);


UPDATE dim_products
SET product_price = TRIM ('£' FROM product_price);
ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(255); 

UPDATE dim_products
SET weight_class = CASE WHEN weight < 2 THEN 'Light'
                        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                        WHEN weight >= 140 THEN 'Truck_Required'
                        END;
ALTER TABLE dim_products RENAME COLUMN removed TO still_available;
UPDATE dim_products
SET still_available = CASE WHEN still_available = 'Still_available' THEN 'True'
                        WHEN still_available = 'Removed' THEN 'False'
                        END;
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(20),
    ALTER COLUMN product_code TYPE VARCHAR(20),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
    ALTER COLUMN weight_class TYPE VARCHAR(20);

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE FLOAT USING month::FLOAT,
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(30),
    ALTER COLUMN expiry_date TYPE VARCHAR(30),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);
ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);
ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);

ALTER TABLE orders_table 
    ADD CONSTRAINT user_uuid_fkey 
    FOREIGN KEY (user_uuid) 
    REFERENCES dim_users (user_uuid),
    ADD CONSTRAINT store_code_fkey 
    FOREIGN KEY (store_code) 
    REFERENCES dim_store_details (store_code),
    ADD CONSTRAINT product_code_fkey 
    FOREIGN KEY (product_code) 
    REFERENCES dim_products (product_code),
    ADD CONSTRAINT date_uuid_fkey 
    FOREIGN KEY (date_uuid) 
    REFERENCES dim_date_times (date_uuid),
    ADD CONSTRAINT card_number_fkey 
    FOREIGN KEY (card_number) 
    REFERENCES dim_card_details (card_number);


--Milestone 3: Querying the data


-- The Operations team would like to know which countries we currently operate in and which country now has the most stores.
SELECT DISTINCT country_code AS Country, 
        COUNT(store_code) AS total_no_stores
        FROM dim_store_details
        GROUP BY country_code;    


-- The business stakeholders would like to know which locations currently have the most stores.
-- They would like to close some stores before opening more in other locations.
-- Find out which locations have the most stores currently. 
SELECT DISTINCT locality, 
        COUNT(store_code) AS total_no_stores 
        FROM dim_store_details
        GROUP BY locality
        HAVING COUNT(store_code) >= 10
        ORDER BY total_no_stores DESC;


-- Query the database to find out which months have produced the most sales.
SELECT SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales, dim_date_times.month 
    FROM orders_table 
    JOIN dim_products ON orders_table.product_code = dim_products.product_code
    JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
    GROUP BY dim_date_times.month
    ORDER BY total_sales DESC
    LIMIT 6


-- The company is looking to increase its online sales.
-- They want to know how many sales are happening online vs offline.
-- Calculate how many products were sold and the amount of sales made for online and offline purchases.
SELECT count(orders_table.product_quantity) AS numbers_of_sales, 
sum(orders_table.product_quantity) AS product_quantity_count, 
(CASE WHEN dim_store_details.store_type 
        LIKE 'Web Portal' 
        THEN 'Web' 
        ELSE 'Offline' 
        END)  
AS location FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY location
ORDER BY location DESC


-- The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
-- Find out the total and percentage of sales coming from each of the different store types.
SELECT store_type, ROUND(total_sales::decimal, 2), 
    ROUND((total_sales * 100.0 / sum(total_sales) OVER ())::decimal, 2) AS "percentage_total(%)" 
    FROM
        (SELECT dim_store_details.store_type, 
        SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
        FROM dim_store_details 
        JOIN orders_table ON orders_table.store_code = dim_store_details.store_code
        JOIN dim_products ON orders_table.product_code = dim_products.product_code
        GROUP BY store_type)
    ORDER BY total_sales DESC


-- The company stakeholders want assurances that the company has been doing well recently.
-- Find which months in which years have had the most sales historically.
WITH sales_year_and_month AS (
    SELECT *, row_number() OVER (PARTITION BY year ORDER BY total_sales DESC) AS rn
        FROM (
            SELECT year, month, SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
            FROM dim_date_times
            JOIN orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
            JOIN dim_products ON dim_products.product_code = orders_table.product_code
            GROUP BY year
                , month
            )
    )
SELECT ROUND(total_sales::decimal, 2), month, year
    FROM sales_year_and_month
    WHERE rn = 1
    ORDER BY total_sales DESC
    LIMIT 10


-- The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.
SELECT SUM(staff_numbers) AS total_staff_numbers, country_code FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC


-- The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.
SELECT ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::decimal, 2) AS total_sales, dim_store_details.store_type, country_code
    FROM orders_table 
    JOIN dim_products ON orders_table.product_code = dim_products.product_code
    JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
    WHERE country_code LIKE 'DE'
    GROUP BY store_type, country_code
    ORDER BY total_sales ASC


-- Sales would like the get an accurate metric for how quickly the company is making sales.
-- Determine the average time taken between each sale grouped by year.
WITH time_table(date_uuid, year, month, day, hour, minutes, seconds) AS (
	SELECT date_uuid, year, month, day,
		EXTRACT(hour FROM timestamp::time) AS hour,
		EXTRACT(minute FROM timestamp::time) AS minutes,
		EXTRACT(second FROM timestamp::time) AS seconds
	FROM dim_date_times),
	
	timestamp_table(timestamp, date_uuid, year) AS (
    SELECT MAKE_TIMESTAMP(time_table.year::INT, time_table.month::INT,
                            time_table.day::INT, time_table.hour::INT,	
                            time_table.minutes::INT, time_table.seconds::FLOAT) AS order_timestamp,
        time_table.date_uuid AS date_uuid, 
        time_table.year AS year
    FROM time_table),
	
	time_stamp_diffs(year, time_diff) AS (
    SELECT timestamp_table.year, timestamp_table.timestamp - LAG(timestamp_table.timestamp) OVER (ORDER BY timestamp_table.timestamp asc) AS time_diff
    FROM orders_table
    JOIN timestamp_table ON orders_table.date_uuid = timestamp_table.date_uuid),

	year_time_diffs(year, average_time_diff) AS (
    SELECT year, AVG(time_diff) AS average_time_diff
    FROM time_stamp_diffs
    GROUP BY year
    ORDER BY average_time_diff desc)
		
SELECT 
	year, 
	CONCAT('hours: ', EXTRACT(HOUR FROM average_time_diff),
					'  minutes: ', EXTRACT(MINUTE FROM average_time_diff),
				   '  seconds: ', CAST(EXTRACT(SECOND FROM average_time_diff) AS INT),
				   '  milliseconds: ', CAST(EXTRACT(MILLISECOND FROM average_time_diff) AS INT)) AS actual_time_taken
FROM year_time_diffs
LIMIT 5;

