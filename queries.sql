
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



