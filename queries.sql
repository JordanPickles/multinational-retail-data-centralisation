
-- Finds lengths of longest value in the column to be used in data casting VARCHAR
SELECT length(max(cast(card_number as Text)))
FROM orders_table
GROUP BY card_number
ORDER BY length(max(cast(card_number as Text))) desc
LIMIT 1; 
-- largest number = 19

SELECT length(max(cast(store_code as Text)))
FROM orders_table
GROUP BY store_code
ORDER BY length(max(cast(store_code as Text))) desc
LIMIT 1; -- = to 12

SELECT length(max(cast(product_code as Text)))
FROM orders_table
GROUP BY product_code
ORDER BY length(max(cast(product_code as Text))) desc
LIMIT 1; --  = to 11

-- Permenantly casts the data typers in the table
ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
	ALTER COLUMN product_quantity TYPE SMALLINT;

SELECT length(max(cast(country_code as Text)))
FROM dim_users
GROUP BY country_code
ORDER BY length(max(cast(country_code as Text))) desc
LIMIT 1; 
-- = to 3

-- Permenantly casts the data typers in the table
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(225),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE USING CAST(date_of_birth AS DATE),
	ALTER COLUMN join_date TYPE DATE USING CAST(join_date AS DATE),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid AS UUID),
	ALTER COLUMN country_code TYPE VARCHAR(3);
	
-- Finds lengths of longest value in the column to be used in data casting VARCHAR
SELECT length(max(cast(country_code as Text)))
FROM dim_store_details
GROUP BY country_code
ORDER BY length(max(cast(country_code as Text))) desc
LIMIT 1; 
-- = to 2


SELECT length(max(cast(store_code as Text)))
FROM dim_store_details
GROUP BY store_code
ORDER BY length(max(cast(store_code as Text))) desc
LIMIT 1; 
-- = to 12

-- Updates NA values to NULL
UPDATE dim_store_details 
SET address = NULL
WHERE address = 'N/A';

UPDATE dim_store_details 
SET longitude = NULL
WHERE longitude = 'N/A';

UPDATE dim_store_details 
SET locality = NULL
WHERE locality = 'N/A';

UPDATE dim_store_details 
SET lat = NULL
WHERE lat = 'N/A';

UPDATE dim_store_details
SET latitude = CONCAT(CAST(lat as FLOAT), CAST(latitude as FLOAT));

-- Dops unneeded columns
ALTER TABLE dim_store_details
	DROP lat,
	DROP level_0;

-- Casts data types permenantly
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING CAST(longitude AS FLOAT),
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE USING CAST(opening_date as DATE),
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);


-- Removes the pound sign in the product price column
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');


-- Alters the column to a float type
ALTER TABLE dim_products 
	ALTER COLUMN weight TYPE FLOAT USING CAST(weight as FLOAT),
	ADD COLUMN weight_class VARCHAR;

-- Adds text categories based on the weights of the products
UPDATE dim_products
SET weight_class =
	CASE 
		WHEN weight < 2.0 THEN 'Light'
		WHEN weight >= 2 
			AND weight < 40 THEN 'Mid_Sized'
		WHEN weight >= 40 
			AND weight <140 THEN 'Heavy'
		WHEN weight >= 140 THEN 'Truck_Required'
	END;
	
SELECT length(max(cast(product_code as Text)))
FROM dim_products
GROUP BY product_code
ORDER BY length(max(cast(product_code as Text))) desc
LIMIT 1; -- = to 11

SELECT length(max(cast(weight_class as Text)))
FROM dim_products
GROUP BY weight_class
ORDER BY length(max(cast(weight_class as Text))) desc
LIMIT 1; -- = to 14

SELECT length(max(cast("EAN" as Text)))
FROM dim_products
GROUP BY "EAN"
ORDER BY length(max(cast("EAN" as Text))) desc
LIMIT 1; -- = to 17

-- Renames column in dim_products
ALTER TABLE dim_products 
	RENAME COLUMN removed to still_available;

-- Permenantly alters data types in the table
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT USING CAST(product_price as FLOAT),
	ALTER COLUMN weight TYPE FLOAT USING CAST(weight as FLOAT),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING CAST(date_added as DATE),
	ALTER COLUMN uuid TYPE UUID USING CAST(uuid as UUID),
	ALTER COLUMN "EAN" TYPE VARCHAR(17)
	ALTER COLUMN weight_class TYPE VARCHAR(14),
	ALTER COLUMN still_available TYPE boolean USING (still_available ='Still_available');
-- 
	