-- Answering business scenario questions in PostgreSQL


-- How many stores does the business have and in wich countires?

SELECT country_code, COUNT(country_code) as total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores desc;

-- The business stakeholders would like to know which locations currently have the most stores.

SELECT locality, count(locality) as total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores desc
limit 20;


-- Which months produce the average highest cost of sales typically? 
-- Query the database to find which months typically have the most sales.

SELECT SUM(dim_products.product_price * product_quantity) as total_sales, dim_date_times.month
FROM orders_table
	LEFT JOIN dim_date_times on orders_table.date_uuid = dim_date_times.date_uuid
	LEFT JOIN dim_products on orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales desc;
