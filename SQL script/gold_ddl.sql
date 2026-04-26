/*
===========================================================================================================================
Gold Layer: Tables were joined to form an analytical view

This view can be queried directly for analysis and reporting
===========================================================================================================================
*/

IF OBJECT_ID ('gold.fintech_users', 'V') IS NOT NULL
	DROP VIEW gold.fintech_users;

GO
CREATE VIEW gold.fintech_users AS
SELECT
	user_id,
	name AS user_name,
	country
FROM silver.fintech_users;

GO
IF OBJECT_ID ('gold.fintech_merchants', 'V') IS NOT NULL
	DROP VIEW gold.fintech_merchants;

GO
CREATE VIEW gold.fintech_merchants AS
SELECT
	merchant_id,
	merchant_name,
	category
FROM silver.fintech_merchants;

GO
IF OBJECT_ID ('gold.fintech_transactions', 'V') IS NOT NULL
	DROP VIEW gold.fintech_transactions;

GO
CREATE VIEW gold.fintech_transactions AS
SELECT
	 transaction_id,
	 user_id,
	 merchant_id,
	 transaction_date,
	 YEAR(transaction_date) AS year,
	 MONTH(transaction_date) AS month,
	 FORMAT(transaction_date, 'MMMM') AS month_name,
	 amount,
	 status,
	 payment_method,
	 device,
	 CASE
		WHEN status = 'Success' THEN 1
		ELSE 0
	END AS success_flag,
	CASE
		WHEN amount < 1000 THEN 'Low'
		WHEN amount BETWEEN 1000 AND 10000 THEN 'Medium'
		ELSE 'High'
	END AS transaction_band
FROM silver.fintech_transactions;
GO

SELECT * FROM gold.fintech_users;
SELECT * FROM gold.fintech_merchants;
SELECT * FROM gold.fintech_transactions;