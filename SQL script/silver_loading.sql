/*
===========================================================================================================================
Silver layer: Containing the ETL (Extraction, Transformation, Loading) process of the data from the bronze layer
Usage Query:
		EXEC silver.load_silver
===========================================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Users Table';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.fintech_users';
		TRUNCATE TABLE silver.fintech_users;
		PRINT '>> Inserting Data Into: silver.fintech_users';
		INSERT INTO silver.fintech_users(
			user_id,
			name,
			email,
			signup_date,
			country
		)
		SELECT
		user_id,
		name,
		CASE
			WHEN email LIKE '%@%' THEN email
			ELSE NULL
		END AS email,
		CAST (signup_date AS DATE) AS signup_date,
		CASE
			WHEN country IN ('NIG','Nigeria') THEN 'Nigeria'
			ELSE 'Others'
		END AS country
		FROM bronze.fintech_users;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading Merchants Table';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.fintech_merchants';
		TRUNCATE TABLE silver.fintech_merchants;
		PRINT '>> Inserting Data Into: silver.fintech_merchants';
		INSERT INTO silver.fintech_merchants(
			merchant_id,
			merchant_name,
			category
		)
		SELECT 
		merchant_id,
		merchant_name,
		CASE
			WHEN category = CHAR(13) THEN ISNULL(NULLIF(REPLACE(category, CHAR(13), ''), ''), 'Unknown')
			ELSE REPLACE(category, CHAR(13), '')
		END AS category
		FROM bronze.fintech_merchants;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading Transactions Table';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.fintech_transactions';
		TRUNCATE TABLE silver.fintech_transactions;
		PRINT '>> Inserting Data Into: silver.fintech_transactions';
		INSERT INTO silver.fintech_transactions(
			transaction_id,
			user_id,
			merchant_id,
			amount,
			status,
			payment_method,
			transaction_date,
			device
		)
		SELECT
		transaction_id, user_id, merchant_id,
		amount, status, payment_method,
		transaction_date, device
		FROM(
			SELECT 
			transaction_id,
			user_id,
			merchant_id,
			CAST(
				REPLACE(REPLACE(amount, 'Géª', ''), 'NGN', '')
			AS DECIMAL(12,2)
			)AS amount,
			CONCAT(
				UPPER(LEFT(status, 1)), 
				LOWER(SUBSTRING(status, 2, LEN(status)))
			) AS status,
			ISNULL(payment_method, 'Unknown') AS payment_method,
			COALESCE(
				TRY_CONVERT(DATE, transaction_date, 23),
				TRY_CONVERT(DATE, transaction_date, 103),
				TRY_CONVERT(DATE, transaction_date, 110)
			)AS transaction_date,
			device,
			ROW_NUMBER() 
				OVER (PARTITION BY transaction_id ORDER BY
				CASE
					WHEN status = 'Success' THEN 1
					WHEN status = 'Pending' THEN 2
					ELSE 3
				END,
				transaction_date DESC,
				amount DESC
			) AS rn
			FROM bronze.fintech_transactions
		) AS deduplicated_transactions
		WHERE rn = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';

		SET @batch_end_time = GETDATE();
		PRINT '================================================'
		PRINT 'Loading silver Layer is Completed';
		PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'second';
		PRINT '================================================'
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END
