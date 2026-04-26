/*
===========================================================================================================================
Bronze layer: Containing the raw datasets from the soure

Usage Query:
		EXEC bronze.load_bronze
===========================================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Users Table';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: fintech_users';
		TRUNCATE TABLE bronze.fintech_users;

		PRINT '>> Inserting Data Into: fintech_users';
		BULK INSERT bronze.fintech_users
		FROM 'C:\Users\cw_86\Desktop\fintech\source\fintech_users.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading Merchants Table';
		PRINT '------------------------------------------------';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: fintech_merchants';
		TRUNCATE TABLE bronze.fintech_merchants;

		PRINT '>> Inserting Data Into: fintech_merchants';
		BULK INSERT bronze.fintech_merchants
		FROM 'C:\Users\cw_86\Desktop\fintech\source\fintech_merchants.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading Transactions Table';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: fintech_transactions';
		TRUNCATE TABLE bronze.fintech_transactions;

		PRINT '>> Inserting Data Into: fintech_transactions';
		BULK INSERT bronze.fintech_transactions
		FROM 'C:\Users\cw_86\Desktop\fintech\source\fintech_transactions.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';

		SET @batch_end_time = GETDATE();
		PRINT '================================================'
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'second';
		PRINT '================================================'
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END