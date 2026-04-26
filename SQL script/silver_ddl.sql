IF OBJECT_ID ('silver.fintech_users', 'U') IS NOT NULL
	DROP TABLE silver.fintech_users;
CREATE TABLE silver.fintech_users (
user_id NVARCHAR(50),
name NVARCHAR(50),
email NVARCHAR(50),
signup_date DATE,
country NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID ('silver.fintech_merchants', 'U') IS NOT NULL
	DROP TABLE silver.fintech_merchants;
CREATE TABLE silver.fintech_merchants (
merchant_id NVARCHAR(50),
merchant_name NVARCHAR(50),
category NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID ('silver.fintech_transactions', 'U') IS NOT NULL
	DROP TABLE silver.fintech_transactions;
CREATE TABLE silver.fintech_transactions(
transaction_id NVARCHAR(50),
user_id NVARCHAR(50),
merchant_id NVARCHAR (50),
amount DECIMAL(12,2),
status NVARCHAR(50),
payment_method NVARCHAR(50),
transaction_date DATE,
device NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);