IF OBJECT_ID ('bronze.fintech_users', 'U') IS NOT NULL
	DROP TABLE bronze.fintech_users;
CREATE TABLE bronze.fintech_users (
user_id NVARCHAR(50),
name NVARCHAR(50),
email NVARCHAR(50),
signup_date NVARCHAR(50),
country NVARCHAR(50)
);


IF OBJECT_ID ('bronze.fintech_merchants', 'U') IS NOT NULL
	DROP TABLE bronze.fintech_merchants;
CREATE TABLE bronze.fintech_merchants (
merchant_id NVARCHAR(50),
merchant_name NVARCHAR(50),
category NVARCHAR(50)
);


IF OBJECT_ID ('bronze.fintech_transactions', 'U') IS NOT NULL
	DROP TABLE bronze.fintech_transactions;
CREATE TABLE bronze.fintech_transactions(
transaction_id NVARCHAR(50),
user_id NVARCHAR(50),
merchant_id NVARCHAR(50),
amount VARCHAR(50),
status NVARCHAR(50),
payment_method NVARCHAR(50),
transaction_date NVARCHAR(50),
device NVARCHAR(50)
);