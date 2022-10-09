CREATE DATABASE commerce;

USE commerce;

CREATE TABLE cust_dimen(
	Customer_Name varchar(50) NOT NULL,
	Province varchar(50) NOT NULL,
	Region varchar(50) NOT NULL,
	Customer_Segment varchar(50) NOT NULL,
	Cust_ID varchar(255) NOT NULL,
	PRIMARY KEY (Cust_ID)
	)

--DROP TABLE cust_dimen

TRUNCATE TABLE cust_dimen;

BULK INSERT [dbo].[cust_dimen]
FROM 'C:\Users\Kayaharman\Desktop\kayaharman\LECTURES\Data Science\SQL\E-COMMERCE PROJECT\cust_dimen.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2
)
GO

SELECT *
FROM dbo.cust_dimen

CREATE TABLE shipping_dimen(
	Order_ID int NOT NULL,
	Ship_Mode varchar(50) NOT NULL,
	Ship_Date date NOT NULL,
	Ship_ID varchar(50) NOT NULL,
	PRIMARY KEY (Ship_ID)
	)

TRUNCATE TABLE dbo.shipping_dimen;

BULK INSERT dbo.shipping_dimen
FROM 'C:\Users\Kayaharman\Desktop\kayaharman\LECTURES\Data Science\SQL\E-COMMERCE PROJECT\Shipping_Dimen.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2
)
GO

SELECT *
FROM dbo.shipping_dimen

CREATE TABLE orders_dimen(
	Order_Date date NOT NULL,
	Order_Priority varchar(50) NOT NULL,
	Ord_ID varchar(50) NOT NULL,
	PRIMARY KEY (Ord_ID)
	)

TRUNCATE TABLE dbo.orders_dimen;

BULK INSERT dbo.orders_dimen
FROM 'C:\Users\Kayaharman\Desktop\kayaharman\LECTURES\Data Science\SQL\E-COMMERCE PROJECT\Orders_Dimen.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2
)
GO

SELECT *
FROM dbo.orders_dimen

CREATE TABLE prod_dimen(
	Product_Category varchar(50) NOT NULL,
	Product_Sub_Category varchar(50) NOT NULL,
	Prod_ID varchar(50) NOT NULL,
	PRIMARY KEY (Prod_ID)
	)

TRUNCATE TABLE dbo.prod_dimen;

BULK INSERT dbo.prod_dimen
FROM 'C:\Users\Kayaharman\Desktop\kayaharman\LECTURES\Data Science\SQL\E-COMMERCE PROJECT\prod_dimen.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2
)
GO

SELECT *
FROM dbo.prod_dimen

CREATE TABLE market_fact(
	Ord_ID varchar(50) NOT NULL,
	Prod_ID varchar(50) NOT NULL,
	Ship_ID varchar(50) NOT NULL,
	Cust_ID varchar(255) NOT NULL,
	Sales BIGINT NOT NULL,
	Discount FLOAT NOT NULL,
	Order_Quantity TINYINT NOT NULL,
	Product_Base_Margin FLOAT NULL,
	FOREIGN KEY (Ord_ID) REFERENCES orders_dimen(Ord_ID),
	FOREIGN KEY (Prod_ID) REFERENCES prod_dimen(Prod_ID),
	FOREIGN KEY (Ship_ID) REFERENCES shipping_dimen(Ship_ID),
	FOREIGN KEY (Cust_ID) REFERENCES cust_dimen(Cust_ID),
	PRIMARY KEY(Ord_ID, Prod_ID, Ship_ID, Cust_ID)
	)

--DROP TABLE dbo.market_fact

TRUNCATE TABLE dbo.market_fact;

BULK INSERT dbo.market_fact
FROM 'C:\Users\Kayaharman\Desktop\kayaharman\LECTURES\Data Science\SQL\E-COMMERCE PROJECT\Market_Fact.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2,
		KEEPNULLS
)
GO

SELECT *
FROM dbo.market_fact

/* Using the columns of “market_fact”, “cust_dimen”, “orders_dimen”, “prod_dimen”, 
“shipping_dimen”, Create a new table, named as “combined_table”. */

SELECT 
	A.Sales,
	A.Discount,
	A.Order_Quantity,
	A.Product_Base_Margin,
	B.Cust_ID,
	B.Customer_Name,
	B.Customer_Segment,
	B.Province,
	B.Region,
	C.Order_ID,
	C.Ship_ID,
	C.Ship_Date,
	C.Ship_Mode,
	D.Ord_ID,
	D.Order_Date,
	D.Order_Priority,
	E.Prod_ID,
	E.Product_Category,
	E.Product_Sub_Category
INTO combined_table
FROM market_fact A JOIN cust_dimen B ON A.Cust_ID = B.Cust_ID
				   JOIN shipping_dimen C ON A.Ship_ID = C.Ship_ID
				   JOIN orders_dimen D ON A.Ord_ID = D.Ord_ID
				   JOIN prod_dimen E ON A.Prod_ID = E.Prod_ID;

SELECT *
FROM combined_table

--Find the top 3 customers who have the maximum count of orders.

SELECT TOP(3) Customer_Name, COUNT(Order_ID) AS total_order_count
FROM combined_table
GROUP BY Customer_Name
ORDER BY COUNT(Order_ID) DESC

/* Create a new column at combined_table as DaysTakenForShipping that contains the
date difference of Order_Date and Ship_Date. */

SELECT *, DATEDIFF(day, Order_Date, Ship_Date) as DaysTakenForShipping INTO combined_table_2
FROM combined_table

SELECT *
FROM combined_table_2

--Find the customer whose order took the maximum time to get shipping.

SELECT TOP(1) Customer_Name, DaysTakenForShipping
FROM combined_table_2
ORDER BY DaysTakenForShipping DESC

--Count the total number of unique customers in January and how many of 
--them came back every month over the entire year in 2011

--Total number of unique customers in January 2011

SELECT COUNT(DISTINCT Cust_ID) AS unique_customer_jan_2011
FROM combined_table_2
WHERE DATENAME(Month, Order_Date) = 'January' AND YEAR(Order_Date) = '2011'

--How many of them came back every month over the entire year?

SELECT DISTINCT Cust_ID 
FROM combined_table_2
WHERE YEAR(Order_Date) = '2011' AND MONTH(Order_Date) = 4
	AND Cust_ID IN (SELECT DISTINCT Cust_ID 
					FROM combined_table_2
					WHERE YEAR(Order_Date) = '2011' AND MONTH(Order_Date) = 3
						AND Cust_ID IN (SELECT DISTINCT Cust_ID 
										FROM combined_table_2
										WHERE YEAR(Order_Date) = '2011' AND MONTH(Order_Date) = 2
											AND Cust_ID IN (SELECT DISTINCT Cust_ID 
															FROM combined_table_2
															WHERE YEAR(Order_Date) = '2011' AND MONTH(Order_Date) = 1)))



--Write a query to return for each user the time elapsed between the 
--first purchasing and the third purchasing, in ascending order by Customer ID.

;WITH T1 AS(
SELECT DISTINCT Cust_ID, Order_Date,
	FIRST_VALUE(Order_Date) OVER(PARTITION BY Cust_ID ORDER BY Order_Date) AS first_order,
	LEAD(Order_Date, 2) OVER(PARTITION BY Cust_ID ORDER BY Order_Date) AS third_order,
	ROW_NUMBER() OVER(PARTITION BY Cust_ID ORDER BY Order_Date) AS row_number
FROM combined_table_2
)
SELECT *, DATEDIFF(DAY, first_order, third_order) as time_elapsed_day
FROM T1
WHERE row_number = 1 AND third_order IS NOT NULL
ORDER BY Cust_ID


/* Write a query that returns customers who purchased both product 11 and product 14, 
as well as the ratio of these products to the total number of products purchased by 
the customer. */

;WITH T1 AS(
SELECT Customer_Name, Prod_ID,
	COUNT(Prod_ID) OVER(PARTITION BY Customer_Name) AS total_num_of_products,
	COUNT(CASE WHEN Prod_ID = 'Prod_11' THEN 1 END) OVER(PARTITION BY Customer_Name) AS prod_11,
	COUNT(CASE WHEN Prod_ID = 'Prod_14' THEN 1 END) OVER(PARTITION BY Customer_Name) AS prod_14
FROM combined_table_2
)
SELECT DISTINCT Customer_Name, total_num_of_products, prod_11, prod_14,
	ROUND(CAST((prod_11 + prod_14) AS decimal) / total_num_of_products, 3) AS ratio
FROM T1
WHERE prod_11 >= 1 AND prod_14 >= 1

/* Create a “view” that keeps visit logs of customers on a monthly basis. 
(For each log, three field is kept: Cust_id, Year, Month) */

CREATE VIEW visit_logs AS
SELECT Cust_ID, YEAR(Order_Date) AS year, MONTH(Order_Date) AS month
FROM combined_table_2

SELECT *
FROM visit_logs
ORDER BY year, month

/* Create a “view” that keeps the number of monthly visits by users. 
(Show separately all months from the beginning business) */

CREATE VIEW monthly_visits AS
SELECT DISTINCT YEAR(Order_Date) AS year, MONTH(Order_Date) AS month, 
	COUNT(Ord_ID) OVER(PARTITION BY YEAR(Order_Date), MONTH(Order_Date)) total_visits
FROM combined_table_2

SELECT *
FROM monthly_visits
ORDER BY year, month

/* For each visit of customers, create the next month of the visit as a separate column. */

;WITH T1 AS(
SELECT Cust_ID, YEAR(Order_Date) AS year, MONTH(Order_Date) AS month
FROM combined_table_2
)
SELECT *, 
	LEAD(month) OVER(PARTITION BY Cust_ID ORDER BY year, month) AS next_month
FROM T1

--Calculate the monthly time gap between two consecutive visits by each customer.

;WITH T1 AS(
SELECT Cust_ID, YEAR(Order_Date) AS year, MONTH(Order_Date) AS month
FROM combined_table_2
),
T2 AS(
SELECT *, 
	LEAD(month) OVER(PARTITION BY Cust_ID ORDER BY year, month) AS next_month
FROM T1
)
SELECT *,
	CASE WHEN month <= next_month THEN next_month - month ELSE (next_month - month + 12) END AS time_gap
FROM T2
WHERE next_month IS NOT NULL

/* Categorise customers using average time gaps. Choose the most fitted labeling model for you.
For example:
o Labeled as churn if the customer hasn't made another purchase in the months since they made 
their first purchase.
o Labeled as regular if the customer has made a purchase every month. Etc. */

;WITH T1 AS(
SELECT Cust_ID, YEAR(Order_Date) AS year, MONTH(Order_Date) AS month
FROM combined_table_2
),
T2 AS(
SELECT *, 
	LEAD(month) OVER(PARTITION BY Cust_ID ORDER BY year, month) AS next_month
FROM T1
),
T3 AS(
SELECT *,
	CASE WHEN month <= next_month THEN next_month - month ELSE (next_month - month + 12) END AS time_gap
FROM T2
WHERE next_month IS NOT NULL
)
SELECT Cust_ID, AVG(time_gap) AS avg_time_gap,
	CASE 
		WHEN AVG(time_gap) >= 9 THEN 'Rare'
		WHEN AVG(time_gap) >= 6 AND AVG(time_gap) < 9 THEN 'Occasional'
		WHEN AVG(time_gap) >= 3 AND AVG(time_gap) < 6 THEN 'Semi-regular'
		WHEN AVG(time_gap) < 3 THEN 'Regular'
		ELSE 'Churn'
	END AS labels
FROM T3
GROUP BY Cust_ID
ORDER BY avg_time_gap, COUNT(month)

--Find month-by-month customer retention rate since the start of the business.

CREATE TABLE sample_table
(Yearr INT, Monthh INT, retention_rate FLOAT)

TRUNCATE TABLE sample_table

DECLARE @num_of_customers INT
DECLARE @num_of_customers_next INT
DECLARE @num_of_customers_retained INT
DECLARE @month_number INT
DECLARE @year_number INT
DECLARE @retention_rate FLOAT

SET @month_number = 1
SET @year_number = 2009

WHILE @year_number < 2013
BEGIN
	WHILE @month_number < 13
	BEGIN
		
		SELECT @num_of_customers_next = COUNT(DISTINCT Cust_ID)
		FROM combined_table_2
		WHERE YEAR(Order_Date) = @year_number AND MONTH(Order_Date) = @month_number

		SELECT DISTINCT @num_of_customers_retained = COUNT(Cust_ID)
		FROM combined_table_2
		WHERE YEAR(Order_Date) = @year_number AND MONTH(Order_Date) = @month_number
			AND Cust_ID IN (SELECT DISTINCT Cust_ID
							FROM combined_table_2
							WHERE YEAR(Order_Date) = @year_number AND MONTH(Order_Date) = @month_number - 1)
		SET @retention_rate = 1.0 * @num_of_customers_retained / @num_of_customers_next
		INSERT INTO sample_table VALUES(@year_number, @month_number, @retention_rate)
		SET @month_number += 1
	END
	SET @month_number = 1
	SET @year_number += 1
END

SELECT *
FROM sample_table
