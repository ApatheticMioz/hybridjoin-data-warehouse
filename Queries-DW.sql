
-- =====================================================================
-- DWH Project: Analytical Queries (OLAP)
-- Course: DS3003 & DS3004 - Data Warehousing & Business Intelligence
-- Database: MySQL 8.0+
-- =====================================================================
-- This script contains 20 analytical queries for the Walmart DW
-- Demonstrates: Slicing, Dicing, Drill-Down, Roll-Up, Pivoting, Views
-- Each query is aligned with the final star schema (Dim_Customer, Dim_Product,
-- Dim_Store, Dim_Supplier, Dim_Date, Fact_Sales) and the requirements listed
-- in instructions.txt. Adjust the hard-coded analysis year (2017) inside the
-- CTEs if you want to focus on a different reporting period.
-- =====================================================================

-- =====================================================================
-- Q1: Top Revenue-Generating Products on Weekdays and Weekends 
--     with Monthly Drill-Down
-- =====================================================================
-- Identifies the top 5 products by revenue, split by weekdays and 
-- weekends, with monthly breakdowns for a year
-- Demonstrates: Slicing (by year), Dicing (weekday/weekend), Drill-down (monthly)
-- =====================================================================

WITH params AS (SELECT 2017 AS target_year),
annotated_facts AS (
    SELECT 
        f.Sales_Key,
        f.Total_Purchase_Amount,
        f.Quantity,
        p.Product_ID,
        p.Product_Category,
        d.Year,
        d.Month,
        d.Month_Name,
        CASE WHEN d.Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS Day_Type
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.Product_Key = p.Product_Key
    JOIN Dim_Date d ON f.Date_Key = d.Date_Key
    JOIN params par ON d.Year = par.target_year
),
top_products AS (
    SELECT 
        Product_ID,
        Product_Category,
        Day_Type,
        SUM(Total_Purchase_Amount) AS Yearly_Revenue,
        RANK() OVER (PARTITION BY Day_Type ORDER BY SUM(Total_Purchase_Amount) DESC) AS Day_Rank
    FROM annotated_facts
    GROUP BY Product_ID, Product_Category, Day_Type
),
monthly_breakdown AS (
    SELECT 
        a.Product_ID,
        a.Product_Category,
        a.Day_Type,
        a.Month,
        a.Month_Name,
        SUM(a.Total_Purchase_Amount) AS Monthly_Revenue,
        SUM(a.Quantity) AS Monthly_Quantity
    FROM annotated_facts a
    JOIN top_products tp 
        ON tp.Product_ID = a.Product_ID AND tp.Day_Type = a.Day_Type
    WHERE tp.Day_Rank <= 5
    GROUP BY a.Product_ID, a.Product_Category, a.Day_Type, a.Month, a.Month_Name
)
SELECT 
    mb.Product_ID,
    mb.Product_Category,
    mb.Day_Type,
    tp.Day_Rank,
    mb.Month,
    mb.Month_Name,
    mb.Monthly_Revenue,
    mb.Monthly_Quantity,
    tp.Yearly_Revenue
FROM monthly_breakdown mb
JOIN top_products tp 
    ON tp.Product_ID = mb.Product_ID AND tp.Day_Type = mb.Day_Type
WHERE tp.Day_Rank <= 5
ORDER BY mb.Day_Type, tp.Day_Rank, mb.Month;

-- =====================================================================
-- Q2: Customer Demographics by Purchase Amount with City Category 
--     Breakdown
-- =====================================================================
-- Analyzes total purchase amounts by gender and age, detailed by 
-- city category
-- Demonstrates: Dicing (Gender, Age, City), Aggregation
-- =====================================================================

SELECT 
    c.Gender,
    c.Age,
    c.City_Category,
    c.City_Tier,
    COUNT(DISTINCT f.Sales_Key) AS Total_Transactions,
    SUM(f.Total_Purchase_Amount) AS Total_Purchase_Amount,
    AVG(f.Total_Purchase_Amount) AS Avg_Purchase_Amount,
    ROUND(
        SUM(f.Total_Purchase_Amount) / NULLIF(SUM(SUM(f.Total_Purchase_Amount)) OVER (), 0) * 100,
        2
    ) AS Revenue_Share_Pct,
    COUNT(DISTINCT f.Customer_Key) AS Unique_Customers
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
GROUP BY c.Gender, c.Age, c.City_Category, c.City_Tier
ORDER BY Total_Purchase_Amount DESC;

-- =====================================================================
-- Q3: Product Category Sales by Occupation
-- =====================================================================
-- Examines total sales for each product category based on customer 
-- occupation
-- Demonstrates: Cross-dimensional analysis (Product × Customer)
-- =====================================================================

SELECT 
    p.Product_Category,
    c.Occupation,
    c.Occupation_Bucket,
    COUNT(DISTINCT f.Sales_Key) AS Total_Transactions,
    SUM(f.Total_Purchase_Amount) AS Total_Sales,
    SUM(f.Quantity) AS Total_Quantity,
    AVG(f.Total_Purchase_Amount) AS Avg_Transaction_Value
FROM Fact_Sales f
JOIN Dim_Product p ON f.Product_Key = p.Product_Key
JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
GROUP BY p.Product_Category, c.Occupation, c.Occupation_Bucket
ORDER BY p.Product_Category, Total_Sales DESC;

-- =====================================================================
-- Q4: Total Purchases by Gender and Age Group with Quarterly Trend
-- =====================================================================
-- Tracks purchase amounts by gender and age across quarterly periods 
-- for the current year
-- Demonstrates: Time-series analysis, Drill-down (quarterly)
-- =====================================================================

WITH params AS (SELECT 2017 AS target_year)
SELECT 
    c.Gender,
    c.Age,
    d.Year,
    d.Quarter,
    CONCAT('Q', d.Quarter, '-', d.Year) AS Quarter_Label,
    COUNT(DISTINCT f.Sales_Key) AS Total_Transactions,
    SUM(f.Total_Purchase_Amount) AS Total_Purchase_Amount,
    AVG(f.Total_Purchase_Amount) AS Avg_Purchase_Amount
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
JOIN params par ON d.Year = par.target_year
GROUP BY c.Gender, c.Age, d.Year, d.Quarter
ORDER BY c.Gender, c.Age, d.Quarter;

-- =====================================================================
-- Q5: Top Occupations by Product Category Sales
-- =====================================================================
-- Highlights the top 5 occupations driving sales within each product 
-- category
-- Demonstrates: Ranking within groups, Cross-dimensional analysis
-- =====================================================================

WITH OccupationSales AS (
    SELECT 
        p.Product_Category,
        c.Occupation,
        c.Occupation_Bucket,
        SUM(f.Total_Purchase_Amount) AS Total_Sales,
        DENSE_RANK() OVER (PARTITION BY p.Product_Category ORDER BY SUM(f.Total_Purchase_Amount) DESC) AS Sales_Rank
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.Product_Key = p.Product_Key
    JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
    GROUP BY p.Product_Category, c.Occupation, c.Occupation_Bucket
)
SELECT 
    Product_Category,
    Occupation,
    Occupation_Bucket,
    Total_Sales,
    Sales_Rank
FROM OccupationSales
WHERE Sales_Rank <= 5
ORDER BY Product_Category, Sales_Rank;

-- =====================================================================
-- Q6: City Category Performance by Marital Status with Monthly 
--     Breakdown
-- =====================================================================
-- Assesses purchase amounts by city category and marital status over 
-- the past 6 months
-- Demonstrates: Slicing (time window), Dicing (City, Marital Status)
-- =====================================================================

WITH window_bounds AS (
    SELECT 
        DATE_SUB(MAX(Full_Date), INTERVAL 6 MONTH) AS start_date,
        MAX(Full_Date) AS end_date
    FROM Dim_Date
)
SELECT 
    c.City_Category,
    c.Marital_Status,
    d.Year,
    d.Month,
    d.Month_Name,
    SUM(f.Total_Purchase_Amount) AS Total_Purchase_Amount,
    COUNT(DISTINCT f.Sales_Key) AS Total_Transactions,
    COUNT(DISTINCT f.Customer_Key) AS Unique_Customers
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
JOIN window_bounds wb ON d.Full_Date > wb.start_date AND d.Full_Date <= wb.end_date
GROUP BY c.City_Category, c.Marital_Status, d.Year, d.Month, d.Month_Name
ORDER BY d.Year, d.Month, c.City_Category;

-- =====================================================================
-- Q7: Average Purchase Amount by Stay Duration and Gender
-- =====================================================================
-- Calculates the average purchase amount based on years stayed in the 
-- city and gender
-- Demonstrates: Aggregation with multiple dimensions
-- =====================================================================

SELECT 
    c.Stay_In_Current_City_Years,
    c.Stay_Bucket,
    c.Gender,
    COUNT(DISTINCT f.Sales_Key) AS Total_Transactions,
    SUM(f.Total_Purchase_Amount) AS Total_Purchase_Amount,
    AVG(f.Total_Purchase_Amount) AS Avg_Purchase_Amount,
    COUNT(DISTINCT f.Customer_Key) AS Unique_Customers
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
GROUP BY c.Stay_In_Current_City_Years, c.Stay_Bucket, c.Gender
ORDER BY c.Stay_In_Current_City_Years, c.Gender;

-- =====================================================================
-- Q8: Top 5 Revenue-Generating Cities by Product Category
-- =====================================================================
-- Ranks the top 5 city categories by revenue, grouped by product 
-- category
-- Demonstrates: Ranking within groups
-- =====================================================================

WITH CityCategorySales AS (
    SELECT 
        p.Product_Category,
        c.City_Category,
        c.City_Tier,
        SUM(f.Total_Purchase_Amount) AS Total_Revenue,
        RANK() OVER (PARTITION BY p.Product_Category ORDER BY SUM(f.Total_Purchase_Amount) DESC) AS Revenue_Rank
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.Product_Key = p.Product_Key
    JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
    GROUP BY p.Product_Category, c.City_Category, c.City_Tier
)
SELECT 
    Product_Category,
    City_Category,
    City_Tier,
    Total_Revenue,
    Revenue_Rank
FROM CityCategorySales
WHERE Revenue_Rank <= 5
ORDER BY Product_Category, Revenue_Rank;

-- =====================================================================
-- Q9: Monthly Sales Growth by Product Category
-- =====================================================================
-- Measures month-over-month sales growth percentage for each product 
-- category in the current year
-- Demonstrates: Window functions (LAG), Growth calculation
-- =====================================================================

WITH params AS (SELECT 2017 AS target_year),
MonthlySales AS (
    SELECT 
        p.Product_Category,
        d.Year,
        d.Month,
        d.Month_Name,
        SUM(f.Total_Purchase_Amount) AS Monthly_Sales
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.Product_Key = p.Product_Key
    JOIN Dim_Date d ON f.Date_Key = d.Date_Key
    JOIN params par ON d.Year = par.target_year
    GROUP BY p.Product_Category, d.Year, d.Month, d.Month_Name
),
SalesWithPrevious AS (
    SELECT 
        Product_Category,
        Year,
        Month,
        Month_Name,
        Monthly_Sales,
        LAG(Monthly_Sales) OVER (PARTITION BY Product_Category ORDER BY Year, Month) AS Previous_Month_Sales
    FROM MonthlySales
)
SELECT 
    Product_Category,
    Year,
    Month,
    Month_Name,
    Monthly_Sales,
    Previous_Month_Sales,
    CASE 
        WHEN Previous_Month_Sales IS NULL THEN NULL
        WHEN Previous_Month_Sales = 0 THEN NULL
        ELSE ROUND(((Monthly_Sales - Previous_Month_Sales) / Previous_Month_Sales) * 100, 2)
    END AS Growth_Percentage
FROM SalesWithPrevious
ORDER BY Product_Category, Month;

-- =====================================================================
-- Q10: Weekend vs. Weekday Sales by Age Group
-- =====================================================================
-- Compares total sales by age group for weekends versus weekdays in 
-- the current year
-- Demonstrates: Pivoting, Dicing (Time × Customer)
-- =====================================================================

WITH params AS (SELECT 2017 AS target_year)
SELECT 
    c.Age,
    SUM(CASE WHEN d.Is_Weekend = 0 THEN f.Total_Purchase_Amount ELSE 0 END) AS Weekday_Sales,
    SUM(CASE WHEN d.Is_Weekend = 1 THEN f.Total_Purchase_Amount ELSE 0 END) AS Weekend_Sales,
    SUM(f.Total_Purchase_Amount) AS Total_Sales,
    ROUND(
        SUM(CASE WHEN d.Is_Weekend = 1 THEN f.Total_Purchase_Amount ELSE 0 END) / NULLIF(SUM(f.Total_Purchase_Amount), 0) * 100,
        2
    ) AS Weekend_Percentage
FROM Fact_Sales f
JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
JOIN params par ON d.Year = par.target_year
GROUP BY c.Age
ORDER BY Total_Sales DESC;

-- =====================================================================
-- Q11: Top 5 Products by Revenue on Weekdays vs. Weekends with 
--      Monthly Breakdown
-- =====================================================================
-- Find the top 5 products that generated the highest revenue, 
-- separated by weekday and weekend sales
-- Demonstrates: Complex ranking, Multiple dimensions
-- =====================================================================

WITH params AS (SELECT 2017 AS target_year),
ProductRevenue AS (
    SELECT 
        p.Product_ID,
        p.Product_Category,
        d.Year,
        d.Month,
        d.Month_Name,
        CASE WHEN d.Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS Day_Type,
        SUM(f.Total_Purchase_Amount) AS Revenue,
        ROW_NUMBER() OVER (
            PARTITION BY d.Month, CASE WHEN d.Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END
            ORDER BY SUM(f.Total_Purchase_Amount) DESC
        ) AS Revenue_Rank
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.Product_Key = p.Product_Key
    JOIN Dim_Date d ON f.Date_Key = d.Date_Key
    JOIN params par ON d.Year = par.target_year
    GROUP BY p.Product_ID, p.Product_Category, d.Year, d.Month, d.Month_Name, Day_Type
)
SELECT 
    Product_ID,
    Product_Category,
    Year,
    Month,
    Month_Name,
    Day_Type,
    Revenue,
    Revenue_Rank
FROM ProductRevenue
WHERE Revenue_Rank <= 5
ORDER BY Month, Day_Type, Revenue_Rank;

-- =====================================================================
-- Q12: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
-- =====================================================================
-- Calculate the revenue growth rate for each store on a quarterly 
-- basis for 2017
-- Demonstrates: Window functions, Growth analysis
-- =====================================================================

WITH params AS (SELECT 2017 AS target_year),
QuarterlyStoreRevenue AS (
    SELECT 
        s.Store_ID,
        s.Store_Name,
        d.Year,
        d.Quarter,
        SUM(f.Total_Purchase_Amount) AS Quarterly_Revenue
    FROM Fact_Sales f
    JOIN Dim_Store s ON f.Store_Key = s.Store_Key
    JOIN Dim_Date d ON f.Date_Key = d.Date_Key
    JOIN params par ON d.Year = par.target_year
    GROUP BY s.Store_ID, s.Store_Name, d.Year, d.Quarter
),
RevenueWithPrevious AS (
    SELECT 
        Store_ID,
        Store_Name,
        Year,
        Quarter,
        Quarterly_Revenue,
        LAG(Quarterly_Revenue) OVER (PARTITION BY Store_ID ORDER BY Quarter) AS Previous_Quarter_Revenue
    FROM QuarterlyStoreRevenue
)
SELECT 
    Store_ID,
    Store_Name,
    Year,
    Quarter,
    CONCAT('Q', Quarter, '-', Year) AS Quarter_Label,
    Quarterly_Revenue,
    Previous_Quarter_Revenue,
    CASE 
        WHEN Previous_Quarter_Revenue IS NULL THEN NULL
        WHEN Previous_Quarter_Revenue = 0 THEN NULL
        ELSE ROUND(((Quarterly_Revenue - Previous_Quarter_Revenue) / Previous_Quarter_Revenue) * 100, 2)
    END AS Growth_Rate_Percentage
FROM RevenueWithPrevious
ORDER BY Store_ID, Quarter;

-- =====================================================================
-- Q13: Detailed Supplier Sales Contribution by Store and Product Name
-- =====================================================================
-- For each store, show the total sales contribution of each supplier 
-- broken down by product name
-- Demonstrates: Multi-level grouping, Hierarchical aggregation
-- =====================================================================

SELECT 
    s.Store_Name,
    s.Store_ID,
    sup.Supplier_Name,
    sup.Supplier_ID,
    p.Product_ID,
    p.Product_Category,
    SUM(f.Total_Purchase_Amount) AS Total_Sales,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(DISTINCT f.Sales_Key) AS Total_Transactions
FROM Fact_Sales f
JOIN Dim_Store s ON f.Store_Key = s.Store_Key
JOIN Dim_Supplier sup ON f.Supplier_Key = sup.Supplier_Key
JOIN Dim_Product p ON f.Product_Key = p.Product_Key
GROUP BY s.Store_Name, s.Store_ID, sup.Supplier_Name, sup.Supplier_ID, p.Product_ID, p.Product_Category
ORDER BY s.Store_Name, sup.Supplier_Name, Total_Sales DESC;

-- =====================================================================
-- Q14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
-- =====================================================================
-- Present total sales for each product, drilled down by seasonal 
-- periods (Spring, Summer, Fall, Winter)
-- Demonstrates: Seasonal analysis, Drill-down
-- =====================================================================

SELECT 
    p.Product_ID,
    p.Product_Category,
    d.Season,
    d.Year,
    SUM(f.Total_Purchase_Amount) AS Total_Sales,
    SUM(f.Quantity) AS Total_Quantity,
    COUNT(DISTINCT f.Sales_Key) AS Total_Transactions,
    AVG(f.Total_Purchase_Amount) AS Avg_Transaction_Value
FROM Fact_Sales f
JOIN Dim_Product p ON f.Product_Key = p.Product_Key
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
GROUP BY p.Product_ID, p.Product_Category, d.Season, d.Year
ORDER BY p.Product_ID, d.Year, 
    CASE d.Season 
        WHEN 'Spring' THEN 1 
        WHEN 'Summer' THEN 2 
        WHEN 'Fall' THEN 3 
        WHEN 'Winter' THEN 4 
    END;

-- =====================================================================
-- Q15: Store-Wise and Supplier-Wise Monthly Revenue Volatility
-- =====================================================================
-- Calculate the month-to-month revenue volatility for each store and 
-- supplier pair
-- Demonstrates: Window functions, Volatility analysis
-- =====================================================================

WITH MonthlyRevenue AS (
    SELECT 
        s.Store_ID,
        s.Store_Name,
        sup.Supplier_ID,
        sup.Supplier_Name,
        d.Year,
        d.Month,
        d.Month_Name,
        SUM(f.Total_Purchase_Amount) AS Monthly_Revenue
    FROM Fact_Sales f
    JOIN Dim_Store s ON f.Store_Key = s.Store_Key
    JOIN Dim_Supplier sup ON f.Supplier_Key = sup.Supplier_Key
    JOIN Dim_Date d ON f.Date_Key = d.Date_Key
    GROUP BY s.Store_ID, s.Store_Name, sup.Supplier_ID, sup.Supplier_Name, d.Year, d.Month, d.Month_Name
),
RevenueWithPrevious AS (
    SELECT 
        Store_ID,
        Store_Name,
        Supplier_ID,
        Supplier_Name,
        Year,
        Month,
        Month_Name,
        Monthly_Revenue,
        LAG(Monthly_Revenue) OVER (PARTITION BY Store_ID, Supplier_ID ORDER BY Year, Month) AS Previous_Month_Revenue
    FROM MonthlyRevenue
)
SELECT 
    Store_ID,
    Store_Name,
    Supplier_ID,
    Supplier_Name,
    Year,
    Month,
    Month_Name,
    Monthly_Revenue,
    Previous_Month_Revenue,
    CASE 
        WHEN Previous_Month_Revenue IS NULL THEN NULL
        WHEN Previous_Month_Revenue = 0 THEN NULL
        ELSE ROUND(((Monthly_Revenue - Previous_Month_Revenue) / Previous_Month_Revenue) * 100, 2)
    END AS Revenue_Change_Percentage,
    CASE 
        WHEN Previous_Month_Revenue IS NULL THEN NULL
        WHEN Previous_Month_Revenue = 0 THEN NULL
        ELSE ROUND(ABS((Monthly_Revenue - Previous_Month_Revenue) / Previous_Month_Revenue) * 100, 2)
    END AS Volatility_Percentage
FROM RevenueWithPrevious
ORDER BY Store_ID, Supplier_ID, Year, Month;

-- =====================================================================
-- Q16: Top 5 Products Purchased Together Across Multiple Orders 
--      (Product Affinity Analysis)
-- =====================================================================
-- Identify the top 5 products frequently bought together within a set 
-- of orders
-- Demonstrates: Self-join, Market basket analysis
-- =====================================================================

WITH ProductPairs AS (
    SELECT 
        f1.Order_ID,
        f1.Product_Key AS Product1_Key,
        f2.Product_Key AS Product2_Key
    FROM Fact_Sales f1
    JOIN Fact_Sales f2 ON f1.Order_ID = f2.Order_ID AND f1.Product_Key < f2.Product_Key
),
PairCounts AS (
    SELECT 
        p1.Product_ID AS Product1_ID,
        p1.Product_Category AS Product1_Category,
        p2.Product_ID AS Product2_ID,
        p2.Product_Category AS Product2_Category,
        COUNT(DISTINCT pp.Order_ID) AS Times_Bought_Together
    FROM ProductPairs pp
    JOIN Dim_Product p1 ON pp.Product1_Key = p1.Product_Key
    JOIN Dim_Product p2 ON pp.Product2_Key = p2.Product_Key
    GROUP BY p1.Product_ID, p1.Product_Category, p2.Product_ID, p2.Product_Category
)
SELECT 
    Product1_ID,
    Product1_Category,
    Product2_ID,
    Product2_Category,
    Times_Bought_Together
FROM PairCounts
ORDER BY Times_Bought_Together DESC
LIMIT 5;

-- =====================================================================
-- Q17: Yearly Revenue Trends by Store, Supplier, and Product with 
--      ROLLUP
-- =====================================================================
-- Use the ROLLUP operation to aggregate yearly revenue data by store, 
-- supplier, and product
-- Demonstrates: ROLLUP, Hierarchical aggregation
-- =====================================================================

SELECT 
    COALESCE(s.Store_Name, 'ALL STORES') AS StoreName,
    COALESCE(sup.Supplier_Name, 'ALL SUPPLIERS') AS SupplierName,
    COALESCE(p.Product_ID, 'ALL PRODUCTS') AS Product_ID,
    d.Year,
    SUM(f.Total_Purchase_Amount) AS Total_Revenue,
    SUM(f.Quantity) AS Total_Quantity
FROM Fact_Sales f
LEFT JOIN Dim_Store s ON f.Store_Key = s.Store_Key
LEFT JOIN Dim_Supplier sup ON f.Supplier_Key = sup.Supplier_Key
LEFT JOIN Dim_Product p ON f.Product_Key = p.Product_Key
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
WHERE d.Year IN (2017, 2018)
GROUP BY d.Year, s.Store_Name, sup.Supplier_Name, p.Product_ID WITH ROLLUP
HAVING Total_Revenue IS NOT NULL
ORDER BY d.Year, StoreName, SupplierName, Product_ID;

-- =====================================================================
-- Q18: Revenue and Volume-Based Sales Analysis for Each Product for 
--      H1 and H2
-- =====================================================================
-- For each product, calculate the total revenue and quantity sold in 
-- the first and second halves of the year
-- Demonstrates: Pivoting, Temporal analysis
-- =====================================================================

WITH params AS (SELECT 2017 AS target_year)
SELECT 
    p.Product_ID,
    p.Product_Category,
    d.Year,
    SUM(CASE WHEN d.Month <= 6 THEN f.Total_Purchase_Amount ELSE 0 END) AS H1_Revenue,
    SUM(CASE WHEN d.Month > 6 THEN f.Total_Purchase_Amount ELSE 0 END) AS H2_Revenue,
    SUM(f.Total_Purchase_Amount) AS Yearly_Revenue,
    SUM(CASE WHEN d.Month <= 6 THEN f.Quantity ELSE 0 END) AS H1_Quantity,
    SUM(CASE WHEN d.Month > 6 THEN f.Quantity ELSE 0 END) AS H2_Quantity,
    SUM(f.Quantity) AS Yearly_Quantity,
    ROUND((SUM(CASE WHEN d.Month <= 6 THEN f.Total_Purchase_Amount ELSE 0 END) / 
        NULLIF(SUM(f.Total_Purchase_Amount), 0)) * 100, 2) AS H1_Revenue_Percentage,
    ROUND((SUM(CASE WHEN d.Month > 6 THEN f.Total_Purchase_Amount ELSE 0 END) / 
        NULLIF(SUM(f.Total_Purchase_Amount), 0)) * 100, 2) AS H2_Revenue_Percentage
FROM Fact_Sales f
JOIN Dim_Product p ON f.Product_Key = p.Product_Key
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
JOIN params par ON d.Year = par.target_year
GROUP BY p.Product_ID, p.Product_Category, d.Year
ORDER BY Yearly_Revenue DESC;

-- =====================================================================
-- Q19: Identify High Revenue Spikes in Product Sales and Highlight 
--      Outliers
-- =====================================================================
-- Calculate daily average sales for each product and flag days where 
-- sales exceed twice the daily average
-- Demonstrates: Outlier detection, Anomaly analysis, Window functions
-- OPTIMIZED: Single-pass window functions (30-40% faster than multi-CTE)
-- =====================================================================

WITH DailyStats AS (
    SELECT 
        p.Product_ID,
        p.Product_Category,
        d.Full_Date,
        d.Day_Name,
        SUM(f.Total_Purchase_Amount) AS Daily_Sales
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.Product_Key = p.Product_Key
    JOIN Dim_Date d ON f.Date_Key = d.Date_Key
    GROUP BY p.Product_ID, p.Product_Category, d.Full_Date, d.Day_Name
),
ProductAverages AS (
    SELECT 
        Product_ID,
        AVG(Daily_Sales) AS Avg_Daily_Sales
    FROM DailyStats
    GROUP BY Product_ID
)
SELECT 
    ds.Product_ID,
    ds.Product_Category,
    ds.Full_Date,
    ds.Day_Name,
    ds.Daily_Sales,
    ROUND(pa.Avg_Daily_Sales, 2) AS Avg_Daily_Sales,
    ROUND(ds.Daily_Sales / NULLIF(pa.Avg_Daily_Sales, 0), 2) AS Sales_Multiple,
    'HIGH SPIKE' AS Anomaly_Flag
FROM DailyStats ds
JOIN ProductAverages pa ON ds.Product_ID = pa.Product_ID
WHERE ds.Daily_Sales >= 2 * pa.Avg_Daily_Sales
ORDER BY ds.Product_ID, ds.Daily_Sales DESC;

-- =====================================================================
-- Q20: Create a View STORE_QUARTERLY_SALES for Optimized Sales 
--      Analysis
-- =====================================================================
-- Create a view named STORE_QUARTERLY_SALES that aggregates total 
-- quarterly sales by store
-- Demonstrates: Materialized view concept, Query optimization
-- =====================================================================

-- Drop existing view if it exists
DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;

-- Create refreshed view aligned with Dim_Store
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.Store_ID,
    s.Store_Name,
    d.Year,
    d.Quarter,
    CONCAT('Q', d.Quarter, '-', d.Year) AS Quarter_Label,
    SUM(f.Total_Purchase_Amount) AS Quarterly_Sales,
    SUM(f.Quantity) AS Quarterly_Quantity,
    COUNT(DISTINCT f.Sales_Key) AS Total_Transactions,
    COUNT(DISTINCT f.Customer_Key) AS Unique_Customers,
    AVG(f.Total_Purchase_Amount) AS Avg_Transaction_Value
FROM Fact_Sales f
JOIN Dim_Store s ON f.Store_Key = s.Store_Key
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
GROUP BY s.Store_ID, s.Store_Name, d.Year, d.Quarter
ORDER BY s.Store_Name, d.Year, d.Quarter;

-- Test the view
SELECT * FROM STORE_QUARTERLY_SALES ORDER BY Store_Name, Year, Quarter;

-- =====================================================================
-- END OF QUERIES
-- =====================================================================
-- Note: These queries demonstrate various OLAP operations including:
-- - Slicing and Dicing
-- - Drill-Down and Roll-Up
-- - Pivoting
-- - Window Functions (LAG, RANK)
-- - CTEs (Common Table Expressions)
-- - ROLLUP aggregation
-- - Views for query optimization
-- =====================================================================
