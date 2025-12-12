-- Walmart Data Warehouse - Star Schema
-- Abdullah Ali (i232523)

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS Fact_Sales;
DROP TABLE IF EXISTS Dim_Customer;
DROP TABLE IF EXISTS Dim_Product;
DROP TABLE IF EXISTS Dim_Store;
DROP TABLE IF EXISTS Dim_Supplier;
DROP TABLE IF EXISTS Dim_Date;

SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE Dim_Date (
    Date_Key INT PRIMARY KEY,
    Full_Date DATE NOT NULL UNIQUE,
    Day_Of_Week VARCHAR(10),
    Day_Name VARCHAR(10),
    Is_Weekend TINYINT(1),
    Day_Of_Month TINYINT,
    Day_Of_Year SMALLINT,
    Week_Number TINYINT,
    Month TINYINT,
    Month_Name VARCHAR(10),
    Quarter TINYINT,
    Quarter_Label CHAR(3),
    Half_Year CHAR(2),
    Year SMALLINT,
    Season VARCHAR(10),
    Fiscal_Month TINYINT,
    Fiscal_Quarter CHAR(2),
    Fiscal_Year SMALLINT,
    INDEX idx_full_date (Full_Date),
    INDEX idx_year_month (Year, Month),
    INDEX idx_year_quarter (Year, Quarter),
    INDEX idx_is_weekend (Is_Weekend)
);

CREATE TABLE Dim_Customer (
    Customer_Key INT AUTO_INCREMENT PRIMARY KEY,
    Customer_ID INT UNSIGNED NOT NULL UNIQUE,
    Gender VARCHAR(1),
    Age VARCHAR(10),
    Occupation INT,
    Occupation_Bucket VARCHAR(20),
    City_Category VARCHAR(1),
    City_Tier VARCHAR(20),
    Stay_In_Current_City_Years VARCHAR(10),
    Stay_Bucket VARCHAR(10),
    Marital_Status INT,
    Marital_Status_Label VARCHAR(10),
    Loyalty_Segment VARCHAR(20),
    INDEX idx_customer_id (Customer_ID),
    INDEX idx_age (Age),
    INDEX idx_city_tier (City_Tier),
    INDEX idx_loyalty (Loyalty_Segment)
);

CREATE TABLE Dim_Product (
    Product_Key INT AUTO_INCREMENT PRIMARY KEY,
    Product_ID VARCHAR(20) NOT NULL UNIQUE,
    Product_Category VARCHAR(50),
    Unit_Price DECIMAL(10,2),
    Price_Band VARCHAR(20),
    Is_Premium TINYINT(1),
    INDEX idx_product_id (Product_ID),
    INDEX idx_category (Product_Category),
    INDEX idx_price_band (Price_Band)
);

CREATE TABLE Dim_Store (
    Store_Key INT AUTO_INCREMENT PRIMARY KEY,
    Store_ID VARCHAR(20) NOT NULL UNIQUE,
    Store_Name VARCHAR(100),
    Store_Channel VARCHAR(20),
    Store_Tier VARCHAR(20),
    SKU_Count INT,
    Category_Count INT,
    Avg_List_Price DECIMAL(10,2),
    Is_Flagship TINYINT(1),
    Is_Active TINYINT(1),
    INDEX idx_store_id (Store_ID),
    INDEX idx_store_tier (Store_Tier),
    INDEX idx_is_flagship (Is_Flagship)
);

CREATE TABLE Dim_Supplier (
    Supplier_Key INT AUTO_INCREMENT PRIMARY KEY,
    Supplier_ID VARCHAR(20) NOT NULL UNIQUE,
    Supplier_Name VARCHAR(100),
    Supplier_Tier VARCHAR(20),
    Primary_Category VARCHAR(50),
    SKU_Count INT,
    Avg_List_Price DECIMAL(10,2),
    Reliability_Score DECIMAL(5,2),
    INDEX idx_supplier_id (Supplier_ID),
    INDEX idx_supplier_tier (Supplier_Tier),
    INDEX idx_primary_category (Primary_Category)
);

CREATE TABLE Fact_Sales (
    Sales_Key BIGINT AUTO_INCREMENT PRIMARY KEY,
    Order_ID BIGINT UNSIGNED NOT NULL,
    Order_Line_Number SMALLINT DEFAULT 1,
    
    Customer_Key INT NOT NULL,
    Product_Key INT NOT NULL,
    Store_Key INT NOT NULL,
    Supplier_Key INT NOT NULL,
    Date_Key INT NOT NULL,
    
    Quantity INT NOT NULL CHECK (Quantity > 0),
    Unit_Price DECIMAL(10,2) NOT NULL,
    Total_Purchase_Amount DECIMAL(12,2) NOT NULL,
    Discount_Amount DECIMAL(12,2) DEFAULT 0,
    Net_Sales_Amount DECIMAL(12,2) AS (Total_Purchase_Amount - Discount_Amount) STORED,
    Weekend_Flag TINYINT(1),
    Order_Channel VARCHAR(20) DEFAULT 'In-Store',
    Created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_customer
        FOREIGN KEY (Customer_Key)
        REFERENCES Dim_Customer(Customer_Key)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_product
        FOREIGN KEY (Product_Key)
        REFERENCES Dim_Product(Product_Key)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_store
        FOREIGN KEY (Store_Key)
        REFERENCES Dim_Store(Store_Key)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_supplier
        FOREIGN KEY (Supplier_Key)
        REFERENCES Dim_Supplier(Supplier_Key)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_date
        FOREIGN KEY (Date_Key)
        REFERENCES Dim_Date(Date_Key)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    UNIQUE uq_order_line (Order_ID, Product_Key, Date_Key),
    INDEX idx_fact_customer (Customer_Key),
    INDEX idx_fact_product (Product_Key),
    INDEX idx_fact_store (Store_Key),
    INDEX idx_fact_supplier (Supplier_Key),
    INDEX idx_fact_date (Date_Key),
    INDEX idx_fact_weekend (Weekend_Flag),
    INDEX idx_fact_order (Order_ID),
    INDEX idx_fact_store_supplier (Store_Key, Supplier_Key),
    INDEX idx_fact_date_product (Date_Key, Product_Key)
);
