-- Master Data Tables for HYBRIDJOIN Algorithm
-- Abdullah Ali (i232523)

DROP TABLE IF EXISTS Master_Customer;
DROP TABLE IF EXISTS Master_Product;

CREATE TABLE Master_Customer (
    Customer_ID INT UNSIGNED PRIMARY KEY,
    Gender VARCHAR(1),
    Age VARCHAR(10),
    Occupation INT,
    City_Category VARCHAR(1),
    Stay_In_Current_City_Years VARCHAR(10),
    Marital_Status INT,
    INDEX idx_customer_id_sorted (Customer_ID)
);

CREATE TABLE Master_Product (
    Product_ID VARCHAR(20) PRIMARY KEY,
    Product_Category VARCHAR(50),
    Price DECIMAL(10, 2),
    StoreID VARCHAR(20),
    StoreName VARCHAR(100),
    SupplierID VARCHAR(20),
    SupplierName VARCHAR(100),
    INDEX idx_product_id_sorted (Product_ID)
);
