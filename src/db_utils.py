# Database utilities for HYBRIDJOIN ETL

import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter


def _city_tier(city_code):
    mapping = {'A': 'Metro', 'B': 'Urban', 'C': 'Town'}
    if city_code is None:
        return 'Unknown'
    return mapping.get(str(city_code).upper(), 'Unknown')


def _stay_numeric(value):
    if value is None:
        return 0
    text = str(value).replace('+', '')
    try:
        return int(text)
    except ValueError:
        return 0


def _stay_bucket(value):
    stay = _stay_numeric(value)
    if stay >= 4:
        return '4+ yrs'
    if stay == 3:
        return '3 yrs'
    if stay == 2:
        return '2 yrs'
    if stay == 1:
        return '1 yr'
    return '0 yrs'


def _loyalty_segment(stay_value, marital_status):
    stay = _stay_numeric(stay_value)
    if stay >= 4:
        return 'Advocate'
    if stay >= 2:
        return 'Loyal'
    return 'Newcomer' if marital_status == 0 else 'Emerging Family'


def _occupation_bucket(occupation_code):
    if occupation_code >= 15:
        return 'Executive'
    if occupation_code >= 10:
        return 'Professional'
    if occupation_code >= 5:
        return 'Skilled'
    return 'Entry'


def _price_band(price):
    if price is None:
        return 'Unknown'
    if price >= 70:
        return 'Premium'
    if price >= 40:
        return 'Core'
    return 'Value'


def _store_tier(sku_count):
    if sku_count >= 750:
        return 'Mega'
    if sku_count >= 350:
        return 'Large'
    return 'Compact'


def _supplier_tier(sku_count):
    if sku_count >= 600:
        return 'Strategic'
    if sku_count >= 250:
        return 'Core'
    return 'Long Tail'

def get_database_connection(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            autocommit=False
        )
        if connection.is_connected():
            print(f"Connected to '{database}'")
            return connection
    except Error as e:
        print(f"Error connecting: {e}")
        raise

def populate_dim_date(connection, start_year=2015, end_year=2020):
    """Populate Dim_Date with Gregorian and fiscal calendar attributes."""
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM Dim_Date")
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"Dim_Date already has {count} records")
            return

        print(f"Populating Dim_Date for years {start_year}-{end_year}...")

        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)

        current_date = start_date
        batch_size = 500
        batch = []

        while current_date <= end_date:
            date_key = int(current_date.strftime('%Y%m%d'))
            day_of_week = current_date.strftime('%A')
            is_weekend = 1 if current_date.weekday() >= 5 else 0
            day_of_month = current_date.day
            day_of_year = current_date.timetuple().tm_yday
            week_number = int(current_date.strftime('%V'))
            month_number = current_date.month
            month_name = current_date.strftime('%B')
            quarter_number = (month_number - 1) // 3 + 1
            quarter_label = f"Q{quarter_number}"
            half_year = 'H1' if month_number <= 6 else 'H2'
            year = current_date.year

            if month_number in [12, 1, 2]:
                season = 'Winter'
            elif month_number in [3, 4, 5]:
                season = 'Spring'
            elif month_number in [6, 7, 8]:
                season = 'Summer'
            else:
                season = 'Fall'

            fiscal_year = year if month_number >= 2 else year - 1
            fiscal_month = ((month_number + 10) % 12) + 1  # Feb -> 1, Jan -> 12
            fiscal_quarter = f"F{((fiscal_month - 1) // 3) + 1}"

            batch.append((
                date_key,
                current_date,
                day_of_week,
                day_of_week,
                is_weekend,
                day_of_month,
                day_of_year,
                week_number,
                month_number,
                month_name,
                quarter_number,
                quarter_label,
                half_year,
                year,
                season,
                fiscal_month,
                fiscal_quarter,
                fiscal_year
            ))

            if len(batch) >= batch_size:
                insert_query = """
                    INSERT INTO Dim_Date 
                    (Date_Key, Full_Date, Day_Of_Week, Day_Name, Is_Weekend, Day_Of_Month,
                     Day_Of_Year, Week_Number, Month, Month_Name, Quarter,
                     Quarter_Label, Half_Year, Year, Season, Fiscal_Month, Fiscal_Quarter, Fiscal_Year)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_query, batch)
                connection.commit()
                batch = []

            current_date += timedelta(days=1)

        if batch:
            insert_query = """
                INSERT INTO Dim_Date 
                (Date_Key, Full_Date, Day_Of_Week, Day_Name, Is_Weekend, Day_Of_Month,
                 Day_Of_Year, Week_Number, Month, Month_Name, Quarter,
                 Quarter_Label, Half_Year, Year, Season, Fiscal_Month, Fiscal_Quarter, Fiscal_Year)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, batch)
            connection.commit()

        cursor.execute("SELECT COUNT(*) FROM Dim_Date")
        count = cursor.fetchone()[0]
        print(f"Dim_Date loaded: {count} records")

    except Error as e:
        connection.rollback()
        print(f"✗ Error populating Dim_Date: {e}")
        raise
    finally:
        cursor.close()

def load_dimension_data(connection, csv_file_path, table_name, id_column):
    """Load Customer or Product dimension from master data CSV."""
    cursor = connection.cursor()

    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"{table_name} already has {count} records")
            cursor.execute(f"SELECT {id_column}, {table_name.replace('Dim_', '')}_Key FROM {table_name}")
            rows = cursor.fetchall()
            if id_column == 'Customer_ID':
                lookup = {int(row[0]): row[1] for row in rows}
            else:
                lookup = {row[0]: row[1] for row in rows}
            return lookup

        print(f"Loading {table_name} from {csv_file_path}...")

        df = pd.read_csv(csv_file_path)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        if 'price$' in df.columns:
            df = df.rename(columns={'price$': 'price'})

        if table_name == 'Dim_Customer':
            records = []
            for row in df.itertuples(index=False):
                loyalty = _loyalty_segment(row.Stay_In_Current_City_Years, int(row.Marital_Status))
                records.append((
                    int(row.Customer_ID),
                    row.Gender,
                    row.Age,
                    int(row.Occupation),
                    _occupation_bucket(int(row.Occupation)),
                    row.City_Category,
                    _city_tier(row.City_Category),
                    row.Stay_In_Current_City_Years,
                    _stay_bucket(row.Stay_In_Current_City_Years),
                    int(row.Marital_Status),
                    'Married' if int(row.Marital_Status) == 1 else 'Single',
                    loyalty
                ))

            insert_query = """
                INSERT INTO Dim_Customer 
                (Customer_ID, Gender, Age, Occupation, Occupation_Bucket,
                 City_Category, City_Tier, Stay_In_Current_City_Years, Stay_Bucket,
                 Marital_Status, Marital_Status_Label, Loyalty_Segment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        elif table_name == 'Dim_Product':
            records = []
            for row in df.itertuples(index=False):
                price = float(row.price)
                records.append((
                    row.Product_ID,
                    row.Product_Category,
                    price,
                    _price_band(price),
                    1 if price >= 70 else 0
                ))

            insert_query = """
                INSERT INTO Dim_Product 
                (Product_ID, Product_Category, Unit_Price, Price_Band, Is_Premium)
                VALUES (%s, %s, %s, %s, %s)
            """

        else:
            raise ValueError(f"Unsupported dimension table: {table_name}")

        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"{table_name} loaded: {count} records")

        cursor.execute(f"SELECT {id_column}, {table_name.replace('Dim_', '')}_Key FROM {table_name}")
        rows = cursor.fetchall()
        if id_column == 'Customer_ID':
            lookup = {int(row[0]): row[1] for row in rows}
        else:
            lookup = {row[0]: row[1] for row in rows}
        return lookup

    except Error as e:
        connection.rollback()
        print(f"Error loading {table_name}: {e}")
        raise
    finally:
        cursor.close()


def load_store_dimension(connection, product_master_path):
    """Load Store dimension derived from product master data."""
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM Dim_Store")
        count = cursor.fetchone()[0]

        if count > 0:
            cursor.execute("SELECT Store_ID, Store_Key FROM Dim_Store")
            return {str(row[0]): row[1] for row in cursor.fetchall()}

        print("Loading Dim_Store from product master data...")

        df = pd.read_csv(product_master_path)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        if 'price$' in df.columns:
            df = df.rename(columns={'price$': 'price'})

        grouped = df.groupby(['storeID', 'storeName'])
        records = []
        for (store_id, store_name), group in grouped:
            sku_count = int(group['Product_ID'].nunique())
            category_count = int(group['Product_Category'].nunique())
            avg_price = round(float(group['price'].mean()), 2)
            tier = _store_tier(sku_count)
            is_flagship = 1 if tier == 'Mega' or avg_price >= 60 else 0
            records.append((
                str(store_id),
                store_name,
                'In-Store',
                tier,
                sku_count,
                category_count,
                avg_price,
                is_flagship,
                1
            ))

        insert_query = """
            INSERT INTO Dim_Store
            (Store_ID, Store_Name, Store_Channel, Store_Tier, SKU_Count,
             Category_Count, Avg_List_Price, Is_Flagship, Is_Active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch_size = 500
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()

        cursor.execute("SELECT COUNT(*) FROM Dim_Store")
        count = cursor.fetchone()[0]
        print(f"Dim_Store loaded: {count} records")

        cursor.execute("SELECT Store_ID, Store_Key FROM Dim_Store")
        return {str(row[0]): row[1] for row in cursor.fetchall()}

    except Error as e:
        connection.rollback()
        print(f"Error loading Dim_Store: {e}")
        raise
    finally:
        cursor.close()


def load_supplier_dimension(connection, product_master_path):
    """Load Supplier dimension derived from product master data."""
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM Dim_Supplier")
        count = cursor.fetchone()[0]

        if count > 0:
            cursor.execute("SELECT Supplier_ID, Supplier_Key FROM Dim_Supplier")
            return {str(row[0]): row[1] for row in cursor.fetchall()}

        print("Loading Dim_Supplier from product master data...")

        df = pd.read_csv(product_master_path)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        if 'price$' in df.columns:
            df = df.rename(columns={'price$': 'price'})

        grouped = df.groupby(['supplierID', 'supplierName'])
        records = []
        for (supplier_id, supplier_name), group in grouped:
            sku_count = int(group['Product_ID'].nunique())
            avg_price = round(float(group['price'].mean()), 2)
            primary_category = Counter(group['Product_Category']).most_common(1)[0][0]
            tier = _supplier_tier(sku_count)
            # Reliability_Score removed: arbitrary formula without business justification
            records.append((
                str(supplier_id),
                supplier_name,
                tier,
                primary_category,
                sku_count,
                avg_price,
                None  # Reliability_Score placeholder for future implementation
            ))

        insert_query = """
            INSERT INTO Dim_Supplier
            (Supplier_ID, Supplier_Name, Supplier_Tier, Primary_Category,
             SKU_Count, Avg_List_Price, Reliability_Score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        batch_size = 500
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()

        cursor.execute("SELECT COUNT(*) FROM Dim_Supplier")
        count = cursor.fetchone()[0]
        print(f"Dim_Supplier loaded: {count} records")

        cursor.execute("SELECT Supplier_ID, Supplier_Key FROM Dim_Supplier")
        return {str(row[0]): row[1] for row in cursor.fetchall()}

    except Error as e:
        connection.rollback()
        print(f"Error loading Dim_Supplier: {e}")
        raise
    finally:
        cursor.close()


def populate_all_dimensions(connection, customer_master_path, product_master_path):
    """
    Populate ALL dimension tables from master data BEFORE running HYBRIDJOIN.
    This is done once at the start, using batch loading for efficiency.
    """
    print("\n" + "="*60)
    print("PHASE 1: Populating dimension tables from master data")
    print("="*60 + "\n")
    
    # 1. Populate Date dimension (2015-2020)
    populate_dim_date(connection, start_year=2015, end_year=2020)
    
    # 2. Populate Customer dimension
    load_dimension_data(connection, customer_master_path, 'Dim_Customer', 'Customer_ID')
    
    # 3. Populate Product dimension  
    load_dimension_data(connection, product_master_path, 'Dim_Product', 'Product_ID')
    
    # 4. Populate Store dimension
    load_store_dimension(connection, product_master_path)
    
    # 5. Populate Supplier dimension
    load_supplier_dimension(connection, product_master_path)
    
    print("\n" + "="*60)
    print("✓ All dimension tables populated successfully!")
    print("  Ready to run HYBRIDJOIN for fact table population.")
    print("="*60 + "\n")


def load_dimension_lookups(connection):
    """
    Load all dimension lookups into memory for O(1) surrogate key lookups during HYBRIDJOIN.
    This is called ONCE after dimensions are populated, before the pipeline starts.
    Returns dictionaries mapping natural keys to surrogate keys.
    """
    cursor = connection.cursor()
    
    try:
        print("Loading dimension lookups into memory...")
        
        lookups = {
            'customer': {},
            'product': {},
            'store': {},
            'supplier': {},
            'date': {}
        }
        
        # Load customer lookups: Customer_ID -> Customer_Key
        cursor.execute("SELECT Customer_ID, Customer_Key FROM Dim_Customer")
        for customer_id, customer_key in cursor.fetchall():
            lookups['customer'][int(customer_id)] = customer_key
        
        # Load product lookups: Product_ID -> Product_Key
        cursor.execute("SELECT Product_ID, Product_Key FROM Dim_Product")
        for product_id, product_key in cursor.fetchall():
            lookups['product'][str(product_id)] = product_key
        
        # Load store lookups: Store_ID -> Store_Key
        cursor.execute("SELECT Store_ID, Store_Key FROM Dim_Store")
        for store_id, store_key in cursor.fetchall():
            lookups['store'][str(store_id)] = store_key
        
        # Load supplier lookups: Supplier_ID -> Supplier_Key
        cursor.execute("SELECT Supplier_ID, Supplier_Key FROM Dim_Supplier")
        for supplier_id, supplier_key in cursor.fetchall():
            lookups['supplier'][str(supplier_id)] = supplier_key
        
        # Load date lookups: Date_Key -> Date_Key (date key IS the primary key)
        cursor.execute("SELECT Date_Key FROM Dim_Date")
        for (date_key,) in cursor.fetchall():
            lookups['date'][date_key] = date_key
        
        print(f"✓ Dimension lookups loaded: {len(lookups['customer'])} customers, "
              f"{len(lookups['product'])} products, {len(lookups['store'])} stores, "
              f"{len(lookups['supplier'])} suppliers, {len(lookups['date'])} dates")
        return lookups
        
    except Error as e:
        print(f"Error loading dimension lookups: {e}")
        raise
    finally:
        cursor.close()

def load_master_data_tables(connection, customer_master_path, product_master_path):
    """Load Master_Customer and Master_Product tables for HYBRIDJOIN disk relations."""
    cursor = connection.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM Master_Customer")
        customer_count = cursor.fetchone()[0]
        
        if customer_count == 0:
            print("Loading Master_Customer table...")
            df = pd.read_csv(customer_master_path)
            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])
            
            df = df.sort_values('Customer_ID')
            
            insert_query = """
                INSERT INTO Master_Customer 
                (Customer_ID, Gender, Age, Occupation, City_Category, 
                 Stay_In_Current_City_Years, Marital_Status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            data = df[['Customer_ID', 'Gender', 'Age', 'Occupation', 'City_Category',
                      'Stay_In_Current_City_Years', 'Marital_Status']].values.tolist()
            
            batch_size = 1000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                connection.commit()
            
            cursor.execute("SELECT COUNT(*) FROM Master_Customer")
            count = cursor.fetchone()[0]
            print(f"Master_Customer loaded with {count} records (sorted by Customer_ID)")
        else:
            print(f"Master_Customer already has {customer_count} records")
        
        cursor.execute("SELECT COUNT(*) FROM Master_Product")
        product_count = cursor.fetchone()[0]
        
        if product_count == 0:
            print("Loading Master_Product table...")
            df = pd.read_csv(product_master_path)
            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])
            if 'price$' in df.columns:
                df = df.rename(columns={'price$': 'price'})
            
            df = df.sort_values('Product_ID')
            
            insert_query = """
                INSERT INTO Master_Product 
                (Product_ID, Product_Category, Price, StoreID, StoreName, 
                 SupplierID, SupplierName)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            data = df[['Product_ID', 'Product_Category', 'price', 'storeID', 'storeName',
                      'supplierID', 'supplierName']].values.tolist()
            
            batch_size = 1000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                connection.commit()
            
            cursor.execute("SELECT COUNT(*) FROM Master_Product")
            count = cursor.fetchone()[0]
            print(f"Master_Product loaded: {count} records")
        else:
            print(f"Master_Product already populated with {product_count} records")
        
    except Error as e:
        connection.rollback()
        print(f"Error loading master data tables: {e}")
        raise
    finally:
        cursor.close()

def get_customer_partition(connection, customer_id, partition_size=500):
    """Fetch customer partition from Master_Customer (disk-based relation for HYBRIDJOIN)."""
    cursor = connection.cursor(dictionary=True)
    
    try:
        query = """
            SELECT Customer_ID, Gender, Age, Occupation, City_Category,
                   Stay_In_Current_City_Years, Marital_Status
            FROM Master_Customer
            WHERE Customer_ID >= %s
            ORDER BY Customer_ID
            LIMIT %s
        """
        cursor.execute(query, (customer_id, partition_size))
        partition = cursor.fetchall()
        return partition
        
    except Error as e:
        print(f"Error querying customer partition: {e}")
        raise
    finally:
        cursor.close()

def get_product_partition(connection, product_id, partition_size=500):
    """Fetch product partition from Master_Product (disk-based relation for HYBRIDJOIN)."""
    cursor = connection.cursor(dictionary=True)
    
    try:
        query = """
            SELECT Product_ID, Product_Category, Price, StoreID, StoreName,
                   SupplierID, SupplierName
            FROM Master_Product
            WHERE Product_ID >= %s
            ORDER BY Product_ID
            LIMIT %s
        """
        cursor.execute(query, (product_id, partition_size))
        partition = cursor.fetchall()
        return partition
        
    except Error as e:
        print(f"Error querying product partition: {e}")
        raise
    finally:
        cursor.close()

def insert_fact_sales_batch(connection, batch):
    """Insert validated facts into Fact_Sales using surrogate keys."""
    cursor = connection.cursor()
    
    try:
        insert_query = """
            INSERT INTO Fact_Sales 
            (Order_ID, Order_Line_Number, Customer_Key, Product_Key, Store_Key, Supplier_Key,
             Date_Key, Quantity, Unit_Price, Total_Purchase_Amount, Discount_Amount, Weekend_Flag, Order_Channel)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, batch)
        connection.commit()
        return len(batch)
        
    except Error as e:
        connection.rollback()
        print(f"Error inserting fact sales batch: {e}")
        raise
    finally:
        cursor.close()
