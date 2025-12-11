"""
Diagnostic script to analyze the transactional CSV structure
"""
import pandas as pd

print("="*70)
print("CSV STRUCTURE ANALYSIS")
print("="*70)

# Read CSV
csv_path = "../Data/transactional_data.csv"
print(f"\nReading: {csv_path}")

df = pd.read_csv(csv_path)

print(f"\n1. DataFrame Shape: {df.shape}")
print(f"   - Rows: {df.shape[0]:,}")
print(f"   - Columns: {df.shape[1]}")

print(f"\n2. Column Names:")
for i, col in enumerate(df.columns):
    print(f"   [{i}] {col}")

print(f"\n3. Column Data Types:")
print(df.dtypes)

print(f"\n4. First 5 Rows:")
print(df.head())

print(f"\n5. Sample Row Access Test:")
print("   Using iterrows():")
for idx, row in df.head(1).iterrows():
    print(f"   - orderID: {row.get('orderID', 'NOT FOUND')}")
    print(f"   - Customer_ID: {row.get('Customer_ID', 'NOT FOUND')}")
    print(f"   - Product_ID: {row.get('Product_ID', 'NOT FOUND')}")
    print(f"   - date: {row.get('date', 'NOT FOUND')}")
    print(f"   - quantity: {row.get('quantity', 'NOT FOUND')}")

print("\n   Using itertuples():")
for row in df.head(1).itertuples(index=False, name=None):
    print(f"   - Row tuple: {row}")
    print(f"   - Length: {len(row)}")
    print(f"   - [0]: {row[0]}")
    print(f"   - [1]: {row[1]}")
    print(f"   - [2]: {row[2]}")
    print(f"   - [3]: {row[3]}")
    print(f"   - [4]: {row[4]}")

print(f"\n6. Missing Values:")
print(df.isnull().sum())

print(f"\n7. After Cleaning 'Unnamed: 0':")
if 'Unnamed: 0' in df.columns:
    df = df.drop(columns=['Unnamed: 0'])
    print(f"   Dropped 'Unnamed: 0' column")
    print(f"   New column count: {df.shape[1]}")
    print(f"   Columns: {list(df.columns)}")

print("\n" + "="*70)
print("ANALYSIS COMPLETE")
print("="*70)
