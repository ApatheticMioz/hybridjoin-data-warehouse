"""
Test script to verify the producer tuple creation is correct
"""
import pandas as pd
from datetime import datetime

print("="*70)
print("PRODUCER TUPLE CREATION TEST")
print("="*70)

# Simulate the producer's data loading
csv_path = "../Data/transactional_data.csv"
print(f"\n1. Loading CSV: {csv_path}")
df = pd.read_csv(csv_path)

print(f"   - Initial shape: {df.shape}")
print(f"   - Initial columns: {list(df.columns)}")

# Clean data (same as producer)
if 'Unnamed: 0' in df.columns:
    df = df.drop(columns=['Unnamed: 0'])
    print(f"   - After dropping 'Unnamed: 0': {df.shape}")
    print(f"   - Final columns: {list(df.columns)}")

# Convert date column
df['date'] = pd.to_datetime(df['date'])
print(f"   - Converted 'date' to datetime")

print(f"\n2. Testing tuple creation (first 5 rows):")
print("-" * 70)

for idx, row in enumerate(df.head(5).itertuples(index=False, name=None)):
    stream_tuple = {
        'Order_ID': int(row[0]),           # orderID
        'Customer_ID': int(row[1]),        # Customer_ID
        'Product_ID': str(row[2]),         # Product_ID
        'quantity': int(row[3]),           # quantity
        'date': row[4]                     # date
    }
    
    print(f"\nRow {idx}:")
    print(f"  Raw tuple: {row}")
    print(f"  Stream tuple:")
    print(f"    - Order_ID: {stream_tuple['Order_ID']}")
    print(f"    - Customer_ID: {stream_tuple['Customer_ID']}")
    print(f"    - Product_ID: {stream_tuple['Product_ID']}")
    print(f"    - quantity: {stream_tuple['quantity']}")
    print(f"    - date: {stream_tuple['date']} (type: {type(stream_tuple['date'])})")

print("\n" + "-" * 70)

print(f"\n3. Verification against original DataFrame:")
for idx in range(5):
    original = df.iloc[idx]
    
    # Create tuple same way
    row_tuple = list(df.iloc[idx:idx+1].itertuples(index=False, name=None))[0]
    stream_tuple = {
        'Order_ID': int(row_tuple[0]),
        'Customer_ID': int(row_tuple[1]),
        'Product_ID': str(row_tuple[2]),
        'quantity': int(row_tuple[3]),
        'date': row_tuple[4]
    }
    
    # Verify
    matches = []
    matches.append(("Order_ID", stream_tuple['Order_ID'] == original['orderID']))
    matches.append(("Customer_ID", stream_tuple['Customer_ID'] == original['Customer_ID']))
    matches.append(("Product_ID", stream_tuple['Product_ID'] == original['Product_ID']))
    matches.append(("quantity", stream_tuple['quantity'] == original['quantity']))
    matches.append(("date", stream_tuple['date'] == original['date']))
    
    all_match = all([m[1] for m in matches])
    status = "✓ PASS" if all_match else "✗ FAIL"
    
    print(f"\nRow {idx}: {status}")
    for field, match in matches:
        symbol = "✓" if match else "✗"
        print(f"  {symbol} {field}")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)
