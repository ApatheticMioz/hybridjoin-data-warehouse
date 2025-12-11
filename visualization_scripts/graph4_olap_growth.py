"""Plot quarterly revenue growth per store (OLAP Q12 – Iteration 6)."""

import matplotlib.pyplot as plt
import numpy as np
import os

def generate_olap_growth():
    # Set professional style
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['font.size'] = 11
    
    # Connect to database and fetch real data from Q12 - NO FALLBACK
    from db_config import DB_CONFIG
    import mysql.connector
    
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Execute Q12 query (full version with all columns)
    query = """
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
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    print(f"[DATABASE] Query returned {len(results)} rows from Fact_Sales star schema")
    print(f"[DATABASE] First 8 rows preview:")
    for i, row in enumerate(results[:8]):
        store_id, store_name, year, quarter, quarter_label, revenue, prev_revenue, growth_rate = row
        growth_display = 'N/A' if growth_rate is None else f"{growth_rate}%"
        print(f"  {i+1}. {store_name} {quarter_label}: Revenue=${revenue:.2f}, Growth={growth_display}")
    
    if not results or len(results) < 8:
        raise Exception(f"Insufficient data from database: got {len(results) if results else 0} rows, need at least 8")
    
    # Parse results - organize by store
    store_data = {}
    for row in results:
        store_id, store_name, year, quarter, quarter_label, revenue, prev_revenue, growth_rate = row
        if store_id not in store_data:
            store_data[store_id] = {'name': store_name, 'data': []}
        store_data[store_id]['data'].append({
            'quarter': quarter,
            'label': quarter_label,
            'growth': 0.0 if growth_rate is None else float(growth_rate)
        })
    
    # Get ALL stores
    store_ids = sorted(store_data.keys())
    num_stores = len(store_ids)
    
    print(f"[INFO] Plotting {num_stores} stores from Q12 query")
    
    # Extract data for plotting - all stores
    quarters = [f"Q{i}-2017" for i in range(1, 5)]
    all_store_data = []
    for store_id in store_ids:
        store_name = store_data[store_id]['name']
        growth_values = [d['growth'] for d in store_data[store_id]['data'][:4]]
        all_store_data.append({'name': store_name, 'growth': growth_values})
        print(f"[DATA] {store_name}: {growth_values}")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Color palette for multiple stores
    colors = ['#E53935', '#1E88E5', '#43A047', '#FB8C00', '#8E24AA', '#00ACC1', '#D81B60', '#6D4C41']
    markers = ['o', 's', '^', 'D', 'v', 'p', '*', 'h']
    
    # Plot lines for all stores
    x = np.arange(len(quarters))
    all_growth_values = []
    
    for i, store_info in enumerate(all_store_data):
        color = colors[i % len(colors)]
        marker = markers[i % len(markers)]
        growth = store_info['growth']
        all_growth_values.extend(growth[1:])  # Skip Q1 (which is 0) for range calculation
        
        ax.plot(x, growth, marker=marker, linewidth=2.5, markersize=10, 
                label=store_info['name'], color=color, 
                linestyle='-', markeredgecolor='black', markeredgewidth=1.2,
                alpha=0.85)
    
    # Calculate dynamic y-axis limits based on actual data
    min_growth = min(all_growth_values)
    max_growth = max(all_growth_values)
    y_padding = (max_growth - min_growth) * 0.2
    y_min = min_growth - y_padding
    y_max = max_growth + y_padding
    
    # Add horizontal line at y=0 with better styling
    ax.axhline(y=0, color='black', linestyle='--', linewidth=2, alpha=0.6, zorder=1)
    
    # Shade positive/negative regions dynamically
    if y_max > 0:
        ax.axhspan(0, y_max, alpha=0.05, color='green', zorder=0)
    if y_min < 0:
        ax.axhspan(y_min, 0, alpha=0.05, color='red', zorder=0)
    
    # Labels and title with professional formatting
    ax.set_xlabel('Quarter (2017)', fontsize=14, fontweight='bold', labelpad=10)
    ax.set_ylabel('Revenue Growth Rate (%)', fontsize=14, fontweight='bold', labelpad=10)
    ax.set_title(f'Quarterly Store Revenue Growth Analysis (2017) - {num_stores} Stores\nOLAP Query 12: Star Schema Data (Fact_Sales ⋈ Dim_Product ⋈ Dim_Date)', 
                 fontsize=15, fontweight='bold', pad=20)
    
    # X-axis formatting
    ax.set_xticks(x)
    ax.set_xticklabels(quarters, fontsize=12, fontweight='bold')
    
    # Y-axis range - DYNAMIC based on actual data
    ax.set_ylim(y_min, y_max)
    ax.tick_params(axis='both', which='major', labelsize=11)
    
    # Enhanced grid
    ax.yaxis.grid(True, linestyle='--', alpha=0.5, linewidth=1)
    ax.xaxis.grid(True, linestyle=':', alpha=0.3, linewidth=0.8)
    ax.set_axisbelow(True)
    
    # Professional legend
    ax.legend(loc='best', fontsize=10, framealpha=0.95, 
             edgecolor='black', fancybox=True, shadow=True, 
             title='Store Performance', title_fontsize=11, ncol=2)
    
    # Add professional annotation box with dynamic insights
    textstr = (f'Real DW extract (18 Nov 2025):\n'
               f'• {num_stores} stores (Dim_Store)\n'
               f'• Query: Q12 against 550,068 rows\n'
               f'• Growth range: {min_growth:.2f}% to {max_growth:.2f}%')
    props = dict(boxstyle='round', facecolor='#FFF9C4', edgecolor='black', 
                linewidth=2, alpha=0.9)
    ax.text(0.97, 0.97, textstr, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', horizontalalignment='right', bbox=props)
    
    # Add border
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.5)
    
    # Tight layout
    plt.tight_layout()
    
    # Save with high quality
    import os
    output_path = os.path.join(os.path.dirname(__file__), '..', 'figures', 'graph4_olap_growth.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    print("[OK] Graph 4 generated: figures/graph4_olap_growth.png")
    plt.close()

if __name__ == '__main__':
    generate_olap_growth()
