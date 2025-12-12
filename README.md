# HYBRIDJOIN Data Warehouse

A near-real-time data warehouse implementation using the **HYBRIDJOIN** stream-relation join algorithm for bounded-memory ETL processing of retail transaction data.

## Overview

This project demonstrates a production-ready approach to building and analyzing a data warehouse for retail sales data. It implements the HYBRIDJOIN algorithm (Naeem et al., 2011) to efficiently join streaming transaction data with disk-based master data relations while maintaining bounded memory usage.

### Key Features

- **Bounded Stream Processing**: O(32,000) tuple memory for stream enrichment, independent of transaction volume
- **High Throughput ETL**: ~15,000 records/second on 550K transaction dataset
- **Star Schema Design**: 5 dimension tables + 1 fact table optimized for OLAP
- **20 Analytical Queries**: Demonstrating slicing, dicing, drill-down, rollup, and window functions
- **4-Phase ETL Architecture**: Batch-load dimensions → Cache lookups → Load master data → Run HYBRIDJOIN

## Project Structure

```
.
├── src/                    # Python HYBRIDJOIN implementation
│   ├── hybridjoin.py       # Main ETL pipeline with 3-thread architecture
│   ├── db_utils.py         # Database utilities and dimension loading
│   └── doubly_linked_list.py  # Queue implementation for HYBRIDJOIN
├── data/                   # Sample datasets
│   ├── customer_master_data.csv
│   ├── product_master_data.csv
│   └── transactional_data.csv
├── Create-DW.sql           # Star schema DDL
├── Master-Data.sql         # Master data loading script
├── Queries-DW.sql          # 20 OLAP analytical queries
├── Project-Report.tex      # Detailed LaTeX report
├── analysis.ipynb          # Data exploration notebook
├── figures/                # Generated diagrams
└── requirements.txt        # Python dependencies
```

## Installation

### Prerequisites

- MySQL Server 8.0+
- Python 3.8+

### Setup

1. **Create Database**:
   ```sql
   CREATE DATABASE DWH_Proj;
   ```

2. **Run DDL Scripts**:
   ```bash
   mysql -u root -p DWH_Proj < Create-DW.sql
   mysql -u root -p DWH_Proj < Master-Data.sql
   ```

3. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Run the ETL Pipeline

```bash
python src/hybridjoin.py
```

Enter your MySQL credentials when prompted. The ETL executes 4 phases:

1. **Phase 1**: Batch-loads all dimension tables
2. **Phase 2**: Caches dimension key mappings in memory (~76 KB)
3. **Phase 3**: Prepares disk-based master data relations
4. **Phase 4**: Runs 3-thread HYBRIDJOIN pipeline

**Expected Output**: ~37 seconds, 550,068 records loaded into `Fact_Sales`

### Run OLAP Queries

```bash
mysql -u root -p DWH_Proj < Queries-DW.sql
```

## HYBRIDJOIN Algorithm

The algorithm joins streaming transactions with disk-based master data using:

- **Hash Table (hS=10,000)**: Stores stream tuples by join key
- **FIFO Queue**: Maintains fairness for processing order
- **Disk Buffer (vP=500)**: Loads partitions from sorted master tables

### Architecture

```
Transaction Stream → Producer Thread → Stream Buffer
                           ↓
        Consumer 1: Join with Master_Customer (disk partitions)
                           ↓
                   Intermediate Queue
                           ↓
        Consumer 2: Join with Master_Product → Lookup Keys → Fact_Sales
```

## Expected Results

| Table | Records | Description |
|-------|---------|-------------|
| `Dim_Customer` | 5,891 | Customer demographics and behavior |
| `Dim_Product` | 3,631 | Product catalog with pricing |
| `Dim_Store` | 8 | Store profiles |
| `Dim_Supplier` | 7 | Supplier characteristics |
| `Dim_Date` | 2,192 | Calendar dates 2015-2020 |
| `Fact_Sales` | 550,068 | Transaction facts with dimension keys |

## Status

**Archived / Refactored** - This is a cleaned and standardized version of an academic data warehousing project.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## References

1. Naeem, M.A., Dobbie, G., & Weber, G. (2011). HYBRIDJOIN for Near-Real-Time Data Warehousing. ADC 2011.
2. Kimball, R., & Ross, M. (2013). The Data Warehouse Toolkit (3rd ed.). Wiley.
