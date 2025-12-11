# Walmart Data Warehouse - Near-Real-Time ETL with HYBRIDJOIN

**Student:** Abdullah Ali (i232523)  
**Course:** DS3003 & DS3004 - Data Warehousing & Business Intelligence  
**Instructor:** Dr. Asif Naeem

---

## Overview

This project implements a **near-real-time data warehouse** for Walmart sales data using the **HYBRIDJOIN** stream-relation join algorithm. The system demonstrates bounded-memory stream processing combined with efficient dimension lookups.

### Key Features

- **Bounded Stream Processing**: HYBRIDJOIN maintains O(32,000) tuple memory for stream enrichment, independent of transaction volume
- **Thin Dimension Lookups**: Only ID mappings (76 KB total) cached in RAM for O(1) surrogate key resolution
- **Wide Master Data Enrichment**: Full attribute payloads loaded via partitioned disk access (500 rows per partition)
- **4-Phase ETL Architecture**: (1) Batch-load dimensions, (2) Cache thin lookups, (3) Load wide master data to disk, (4) Run bounded HYBRIDJOIN
- **High Throughput**: 15,000 records/second on 550K transaction prototype
- **Star Schema**: 5 dimensions + 1 fact table
- **20 OLAP Queries**: Sub-second to low-second response times

### Memory Architecture

- **Thin Lookups**: Customer\_ID → Customer\_Key (47 KB), Product\_ID → Product\_Key (29 KB), total ~76 KB for all dimensions
- **Wide Enrichment**: Master\_Customer (7 columns), Master\_Product (7 columns) loaded in bounded partitions
- **Stream Processing**: Constant 32K tuples regardless of transaction volume

---

## System Requirements

- MySQL Server 8.0+
- Python 3.8+
- Packages: `mysql-connector-python`, `pandas` (in `hybrid_join/requirements.txt`)
- Hardware: Any modern system (prototype tested on Ryzen 7, 32GB RAM)

---

## Installation & Setup

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
   cd hybrid_join
   pip install -r requirements.txt
   ```

---

## Running the ETL Pipeline

```bash
cd hybrid_join
python hybridjoin.py
```

Provide MySQL credentials when prompted. The ETL runs 4 phases:

1. **Phase 1**: Batch-loads ALL dimensions (Customer, Product, Store, Supplier, Date)
2. **Phase 2**: Caches dimension lookups in memory for O(1) key resolution
3. **Phase 3**: Loads Master_Customer and Master_Product disk relations (sorted, indexed)
4. **Phase 4**: Runs 3-thread HYBRIDJOIN pipeline (Producer → Consumer-Customer → Consumer-Product → Fact_Sales)

**Expected**: ~37 seconds, ~15,000 rec/sec, 550,068 records in Fact_Sales

---

## Running OLAP Queries

```bash
mysql -u root -p DWH_Proj < Queries-DW.sql
```

20 analytical queries demonstrate OLAP operations (slicing, dicing, window functions, trend analysis).

---

## Understanding HYBRIDJOIN

**HYBRIDJOIN** [Naeem et al., 2011] is a stream-relation join algorithm for bounded-memory stream processing:

### Architecture

```
Transaction Stream (CSV) → Producer Thread → Stream Buffer
   ↓
Consumer 1: Join with Master_Customer (disk partitions, vP=500)
   ↓  
Intermediate Queue
   ↓
Consumer 2: Join with Master_Product → Lookup dimension keys (RAM) → Fact_Sales
```

### Key Components

- **Hash Table (hS=10,000)**: Stores stream tuples by join key
- **FIFO Queue**: Maintains fairness for old stream tuples
- **Disk Buffer (vP=500)**: Loads partitions from sorted Master tables
- **Dimension Lookups (RAM)**: O(1) surrogate key resolution (pre-loaded in Phase 2)

### Memory Architecture

- **Stream Processing**: O(32,000) tuples constant—independent of transaction volume
- **Dimension Lookups**: O(dimension cardinality)—scales linearly with available RAM

### Extensibility for Enterprise Scale

The architectural patterns demonstrated (bounded stream + in-memory dimensions) are extensible through:
- **Distributed memory grids** (Redis clusters for shared dimension lookups)
- **Partitioned ETL nodes** (each processing a subset of stream keys)
- **High-RAM servers** (modern servers with 256GB+ can hold large dimension sets)

Stream processing remains bounded regardless of transaction volume.

---

## Expected Results

After successful execution:

| Table | Records | Note |
|-------|---------|------|
| `Dim_Customer` | 5,891 | Demographics and behavioral attributes |
| `Dim_Product` | 3,631 | Product catalog with pricing |
| `Dim_Store` | 8 | Store profiles with merchandising metrics |
| `Dim_Supplier` | 7 | Supplier characteristics |
| `Dim_Date` | 2,192 | Calendar dates 2015-2020 with fiscal attributes |
| `Fact_Sales` | 550,068 | Transaction-level facts with foreign keys to dimensions |

---

## Troubleshooting

**Database connection error**: Verify MySQL is running, credentials are correct  
**FileNotFoundError**: Run script from `hybrid_join/` directory  
**ModuleNotFoundError**: Run `pip install -r requirements.txt`  
**Duplicate entry error**: Truncate `Fact_Sales` before re-running ETL  
**Slow performance**: Close other applications, use SSD storage

---

## Architecture Highlights

### Memory Strategy

- **Stream Processing**: HYBRIDJOIN maintains constant O(32,000) tuple memory regardless of transaction volume
- **Dimension Lookups**: Pre-loaded into RAM for O(1) key resolution, scales linearly with dimension cardinality
- **Separation of Concerns**: Bounded stream processing + in-memory dimension lookups = efficient near-real-time ETL

### Design Patterns Demonstrated

This prototype demonstrates architectural patterns applicable to larger systems:
1. **Batch dimension loading** (Phase 1) separate from stream processing (Phase 4)
2. **Disk-based partitioned access** for master data relations
3. **Multi-threaded pipeline** with bounded queues for backpressure handling
4. **In-memory caching** for frequently accessed dimension lookups

For enterprise deployment, these patterns extend through distributed infrastructure (memory grids, partitioned databases, message queues).

---

## References

1. Naeem, M.A., Dobbie, G., & Weber, G. (2011). HYBRIDJOIN for Near-Real-Time Data Warehousing. ADC 2011.
2. Kimball, R., & Ross, M. (2013). The Data Warehouse Toolkit (3rd ed.). Wiley.

---

**Last Updated**: November 2025  
**Version**: 1.0 (Iteration 6 - Production Architecture)
