# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.0.0] - Initial Archival Release

### Added
- HYBRIDJOIN algorithm implementation for near-real-time ETL
- Star schema with 5 dimension tables and 1 fact table
- 20 OLAP analytical queries demonstrating slicing, dicing, drill-down, and window functions
- Sample Walmart transaction data (550,068 records)
- Comprehensive documentation and project report

### Features
- Bounded-memory stream processing (O(32,000) tuples)
- High throughput ETL pipeline (~15,000 records/second)
- 4-phase ETL architecture with dimension pre-loading
- Multi-threaded producer-consumer pipeline

### Documentation
- Professional README with setup instructions
- LaTeX project report with algorithm analysis
- Jupyter notebook for data exploration
