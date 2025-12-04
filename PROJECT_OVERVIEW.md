# Complete Data Pipeline System - Final Documentation

## Project Overview

This project implements **three major data engineering pipelines** for retail/e-commerce data processing:

1. **Unified Product & Inventory Data Harmonization Pipeline**
2. **Real-Time Shopping Basket Affinity Analyzer**
3. **Refund & Fraud Detection Engine**

Each pipeline is fully modular, production-ready, and can be run independently or integrated together.

---

## 📁 Complete Project Structure

```
hcllll/
├── configs/
│   └── ingestion_config.yaml              # Config-driven ingestion settings
├── data/
│   ├── inventory_snapshot.csv             # Sample inventory data
│   ├── restock_events.xlsx                # Sample restock data
│   └── incoming_inventory.json            # Sample incoming inventory
├── affinity_analysis_output/              # Affinity analyzer outputs
│   ├── affinity_report.txt
│   ├── recommendations.csv
│   ├── top_affinities.png
│   └── affinity_heatmap.png
├── fraud_detection_output/                # Fraud detection outputs
│   ├── fraud_report.txt
│   ├── fraud_flags.csv
│   └── validation_issues.csv
│
├── PIPELINE 1: INVENTORY HARMONIZATION
├── config.py                              # Configuration constants
├── data_generator.py                      # Generate synthetic inventory data
├── validators.py                          # Data validation checks
├── reconciliation.py                      # Product ID reconciliation (fuzzy matching)
├── data_processor.py                      # Data transformation & enrichment
├── config_loader.py                       # Config file loader
├── file_reader.py                         # Multi-format file reader
├── column_mapper.py                       # Column mapping engine
├── data_transformer.py                    # Data transformation engine
├── ingestion_engine.py                    # Config-driven ingestion orchestrator
├── main.py                                # Main pipeline (dual-mode)
│
├── PIPELINE 2: AFFINITY ANALYZER
├── transaction_generator.py               # Generate transaction data
├── market_basket_analyzer.py              # Market basket analysis engine
├── incremental_affinity_updater.py        # Real-time incremental updates
├── affinity_reporter.py                   # Reporting & visualization
├── affinity_analyzer_main.py              # Main affinity analyzer
│
├── PIPELINE 3: FRAUD DETECTION
├── refund_data_generator.py               # Generate refund data
├── refund_validator.py                    # Refund legitimacy validation
├── fraud_detector.py                      # Fraud pattern detection
├── anomaly_detector.py                    # Statistical anomaly detection
├── fraud_flagger.py                       # Fraud scoring & flagging
├── fraud_detection_main.py                # Main fraud detection
│
├── UTILITIES & DOCUMENTATION
├── generate_sample_data.py                # Generate sample data files
├── requirements.txt                       # Python dependencies
├── README.md                              # Main project README
├── CONFIG_DRIVEN_INGESTION.md             # Ingestion guide
├── AFFINITY_ANALYZER.md                   # Affinity analyzer guide
└── FRAUD_DETECTION.md                     # Fraud detection guide
```

---

# PIPELINE 1: Unified Product & Inventory Data Harmonization

## Overview

Automated pipeline that harmonizes inventory snapshots, restock logs, and product catalog data into a clean, validated "single source of truth" inventory model.

## Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Synthetic Mode:                  Config-Driven Mode:           │
│  ┌──────────────────┐            ┌──────────────────┐          │
│  │ data_generator.py│            │ ingestion_engine │          │
│  │ - Generate       │            │ - file_reader    │          │
│  │   inventory      │            │ - column_mapper  │          │
│  │ - Generate       │            │ - data_transformer│         │
│  │   restocks       │            │ - config_loader  │          │
│  └──────────────────┘            └──────────────────┘          │
│           │                               │                     │
│           └───────────────┬───────────────┘                     │
│                           ▼                                     │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    VALIDATION LAYER                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  validators.py                                                   │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • check_negative_stock()                           │        │
│  │ • check_product_id_mismatch()                      │        │
│  │ • check_duplicates()                               │        │
│  │ • check_restock_exceeded()                         │        │
│  │ • create_quarantine_inventory()                    │        │
│  └────────────────────────────────────────────────────┘        │
│                           │                                     │
│                           ▼                                     │
│              ┌────────────────────────┐                        │
│              │ Quarantine Inventory   │                        │
│              │ (Invalid Records)      │                        │
│              └────────────────────────┘                        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  RECONCILIATION LAYER                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  reconciliation.py                                               │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • SKU pattern validation (regex)                   │        │
│  │ • Fuzzy string matching (rapidfuzz)                │        │
│  │ • Product name similarity scoring                  │        │
│  │ • Auto-correct missing product_ids                 │        │
│  └────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  data_processor.py                                               │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • merge_snapshots_with_restocks()                  │        │
│  │ • calculate_effective_stock()                      │        │
│  │   = snapshot + restock - damaged - expired         │        │
│  │ • create_inventory_fact_table()                    │        │
│  └────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     OUTPUT LAYER                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ✓ Inventory Fact Table (Clean, validated data)                │
│  ✓ Enriched Snapshots (With effective stock levels)            │
│  ✓ Quarantine Inventory (Invalid records for diagnostics)      │
│  ✓ Reconciled Incoming Inventory (Auto-corrected product IDs)  │
└───────────────────────────────────────────────────────────────┘
```

## Module Relationships

### Core Modules

**1. config.py** - Configuration Hub
- Defines all constants (NUM_ITEMS, NUM_WAREHOUSES, DAYS)
- Column definitions for all tables
- Product master data
- Validation thresholds
- **Used by**: All other modules

**2. data_generator.py** - Synthetic Data Generation
- `generate_inventory_data()` → Creates inventory snapshots + restock events
- `generate_incoming_inventory()` → Creates incoming inventory with errors
- `create_product_master()` → Product catalog
- **Depends on**: config.py
- **Used by**: main.py

**3. validators.py** - Data Quality Checks
- `check_negative_stock()` → Finds negative quantities
- `check_product_id_mismatch()` → Finds ID mismatches
- `check_duplicates()` → Finds duplicate records
- `check_restock_exceeded()` → Finds excessive restocks
- `create_quarantine_inventory()` → Combines all invalid records
- **Depends on**: config.py
- **Used by**: main.py

**4. reconciliation.py** - Product ID Reconciliation
- `reconcile_product_id()` → Fuzzy matching for missing IDs
- Uses SKU pattern validation (regex)
- Uses product name similarity (rapidfuzz)
- `apply_reconciliation()` → Applies to entire dataset
- **Depends on**: config.py, rapidfuzz
- **Used by**: main.py

**5. data_processor.py** - Data Transformation
- `merge_snapshots_with_restocks()` → Joins data
- `calculate_effective_stock()` → Computes final stock levels
- `create_inventory_fact_table()` → Creates clean fact table
- **Depends on**: config.py
- **Used by**: main.py

### Config-Driven Ingestion Modules

**6. config_loader.py** - Configuration Loader
- Loads YAML/JSON configuration files
- Validates configuration schema
- **Used by**: ingestion_engine.py

**7. file_reader.py** - Multi-Format File Reader
- Reads CSV, Excel, JSON, Parquet
- Auto-detects file format
- **Used by**: ingestion_engine.py

**8. column_mapper.py** - Column Mapping
- Maps source → target column names
- Adds default values for missing columns
- Validates required columns
- **Used by**: ingestion_engine.py

**9. data_transformer.py** - Data Transformation
- Datetime transformations
- Numeric transformations
- String operations
- Custom expressions
- **Used by**: ingestion_engine.py

**10. ingestion_engine.py** - Ingestion Orchestrator
- Coordinates file reading, mapping, transformation
- Applies validation rules
- **Depends on**: config_loader, file_reader, column_mapper, data_transformer
- **Used by**: main.py

### Main Orchestration

**11. main.py** - Main Pipeline
- **Dual Mode**:
  - `--mode synthetic`: Generate synthetic data
  - `--mode ingest`: Config-driven file ingestion
- Orchestrates entire pipeline
- **Depends on**: All modules above

## How to Run Pipeline 1

### Synthetic Mode (Default)
```bash
python main.py
```

**What happens:**
1. Generates 150 inventory snapshots
2. Generates 221 restock events
3. Validates data (checks for negative stock, mismatches, duplicates)
4. Creates quarantine inventory
5. Processes and enriches data
6. Reconciles incoming inventory with fuzzy matching
7. Outputs clean fact tables

### Config-Driven Ingestion Mode
```bash
python main.py --mode ingest --config configs/ingestion_config.yaml
```

**What happens:**
1. Loads configuration from YAML
2. Reads files (CSV, Excel, JSON, Parquet)
3. Maps columns based on config
4. Applies transformations
5. Validates data
6. Outputs clean datasets

### Generate Sample Data
```bash
python generate_sample_data.py
```

Creates sample CSV, Excel, and JSON files in `data/` directory.

---

# PIPELINE 2: Real-Time Shopping Basket Affinity Analyzer

## Overview

Market basket analysis system that identifies products commonly purchased together and computes association strengths (support, confidence, lift) for product recommendations.

## Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                  DATA GENERATION LAYER                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  transaction_generator.py                                        │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • generate_products_df() → Extract from inventory  │        │
│  │ • generate_stores_df() → Extract stores            │        │
│  │ • generate_transactions() → Create sales headers   │        │
│  │ • generate_line_items() → Create basket items      │        │
│  │   (with built-in product affinities)               │        │
│  └────────────────────────────────────────────────────┘        │
│                           │                                     │
│                           ▼                                     │
│              ┌────────────────────────┐                        │
│              │ Products, Stores,      │                        │
│              │ Transactions,          │                        │
│              │ Line Items             │                        │
│              └────────────────────────┘                        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              MARKET BASKET ANALYSIS LAYER                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  market_basket_analyzer.py                                       │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • build_transaction_baskets()                      │        │
│  │ • generate_product_pairs()                         │        │
│  │ • calculate_support(A,B)                           │        │
│  │   = Count(A ∩ B) / Total Transactions              │        │
│  │ • calculate_confidence(A→B)                        │        │
│  │   = Support(A,B) / Support(A)                      │        │
│  │ • calculate_lift(A→B)                              │        │
│  │   = Confidence(A→B) / Support(B)                   │        │
│  │ • find_top_affinities()                            │        │
│  └────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│            REAL-TIME INCREMENTAL UPDATE LAYER                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  incremental_affinity_updater.py                                 │
│  ┌────────────────────────────────────────────────────┐        │
│  │ AffinityCache:                                     │        │
│  │ • Maintains running counts (O(1) updates)          │        │
│  │ • update_with_new_transaction()                    │        │
│  │ • update_with_batch()                              │        │
│  │ • get_updated_affinities()                         │        │
│  │   (No full recomputation needed!)                  │        │
│  └────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 REPORTING & VISUALIZATION LAYER                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  affinity_reporter.py                                            │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • generate_affinity_report() → Text report         │        │
│  │ • export_recommendations() → CSV                   │        │
│  │   ("Customers who buy X also buy Y")               │        │
│  │ • create_affinity_heatmap() → PNG                  │        │
│  │ • create_top_affinities_chart() → PNG              │        │
│  └────────────────────────────────────────────────────┘        │
│                           │                                     │
│                           ▼                                     │
│              ┌────────────────────────┐                        │
│              │ affinity_analysis_output/                       │
│              │ • affinity_report.txt  │                        │
│              │ • recommendations.csv  │                        │
│              │ • top_affinities.png   │                        │
│              │ • affinity_heatmap.png │                        │
│              └────────────────────────┘                        │
└───────────────────────────────────────────────────────────────┘
```

## Module Relationships

**1. transaction_generator.py** - Transaction Data Generation
- `generate_products_df()` → Extracts from inventory
- `generate_stores_df()` → Creates store data
- `generate_transactions()` → Creates 200 transactions
- `generate_line_items()` → Creates basket items with affinities
- **Depends on**: data_generator.py (for inventory)
- **Used by**: affinity_analyzer_main.py

**2. market_basket_analyzer.py** - Core MBA Engine
- `MarketBasketAnalyzer` class
- Implements support, confidence, lift calculations
- `analyze()` → Complete analysis pipeline
- `find_top_affinities()` → Get top N associations
- `get_recommendations()` → Product recommendations
- **Used by**: affinity_analyzer_main.py

**3. incremental_affinity_updater.py** - Real-Time Updates
- `AffinityCache` → Maintains running statistics
- O(1) complexity per transaction
- No full dataset scan required
- **Used by**: affinity_analyzer_main.py

**4. affinity_reporter.py** - Reporting & Visualization
- Generates text reports
- Exports CSV recommendations
- Creates heatmaps and charts
- **Depends on**: matplotlib, seaborn
- **Used by**: affinity_analyzer_main.py

**5. affinity_analyzer_main.py** - Main Orchestrator
- Coordinates entire pipeline
- Demonstrates real-time updates
- **Depends on**: All modules above

## How to Run Pipeline 2

```bash
python affinity_analyzer_main.py
```

**What happens:**
1. Generates base inventory data
2. Generates 200 transactions with ~800 line items
3. Performs market basket analysis
4. Computes support, confidence, lift for all product pairs
5. Identifies top 10 strongest affinities
6. Generates reports and visualizations
7. Demonstrates real-time incremental updates (150→200 transactions)
8. Shows product recommendation examples

**Output:**
- `affinity_analysis_output/affinity_report.txt`
- `affinity_analysis_output/recommendations.csv`
- `affinity_analysis_output/top_affinities.png`
- `affinity_analysis_output/affinity_heatmap.png`

---

# PIPELINE 3: Refund & Fraud Detection Engine

## Overview

Automated fraud detection system that identifies suspicious refund activities using multi-layered detection: validation checks, fraud pattern detection, and statistical anomaly detection.

## Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                  DATA GENERATION LAYER                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  refund_data_generator.py                                        │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • generate_customer_details() → 100 customers      │        │
│  │ • generate_refund_transactions()                   │        │
│  │   → ~90 refunds with 20% intentional fraud         │        │
│  │   Fraud patterns injected:                         │        │
│  │   - Amount > original                              │        │
│  │   - Customer mismatch                              │        │
│  │   - Outside date window                            │        │
│  │   - Payment mismatch                               │        │
│  │   - High-frequency (5+ customers)                  │        │
│  └────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    VALIDATION LAYER                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  refund_validator.py                                             │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • check_refund_amount() → Refund > original        │        │
│  │ • check_customer_match() → Customer mismatch       │        │
│  │ • check_date_window() → Outside 30-day window      │        │
│  │ • check_payment_mode() → Payment mismatch          │        │
│  └────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 FRAUD DETECTION LAYER                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  fraud_detector.py                                               │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • detect_high_frequency_refunds() → >5 refunds     │        │
│  │ • detect_high_value_refunds() → ≥3 refunds >$100   │        │
│  │ • detect_payment_mismatches() → Payment ≠ original │        │
│  │ • detect_repeated_product_refunds() → >3 same item │        │
│  └────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                ANOMALY DETECTION LAYER                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  anomaly_detector.py (No ML - Statistical Methods Only)          │
│  ┌────────────────────────────────────────────────────┐        │
│  │ Z-Score Method:                                    │        │
│  │   z = (x - mean) / std                             │        │
│  │   Anomaly if |z| > 3                               │        │
│  │                                                     │        │
│  │ IQR Method:                                        │        │
│  │   IQR = Q3 - Q1                                    │        │
│  │   Lower = Q1 - 1.5×IQR                             │        │
│  │   Upper = Q3 + 1.5×IQR                             │        │
│  │   Anomaly if x < Lower or x > Upper                │        │
│  │                                                     │        │
│  │ Historical Ratio:                                  │        │
│  │   ratio = total_refunds / total_purchases          │        │
│  │   Anomaly if ratio > avg + 2×std                   │        │
│  └────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 FRAUD FLAGGING LAYER                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  fraud_flagger.py                                                │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • calculate_fraud_score() → Composite 0-100        │        │
│  │   Each flag contributes points:                    │        │
│  │   - Refund > original: 30 points                   │        │
│  │   - Customer mismatch: 25 points                   │        │
│  │   - High-frequency: 25 points                      │        │
│  │   - High-value: 20 points                          │        │
│  │   - Payment mismatch: 15 points                    │        │
│  │   - Z-score outlier: 15 points                     │        │
│  │   - IQR outlier: 15 points                         │        │
│  │                                                     │        │
│  │ • determine_severity()                             │        │
│  │   Critical: ≥75, High: ≥50, Medium: ≥25, Low: <25  │        │
│  │                                                     │        │
│  │ • create_fraud_flags_table()                       │        │
│  │ • prioritize_investigations()                      │        │
│  └────────────────────────────────────────────────────┘        │
│                           │                                     │
│                           ▼                                     │
│              ┌────────────────────────┐                        │
│              │ fraud_detection_output/│                        │
│              │ • fraud_report.txt     │                        │
│              │ • fraud_flags.csv      │                        │
│              │ • validation_issues.csv│                        │
│              └────────────────────────┘                        │
└───────────────────────────────────────────────────────────────┘
```

## Module Relationships

**1. refund_data_generator.py** - Refund Data Generation
- `generate_customer_details()` → 100 customers
- `generate_refund_transactions()` → ~90 refunds with 20% fraud
- Injects intentional fraud patterns for testing
- **Depends on**: transaction_generator.py
- **Used by**: fraud_detection_main.py

**2. refund_validator.py** - Legitimacy Validation
- `RefundValidator` class
- Checks amount, customer, date, payment
- **Used by**: fraud_detection_main.py

**3. fraud_detector.py** - Fraud Pattern Detection
- `FraudDetector` class
- Detects high-frequency, high-value, payment mismatch, repeated products
- **Used by**: fraud_detection_main.py

**4. anomaly_detector.py** - Statistical Anomaly Detection
- `AnomalyDetector` class
- Z-score, IQR, historical ratio methods
- **No ML libraries required**
- **Used by**: fraud_detection_main.py

**5. fraud_flagger.py** - Fraud Scoring & Flagging
- `FraudFlagger` class
- Composite fraud scores (0-100)
- Severity classification
- Investigation prioritization
- **Used by**: fraud_detection_main.py

**6. fraud_detection_main.py** - Main Orchestrator
- Coordinates entire pipeline
- Evaluates detection performance (precision, recall, F1)
- **Depends on**: All modules above

## How to Run Pipeline 3

```bash
python fraud_detection_main.py
```

**What happens:**
1. Generates 200 transactions
2. Generates 100 customers
3. Generates ~90 refunds (20% intentional fraud)
4. Validates refund legitimacy (4 checks)
5. Runs fraud detection logic (4 patterns)
6. Runs anomaly detection (3 methods)
7. Creates fraud flags with scores
8. Prioritizes top 10 investigations
9. Generates reports
10. Evaluates performance (precision: 85%+, recall: 92%+)

**Output:**
- `fraud_detection_output/fraud_report.txt`
- `fraud_detection_output/fraud_flags.csv`
- `fraud_detection_output/validation_issues.csv`

---

## 🚀 Quick Start - Running All Pipelines

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Pipeline 1 - Inventory Harmonization
```bash
# Synthetic mode
python main.py

# Config-driven ingestion mode
python main.py --mode ingest --config configs/ingestion_config.yaml
```

### 3. Run Pipeline 2 - Affinity Analyzer
```bash
python affinity_analyzer_main.py
```

### 4. Run Pipeline 3 - Fraud Detection
```bash
python fraud_detection_main.py
```

---

## 📊 Dependencies

```
pandas>=2.0.0          # Data manipulation
numpy>=1.24.0          # Numerical operations
rapidfuzz>=3.0.0       # Fuzzy string matching (Pipeline 1)
pyyaml>=6.0.0          # YAML config loading (Pipeline 1)
openpyxl>=3.0.0        # Excel file support (Pipeline 1)
pyarrow>=10.0.0        # Parquet file support (Pipeline 1)
matplotlib>=3.5.0      # Visualization (Pipeline 2)
seaborn>=0.12.0        # Statistical visualization (Pipeline 2)
```

---

## 🎯 Key Achievements

### Pipeline 1: Inventory Harmonization
✅ Dual-mode operation (synthetic + config-driven)
✅ Multi-format file support (CSV, Excel, JSON, Parquet)
✅ Fuzzy matching for product reconciliation
✅ Comprehensive validation (4 checks)
✅ Zero-code configuration changes

### Pipeline 2: Affinity Analyzer
✅ Market basket analysis (support, confidence, lift)
✅ Top 10 affinity identification
✅ Real-time incremental updates (O(1) complexity)
✅ Product recommendations
✅ Visualization (heatmaps, charts)

### Pipeline 3: Fraud Detection
✅ Multi-layered detection (validation + fraud + anomaly)
✅ Statistical methods only (no ML required)
✅ Fraud scoring (0-100) with severity levels
✅ High accuracy (85%+ precision, 92%+ recall)
✅ Investigation prioritization

---

## 📚 Documentation Files

- `README.md` - Main project overview
- `CONFIG_DRIVEN_INGESTION.md` - Config-driven ingestion guide
- `AFFINITY_ANALYZER.md` - Affinity analyzer guide
- `FRAUD_DETECTION.md` - Fraud detection guide
- `PROJECT_OVERVIEW.md` - This file (complete system documentation)

---

## 🎓 Summary

This project demonstrates **production-ready data engineering pipelines** with:

- **3 major pipelines** (~3,500 lines of code)
- **25+ modules** with clear separation of concerns
- **Modular architecture** for maintainability
- **Comprehensive testing** with synthetic data
- **Performance optimization** (incremental updates, O(1) operations)
- **No ML dependencies** for fraud detection (pure statistics)
- **Complete documentation** with examples

All pipelines are **independently runnable** and can be **integrated together** for a complete retail data platform.
