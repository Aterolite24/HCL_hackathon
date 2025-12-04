# Config-Driven Ingestion System

## Overview

The inventory harmonization pipeline now supports **config-driven file ingestion**, allowing you to ingest any inventory file format (CSV, Excel, JSON, Parquet) without code changes—just update the YAML configuration!

## 🚀 Quick Start

### Synthetic Mode (Default)
```bash
python main.py
```

### Config-Driven Ingestion Mode
```bash
python main.py --mode ingest --config configs/ingestion_config.yaml
```

## 📁 New File Structure

```
hcllll/
├── configs/
│   └── ingestion_config.yaml      # Main ingestion configuration
├── data/
│   ├── inventory_snapshot.csv     # Sample CSV file
│   ├── restock_events.xlsx        # Sample Excel file
│   └── incoming_inventory.json    # Sample JSON file
├── config_loader.py               # Configuration loader
├── file_reader.py                 # Multi-format file reader
├── column_mapper.py               # Column mapping engine
├── data_transformer.py            # Data transformation engine
├── ingestion_engine.py            # Main ingestion orchestrator
├── generate_sample_data.py        # Sample data generator
└── main.py                        # Updated with dual-mode support
```

## 📋 Configuration File Format

The `configs/ingestion_config.yaml` file defines how to ingest files:

```yaml
ingestion_configs:
  - name: inventory_snapshot
    file_path: data/inventory_snapshot.csv
    file_format: csv
    
    column_mapping:
      snapshot_id: SnapshotID      # target: source
      item_id: ItemID
      warehouse_id: WarehouseID
    
    transformations:
      - column: snapshot_timestamp
        type: datetime
        format: "%Y-%m-%d %H:%M:%S"
      
      - column: current_quantity
        type: integer
    
    defaults:
      max_stock_capacity: 100
      damaged_quantity: 0
    
    validation_rules:
      - column: current_quantity
        rule: non_negative
```

## 🔧 Configuration Options

### File Formats
- **CSV**: `file_format: csv`
- **Excel**: `file_format: excel` (specify `sheet_name` in `reader_options`)
- **JSON**: `file_format: json` (specify `orient` in `reader_options`)
- **Parquet**: `file_format: parquet`

### Column Mapping
Map any source column name to your target schema:
```yaml
column_mapping:
  target_column: SourceColumn
  item_id: ItemID
  product_id: ProductID
```

### Transformations

#### Datetime
```yaml
- column: timestamp
  type: datetime
  format: "%Y-%m-%d %H:%M:%S"
```

#### Numeric
```yaml
- column: quantity
  type: integer

- column: price
  type: float
  decimals: 2
```

#### String
```yaml
- column: product_id
  type: string
  operation: upper  # Options: upper, lower, title, strip
```

#### Boolean
```yaml
- column: is_active
  type: boolean
  true_values: [true, yes, 1, "Y"]
```

#### Custom
```yaml
- column: calculated_field
  type: custom
  expression: "x * 2 + 10"  # x is the column
```

### Default Values
Add columns with default values if missing:
```yaml
defaults:
  max_stock_capacity: 100
  damaged_quantity: 0
  status: "active"
```

### Validation Rules

#### Non-negative
```yaml
- column: quantity
  rule: non_negative
```

#### Non-null
```yaml
- column: product_id
  rule: non_null
```

#### Value list
```yaml
- column: warehouse_id
  rule: in_list
  values: [1, 2, 3]
```

#### Range
```yaml
- column: quantity
  rule: range
  min: 0
  max: 1000
```

#### Unique
```yaml
- column: snapshot_id
  rule: unique
```

## 🎯 Usage Examples

### Example 1: Ingest CSV with Custom Delimiter
```yaml
- name: custom_csv
  file_path: data/custom.csv
  file_format: csv
  reader_options:
    delimiter: "|"
    encoding: utf-8
```

### Example 2: Ingest Specific Excel Sheet
```yaml
- name: excel_data
  file_path: data/inventory.xlsx
  file_format: excel
  reader_options:
    sheet_name: "January"
```

### Example 3: Ingest Nested JSON
```yaml
- name: json_data
  file_path: data/inventory.json
  file_format: json
  reader_options:
    orient: records
```

## 🔄 Adding New File Sources

To add a new inventory file source:

1. **Add configuration** to `configs/ingestion_config.yaml`:
```yaml
- name: new_inventory_source
  file_path: data/new_source.csv
  file_format: csv
  column_mapping:
    item_id: Item_ID
    quantity: Qty
  transformations:
    - column: quantity
      type: integer
```

2. **Run ingestion**:
```bash
python main.py --mode ingest
```

**No code changes required!**

## 📊 Sample Data Generation

Generate sample data files for testing:
```bash
python generate_sample_data.py
```

This creates:
- `data/inventory_snapshot.csv` (50 records)
- `data/restock_events.xlsx` (30 records)
- `data/incoming_inventory.json` (20 records)

## 🏗️ Architecture

### Ingestion Pipeline Flow

```
Configuration File (YAML)
         ↓
   Config Loader
         ↓
    File Reader (CSV/Excel/JSON/Parquet)
         ↓
   Column Mapper (Rename columns)
         ↓
  Data Transformer (Apply transformations)
         ↓
    Validator (Check rules)
         ↓
   Ingested DataFrame
```

### Module Responsibilities

| Module | Responsibility |
|--------|---------------|
| `config_loader.py` | Load and validate YAML/JSON configs |
| `file_reader.py` | Read files in multiple formats |
| `column_mapper.py` | Map source → target columns |
| `data_transformer.py` | Apply data transformations |
| `ingestion_engine.py` | Orchestrate the pipeline |

## 🎓 Benefits

1. **Zero Code Changes**: Add new files via configuration only
2. **Format Agnostic**: Support CSV, Excel, JSON, Parquet
3. **Flexible Mapping**: Handle any column naming convention
4. **Reusable**: Same engine for all inventory types
5. **Maintainable**: Business users can update configs
6. **Testable**: Easy to test with different configurations
7. **Auditable**: Config changes tracked in version control

## 🔍 Troubleshooting

### File Not Found
Ensure the `file_path` in your config is correct (relative to project root).

### Column Not Found
Check that source column names in `column_mapping` match your file exactly (case-sensitive).

### Transformation Errors
Verify the transformation type matches your data (e.g., don't use `integer` on text fields).

### Validation Failures
Review validation rules and ensure your data meets the criteria.

## 📚 Advanced Features

### Conditional Transformations
Apply transformations only to specific rows:
```yaml
- condition: "warehouse_id == 1"
  transformation:
    column: quantity
    type: custom
    expression: "x * 1.1"
```

### Fill Missing Values
```yaml
fill_missing:
  quantity:
    method: value
    value: 0
  timestamp:
    method: forward  # Options: forward, backward, mean, median
```

## 🚀 Next Steps

1. Create your own configuration file
2. Point it to your inventory files
3. Run `python main.py --mode ingest --config your_config.yaml`
4. Integrate with your existing pipeline

No code changes needed—just configuration!
