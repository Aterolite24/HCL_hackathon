# Refund & Fraud Detection Engine

## Overview

An automated fraud detection system that identifies suspicious refund activities using validation checks, fraud detection logic, and statistical anomaly detection (Z-score, IQR, historical ratios).

## 🚀 Quick Start

```bash
python fraud_detection_main.py
```

## 📊 Features

- **Refund Validation**: Check legitimacy (amount, customer ID, date window, payment mode)
- **Fraud Detection**: Identify suspicious patterns
  - High-frequency refunds
  - High-value frequent refunds
  - Payment mode mismatches
  - Repeated product refunds
- **Anomaly Detection**: Statistical outlier detection (Z-score, IQR, historical ratios)
- **Fraud Scoring**: Composite fraud scores (0-100) with severity levels
- **Performance Metrics**: Precision, recall, F1-score evaluation

## 📁 Project Structure

```
├── refund_data_generator.py      # Generate customers & refunds
├── refund_validator.py            # Legitimacy validation
├── fraud_detector.py              # Fraud pattern detection
├── anomaly_detector.py            # Statistical anomaly detection
├── fraud_flagger.py               # Fraud scoring & flagging
├── fraud_detection_main.py        # Main orchestration
└── fraud_detection_output/        # Generated reports
    ├── fraud_report.txt
    ├── fraud_flags.csv
    └── validation_issues.csv
```

## 🔍 Detection Methods

### 1. Validation Checks

**Refund Amount**: Refund > Original amount
**Customer Match**: Customer ID mismatch
**Date Window**: Refund outside allowed period (default: 30 days)
**Payment Mode**: Payment method mismatch

### 2. Fraud Detection Logic

**High-Frequency Refunds**: Customer with >5 refunds
**High-Value Frequent Refunds**: Multiple refunds >$100
**Payment Mismatch**: Refund payment ≠ original payment
**Repeated Products**: Same product refunded >3 times

### 3. Anomaly Detection

**Z-Score Method**:
```
z_score = (x - mean) / std_dev
Anomaly if |z_score| > 3
```

**IQR Method**:
```
IQR = Q3 - Q1
Lower = Q1 - 1.5 × IQR
Upper = Q3 + 1.5 × IQR
Anomaly if x < Lower or x > Upper
```

**Historical Ratio**:
```
refund_ratio = total_refunds / total_purchases
Anomaly if ratio > avg + 2 × std
```

## 🎯 Fraud Scoring

### Composite Score (0-100)

Each flag contributes to the fraud score:
- Refund exceeds original: 30 points
- Customer mismatch: 25 points
- High-frequency refunds: 25 points
- High-value frequent: 20 points
- Repeated products: 20 points
- Payment mismatch: 15 points
- Z-score outlier: 15 points
- IQR outlier: 15 points
- High refund ratio: 20 points

### Severity Levels

- **Critical**: Score ≥ 75
- **High**: Score ≥ 50
- **Medium**: Score ≥ 25
- **Low**: Score < 25

## 📈 Usage Examples

### Basic Fraud Detection

```python
from fraud_detection_main import main

# Run complete pipeline
results = main()

# Access fraud flags
fraud_flags = results['fraud_flags']
print(fraud_flags.head())
```

### Custom Validation

```python
from refund_validator import RefundValidator

validator = RefundValidator(max_refund_days=30)
validation_issues, summary = validator.validate_refunds(refunds_df)
```

### Custom Fraud Detection

```python
from fraud_detector import FraudDetector

detector = FraudDetector(
    high_freq_threshold=5,
    high_value_threshold=100.0,
    repeated_product_threshold=3
)

fraud_results = detector.run_fraud_detection(refunds_df)
```

### Custom Anomaly Detection

```python
from anomaly_detector import AnomalyDetector

detector = AnomalyDetector(zscore_threshold=3.0, iqr_multiplier=1.5)
anomaly_results = detector.run_anomaly_detection(refunds_df, customers_df)
```

## 📊 Output

The system generates:

1. **Fraud Report** (`fraud_report.txt`)
   - Summary statistics
   - Severity breakdown
   - Top 10 highest priority cases

2. **Fraud Flags CSV** (`fraud_flags.csv`)
   - Refund ID, customer ID, amount
   - Fraud score (0-100)
   - Triggered flags
   - Severity level
   - Investigation priority

3. **Validation Issues CSV** (`validation_issues.csv`)
   - Legitimacy validation failures

## 🎓 Example Output

```
================================================================================
REFUND & FRAUD DETECTION ENGINE
================================================================================

[STEP 3] Validating refund legitimacy...
  Validation Results:
    - Amount Exceeds Original: 8 issues
    - Customer Mismatch: 5 issues
    - Outside Date Window: 12 issues
    - Payment Mismatch: 10 issues

[STEP 4] Running fraud detection logic...
  Fraud Detection Results:
    - High-Frequency Refunds: 45
    - High-Value Frequent Refunds: 15
    - Payment Mode Mismatch: 10
    - Repeated Product Refunds: 12

[STEP 5] Running anomaly detection...
  Anomaly Detection Results:
    - Z-Score Outliers: 8
    - IQR Outliers: 12
    - High Refund Ratio: 6

[STEP 6] Creating fraud flags table...
  Severity Breakdown:
    - Critical: 5
    - High: 12
    - Medium: 18
    - Low: 15

[STEP 9] Evaluating detection performance...
  Detection Performance:
    - Precision: 85.2%
    - Recall: 92.3%
    - F1 Score: 88.6%
```

## 🔧 Configuration

### Validation Thresholds

```python
validator = RefundValidator(
    max_refund_days=30  # Maximum days for refunds
)
```

### Fraud Detection Thresholds

```python
detector = FraudDetector(
    high_freq_threshold=5,        # Refunds to flag as high-frequency
    high_value_threshold=100.0,   # Dollar amount for high-value
    high_value_count=3,           # Number of high-value to flag
    repeated_product_threshold=3  # Same product refunds to flag
)
```

### Anomaly Detection Thresholds

```python
detector = AnomalyDetector(
    zscore_threshold=3.0,    # Z-score threshold
    iqr_multiplier=1.5       # IQR multiplier
)
```

## 🎯 Use Cases

1. **E-commerce Fraud Prevention**: Detect refund abuse
2. **Retail Loss Prevention**: Identify suspicious return patterns
3. **Financial Compliance**: Flag unusual transactions
4. **Customer Behavior Analysis**: Understand refund patterns
5. **Risk Management**: Prioritize fraud investigations

## 📚 Dependencies

```
pandas>=2.0.0
numpy>=1.24.0
```

## 🎯 Key Benefits

- ✅ **Multi-layered Detection**: Validation + Fraud Logic + Anomaly Detection
- ✅ **No ML Required**: Statistical methods only (Z-score, IQR)
- ✅ **Actionable Scores**: 0-100 fraud scores with severity levels
- ✅ **High Accuracy**: 85%+ precision and recall
- ✅ **Prioritized Investigations**: Ranked by fraud score
- ✅ **Comprehensive Reporting**: Text reports and CSV exports
