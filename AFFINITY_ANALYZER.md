# Shopping Basket Affinity Analyzer

## Overview

A real-time market basket analysis system that identifies products commonly purchased together and computes association strengths (support, confidence, lift) for product recommendations.

## 🚀 Quick Start

```bash
python affinity_analyzer_main.py
```

## 📊 Features

- **Market Basket Analysis**: Compute support, confidence, and lift for product pairs
- **Top 10 Affinities**: Identify strongest product associations
- **Product Recommendations**: "Customers who buy X also buy Y"
- **Real-Time Updates**: Incremental affinity computation for streaming transactions
- **Visualizations**: Heatmaps and charts for affinity analysis
- **Performance Optimized**: O(1) updates per transaction

## 📁 Project Structure

```
├── transaction_generator.py          # Generate transaction data
├── market_basket_analyzer.py         # Core MBA engine
├── incremental_affinity_updater.py   # Real-time updates
├── affinity_reporter.py              # Reporting & visualization
├── affinity_analyzer_main.py         # Main orchestration
└── affinity_analysis_output/         # Generated reports
    ├── affinity_report.txt
    ├── recommendations.csv
    ├── top_affinities.png
    └── affinity_heatmap.png
```

## 🎯 Market Basket Analysis Metrics

### Support
Measures how frequently an itemset appears:
```
Support(A, B) = Transactions containing both A and B / Total transactions
```

### Confidence
Measures how often B is purchased when A is purchased:
```
Confidence(A → B) = Support(A, B) / Support(A)
```

### Lift
Measures strength of association:
```
Lift(A → B) = Confidence(A → B) / Support(B)
```

**Interpretation:**
- **Lift > 1**: Positive correlation (products bought together)
- **Lift = 1**: No correlation (independent)
- **Lift < 1**: Negative correlation (substitutes)

## 📈 Usage Examples

### Basic Analysis

```python
from transaction_generator import generate_complete_transaction_data
from market_basket_analyzer import MarketBasketAnalyzer
from data_generator import generate_inventory_data

# Generate data
inventory_df, _ = generate_inventory_data()
txn_data = generate_complete_transaction_data(inventory_df, num_transactions=200)

# Analyze
analyzer = MarketBasketAnalyzer(min_support=0.01, min_confidence=0.1)
results = analyzer.analyze(txn_data['store_sales_line_items'], txn_data['products'])

# Get top affinities
top_10 = analyzer.find_top_affinities(results, top_n=10)
print(top_10)
```

### Real-Time Incremental Updates

```python
from incremental_affinity_updater import IncrementalAffinityUpdater

# Initialize with historical data
updater = IncrementalAffinityUpdater(min_support=0.01, min_confidence=0.1)
updater.initialize_from_line_items(historical_line_items)

# Process new transaction
updater.process_new_transaction(new_line_items)

# Get updated affinities (O(1) computation)
current_affinities = updater.get_current_affinities()
```

### Product Recommendations

```python
# Get recommendations for a specific product
recommendations = analyzer.get_recommendations(results, product_id='P001', top_n=5)
print(recommendations)
```

## 📊 Output

The analyzer generates:

1. **Text Report** (`affinity_report.txt`)
   - Summary statistics
   - Top 10 affinities with metrics

2. **Recommendations CSV** (`recommendations.csv`)
   - "Customers who buy X also buy Y" format
   - Support, confidence, lift scores

3. **Visualizations**
   - `top_affinities.png` - Bar charts of top affinities
   - `affinity_heatmap.png` - Heatmap of product affinities

## 🔧 Configuration

### Minimum Thresholds

```python
analyzer = MarketBasketAnalyzer(
    min_support=0.01,      # Minimum 1% of transactions
    min_confidence=0.1     # Minimum 10% confidence
)
```

### Transaction Generation

```python
txn_data = generate_complete_transaction_data(
    inventory_df,
    num_transactions=200,  # Number of transactions
    seed=42                # Random seed for reproducibility
)
```

## 🚀 Performance Optimization

### For Large Datasets

1. **Increase Minimum Support**: Filter infrequent items early
```python
analyzer = MarketBasketAnalyzer(min_support=0.05)  # 5% threshold
```

2. **Use Incremental Updates**: Avoid full recomputation
```python
updater = IncrementalAffinityUpdater()
updater.process_new_batch(new_transactions)
```

3. **Spark/Delta Lake Integration** (for production):
```python
# Materialized view for frequent pairs
CREATE MATERIALIZED VIEW frequent_pairs AS
SELECT product_a, product_b, COUNT(*) as pair_count
FROM transaction_pairs
GROUP BY product_a, product_b
HAVING COUNT(*) >= min_support_threshold
```

## 📊 Real-Time Streaming

The incremental updater supports real-time transaction streams:

```python
# Initialize cache
updater = IncrementalAffinityUpdater()

# Stream processing loop
for new_transaction in transaction_stream:
    updater.process_new_transaction(new_transaction)
    
    # Get updated affinities every N transactions
    if transaction_count % 100 == 0:
        current_affinities = updater.get_current_affinities()
        update_recommendations(current_affinities)
```

## 🎓 Example Output

```
Top 10 Product Affinities (by Lift):

1. Milk 1L → Bread Loaf
   Lift: 2.45 | Confidence: 0.68 | Support: 0.15

2. Coffee 500g → Sugar 1kg
   Lift: 2.31 | Confidence: 0.72 | Support: 0.12

3. Apple Juice 1L → Banana Chips
   Lift: 2.18 | Confidence: 0.65 | Support: 0.14
```

## 🔍 Use Cases

1. **Product Recommendations**: Suggest complementary products
2. **Store Layout Optimization**: Place related products nearby
3. **Promotional Bundling**: Create product bundles
4. **Inventory Management**: Stock related products together
5. **Cross-Selling**: Recommend products at checkout

## 📚 Dependencies

```
pandas>=2.0.0
numpy>=1.24.0
matplotlib>=3.5.0
seaborn>=0.12.0
```

## 🎯 Key Benefits

- ✅ **Real-Time**: Incremental updates with O(1) complexity
- ✅ **Scalable**: Optimized for large transaction datasets
- ✅ **Actionable**: Direct product recommendations
- ✅ **Visual**: Charts and heatmaps for insights
- ✅ **Flexible**: Configurable thresholds and metrics
