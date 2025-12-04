# HCL_hackathon
HCL hackathon

1. Unified Product & Inventory Data Harmonization
   ```
          ┌────────────────────┐
          │   RAW INGESTION    │
          └───────┬────────────┘
                  │
        ┌─────────▼─────────┐
        │  PREPROCESSING     │
        │ clean + validate   │
        └─────────┬─────────┘
                  │
        ┌─────────▼─────────┐
        │  TRANSFORMATION    │
        │ compute stock,     │
        │ join tables        │
        └─────────┬─────────┘
                  │
      ┌───────────▼────────────┐
      │ ML RECONCILIATION ENG. │
      │ tfidf/embeddings+fuzzy │
      └───────┬───────┬────────┘
              │       │
     ┌────────▼───┐  ┌▼───────────┐
     │  CURATED    │  │ QUARANTINE │
     │ INVENTORY   │  │  RECORDS   │
     └─────────────┘  └────────────┘
```
2. Basket Pulse
```
RAW SALES STREAM (Kafka)
            |
            v
   Spark Structured Streaming
            |
   +--------+---------+
   |  Pre-processing  |
   | (Join, Grouping) |
   +--------+---------+
            |
            v
      DELTA LAKE TABLE (Bronze/Silver)
            |
   +--------v--------+
   |   BATCH Layer   |
   |   (FP-Growth)   |
   | (Full historical)|
   +--------+--------+
            |
            v
   +-----------------------+
   |      RULE STORAGE     |
   | (Redis Cache/API DB)  |
   +-----------------------+
            |
            v
        FRONTEND (Streamlit)

```

3. Refund Fraud Detection
```
             ┌────────────────┐
             │  RAW DATA      │
             │ refunds.csv    │
             │ sales_header   │
             │ line_items     │
             │ customer_data  │
             └───────┬────────┘
                     │
             ┌───────▼────────┐
             │ Preprocessing   │
             │ clean + join    │
             │ validate rules  │
             └───────┬────────┘
                     │
             ┌───────▼────────┐
             │ Transformation   │
             │ domain feats     │
             │ stats feats      │
             │ behavior feats   │
             └───────┬────────┘
                     │
        ┌────────────┼──────────────────┐
        │            │                  │
┌───────▼──────┐ ┌───▼─────────┐ ┌─────▼────────┐
│ Rule Engine  │ │ Stats Model  │ │ ML Model     │
│ flags risks  │ │ Z-score/IQR  │ │ LightGBM     │
└───────┬──────┘ └────┬────────┘ └────┬──────────┘
        │              │               │
        └──────────────┼───────────────┘
                       ▼
              ┌─────────────────┐
              │ FINAL FRAUD     │
              │   SCORE         │
              │ fraud_flags tbl │
              └─────────────────┘

```
5
```
                ┌─────────────────────┐
                │   Raw Data Sources  │
                │ products, sales,    │
                │ inventory, competitor│
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────┐
                │  Data Preprocessing │
                │ clean, merge, impute│
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────┐
                │  Feature Engineering │
                │ velocity, elasticity │
                │ stock ratios, gaps   │
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────┐
                │     ML Model        │
                │ (RF / XGB / Linear) │
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────┐
                │ Price Recommendation │
                │ new_price, markdown  │
                └──────────────────────┘

```





