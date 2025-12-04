# HCL_hackathon
HCL hackathon

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
