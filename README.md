# Customer Behavior Analytics Pipeline on Azure

##  Project Overview
This project builds an end-to-end data pipeline using Azure services to analyze customer behavior.

##  Architecture
- Azure Data Factory (Batch ingestion)
- Azure Event Hubs (Streaming ingestion)
- Azure Data Lake Gen2 (Storage)
- Azure Databricks (ETL + ML)
- Azure SQL Database (Serving layer)
- Power BI Desktop (Visualization)

##  Workflow
1. Data ingestion (ADF + Event Hubs)
2. Storage (Raw → Processed → Curated)
3. Processing (Databricks ETL + ML)
4. Reporting (Azure SQL + Power BI)

##  Use Cases

### 1. Customer Segmentation
- K-Means clustering
- Segments: Frequent Shoppers, Price-Sensitive Browsers

### 2. Cart Abandonment Analysis
- Streaming + batch simulation
- Identify top abandonment reasons

### 3. Recommendation Effectiveness
- Analyze recommendation impact on satisfaction

##  Tools Used
- Azure Data Factory
- Azure Databricks
- Azure Event Hubs
- Azure SQL Database
- Power BI Desktop

##  Dataset
Amazon Customer Behavior Survey Dataset

##  How to Run
1. Upload data to ADLS
2. Run ADF pipeline
3. Execute Databricks notebooks
4. Load results into Azure SQL
5. Open Power BI dashboard

## Dashboards
<img width="1142" height="623" alt="Screenshot 2026-03-17 133335" src="https://github.com/user-attachments/assets/c46f54a2-5bd7-4a5d-99bc-58322bbcdbdf" />
<img width="1103" height="586" alt="Screenshot 2026-03-17 133358" src="https://github.com/user-attachments/assets/d4772a99-b773-4141-845e-b9d18c156d62" />
<img width="1174" height="633" alt="Screenshot 2026-03-17 133412" src="https://github.com/user-attachments/assets/ad40dc2f-0ccf-4fb0-82bc-0c4a3fa29e32" />



