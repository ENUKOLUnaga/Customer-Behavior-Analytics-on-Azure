# Customer Behavior Analytics Pipeline on Azure

## 📌 Project Overview

This project demonstrates a modern end-to-end data engineering and analytics pipeline built on Microsoft Azure to process and analyze customer behavior data.

The pipeline integrates:

* Batch Data Ingestion
* Real-Time Streaming
* ETL Processing
* Machine Learning Analytics
* Dashboard Reporting

🎯 **Goal:** Transform raw customer interaction data into actionable business insights for:

* Customer Segmentation
* Cart Abandonment Analysis
* Recommendation Effectiveness Analysis

---

## 🔹 Step 1: Data Ingestion

### Batch Ingestion (Azure Data Factory)

* Ingest historical customer behavior datasets (CSV)
* Load data into Azure Data Lake Storage Gen2

### Streaming Ingestion (Azure Event Hubs)

Capture real-time customer events such as:

* Cart abandonment events
* Recommendation interactions
* Customer feedback activities

---

## 🔹 Step 2: Data Storage

Data is stored in **Azure Data Lake Storage Gen2** using a layered architecture.

### Raw Zone

Stores original unprocessed data.

Example:

```
raw/amazon_customer_behavior.csv
```

### Processed Zone

Stores cleaned and transformed datasets.

### Curated Zone

Stores business-ready analytical datasets.

Example:

```
curated.customer_segments
curated.cart_abandonment_analysis
curated.recommendation_effectiveness
```

---

## 🔹 Step 3: Data Processing (ETL)

Processing is performed using **Azure Databricks (Apache Spark)**.

### ETL Tasks

* Data cleaning
* Data validation
* Data transformation
* Feature engineering
* Aggregation
* Machine Learning model execution

---

## Use Cases

### 1️⃣ Customer Segmentation

Apply K-Means Clustering to group customers based on behavioral patterns.

#### Segments Identified

* Frequent Shoppers
* Price-Sensitive Browsers
* High-Value Customers
* Occasional Buyers

#### Output Table

```
dbo.CustomerSegments
```

---

### 2️⃣ Cart Abandonment Analysis

Analyze customer shopping cart activities using batch and streaming data.

#### Objectives

* Identify top abandonment reasons
* Track abandonment trends
* Improve conversion rates

#### Output Table

```
dbo.CartAbandonmentAnalysis
```

---

### 3️⃣ Recommendation Effectiveness Analysis

Measure the impact of product recommendations on customer satisfaction and engagement.

#### Objectives

* Analyze recommendation acceptance rates
* Measure customer satisfaction improvements
* Evaluate recommendation performance

#### Output Table

```
dbo.RecommendationEffectiveness
```

---

## 🔹 Step 4: Reporting Layer

### Database

**Azure SQL Database** serves as the reporting layer.

### Tables

```
dbo.CustomerSegments
dbo.CartAbandonmentAnalysis
dbo.RecommendationEffectiveness
```

---

## 📊 Visualization

Built using **Power BI Desktop**.

### Dashboards

#### Customer Segmentation Dashboard

* Customer segment distribution
* Segment-wise spending behavior
* Customer engagement insights

#### Cart Abandonment Dashboard

* Top abandonment reasons
* Abandonment trends over time
* Cart recovery opportunities

#### Recommendation Effectiveness Dashboard

* Recommendation acceptance rates
* Customer satisfaction scores
* Product recommendation performance

---

## 📈 Key Insights

* Customer purchasing behavior patterns
* High-value customer identification
* Major cart abandonment drivers
* Recommendation impact on customer engagement
* Customer satisfaction trends

---

## 📂 Dataset

**Amazon Customer Behavior Survey Dataset**

---

## 🛠 Technologies Used

| Category         | Technology                        |
| ---------------- | --------------------------------- |
| Cloud Platform   | Microsoft Azure                   |
| Ingestion        | Azure Data Factory                |
| Streaming        | Azure Event Hubs                  |
| Storage          | Azure Data Lake Storage Gen2      |
| Processing       | Azure Databricks                  |
| Database         | Azure SQL Database                |
| Visualization    | Power BI Desktop                  |
| Language         | Python / PySpark                  |
| Machine Learning | Scikit-Learn (K-Means Clustering) |

---

## 🏗 Architecture

```
Azure Data Factory (Batch Ingestion)
                │
                ▼
Azure Data Lake Storage Gen2
                │
                ▼
Azure Databricks (ETL + ML Processing)
                │
                ▼
Azure SQL Database (Serving Layer)
                │
                ▼
Power BI Desktop (Visualization)
```

---

## 🚀 Project Outcomes

* Scalable cloud-based analytics pipeline
* Real-time and batch data processing
* Automated ETL workflows
* Machine learning-driven customer insights
* Interactive business dashboards
* Enhanced customer behavior understanding
* Data-driven decision-making support

---

## ▶️ How to Run

### Step 1

Upload the dataset to Azure Data Lake Storage Gen2.

### Step 2

Run the Azure Data Factory pipeline for batch ingestion.

### Step 3

Start Azure Event Hubs streaming simulation.

### Step 4

Execute Azure Databricks notebooks for ETL and machine learning processing.

### Step 5

Load processed data into Azure SQL Database.

### Step 6

Open the Power BI Desktop dashboard and refresh the data source.


## Dashboards
<img width="1142" height="623" alt="Screenshot 2026-03-17 133335" src="https://github.com/user-attachments/assets/c46f54a2-5bd7-4a5d-99bc-58322bbcdbdf" />
<img width="1103" height="586" alt="Screenshot 2026-03-17 133358" src="https://github.com/user-attachments/assets/d4772a99-b773-4141-845e-b9d18c156d62" />
<img width="1174" height="633" alt="Screenshot 2026-03-17 133412" src="https://github.com/user-attachments/assets/ad40dc2f-0ccf-4fb0-82bc-0c4a3fa29e32" />

## conclusion

This project demonstrates an end-to-end Customer Behavior Analytics Pipeline on Azure, integrating batch and streaming data processing. It analyzes customer behavior using machine learning techniques such as clustering and provides actionable insights through Power BI dashboards. The solution helps in understanding customer segments, reducing cart abandonment, and improving recommendation effectiveness, enabling data-driven decision-making.



