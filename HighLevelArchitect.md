## 🏗️ Architecture Summary

This project implements a modular **lakehouse-style data pipeline** using **PySpark**, **Apache Airflow**, and **MinIO** to simulate an end-to-end emissions analytics workflow. The architecture is designed for flexibility, observability, and ease of migration to cloud platforms like AWS.

### 1. **Data Generation Layer**

A synthetic data generator (`generate_emissions_data.py`) creates 1,000+ rows of emissions data with realistic variability and intentional quality issues. This data mimics digital advertising metrics such as domain coverage, emissions types, country, device, and format, reflecting common patterns in ad tech analytics.

### 2. **Storage Layer**

Raw and processed data is stored in **MinIO**, which emulates Amazon S3. Data is written and read using the `s3a://` protocol from PySpark, allowing easy transition to AWS S3 if needed. All output artifacts (cleaned data, business logic outputs) are stored as Parquet files under distinct output prefixes.

### 3. **Orchestration Layer**

An **Apache Airflow DAG** (`emissions_pipeline_dag.py`) coordinates all steps in the pipeline:

* `generate_data`: (optional) generates raw data
* `ingest_to_minio`: uploads CSV to MinIO
* `clean_data`: runs PySpark job to cleanse and validate data
* `process_business_logic`: executes PySpark aggregations and flags anomalies
  Each task has built-in retry and error logging for robustness.

### 4. **Processing & Transformation Layer**

All core transformation logic is implemented in **PySpark**, making it scalable for large datasets. Business logic includes:

* Aggregations by country, domain, device, format
* Percentage breakdown of emissions types
* Anomaly detection on emission values
* Top-N domain analysis

The pipeline is modular, with independent scripts for each stage (`clean_data.py`, `process_business_logic.py`), making it testable and maintainable.

### 5. **Model Layer (In-Progress)**

The project includes a placeholder for **dbt models**, which will support reusable, testable SQL logic built on the staged and curated layers. Once integrated, dbt will power semantic modeling, metrics standardization, and unit testing.

### 6. **Deployment & Environment**

All components run in **Docker containers** orchestrated via `docker-compose`. This provides a fully portable local development environment that closely resembles cloud-native infrastructure.

---

<pre>
┌────────────────────────────┐
│  Synthetic Data Generator  │
│ (generate_emissions_data) │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     Raw CSV File (Local)   │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│    Ingest to MinIO Bucket  │
│  (s3a://raw_data via Airflow) │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     Clean Data with Spark  │
│     (clean_data.py task)   │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│  Staged Data (s3a://cleaned)│
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│ Apply Business Logic (Spark)│
│ (process_business_logic.py) │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│ Final Output (Parquet files)│
│ (s3a://output/metrics)     │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     (Optional) dbt Models  │
│    (on staged/output data) │
└────────────────────────────┘
</pre>
