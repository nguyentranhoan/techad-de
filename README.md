# Hoan Nguyen's Implementation

This repository is my implementation of the Senior Data Engineer take-home assignment. It features a modular data pipeline using **PySpark**, **Apache Airflow**, **Docker**, **MinIO**, and **dbt (in-progress)** to simulate a production-grade emissions data processing system.

---

## 🚀 Overview

This project builds a complete data pipeline to process **digital advertising emissions data**. The pipeline flows through:

1. **Data Generation**
2. **Ingestion**
3. **Cleaning**
4. **Transformation**
5. **Business Logic & Aggregation**
6. **Final Output to MinIO (S3-compatible)**
7. **(WIP) DBT model development**

---

## 📁 Project Structure

```
techad-de/
├── dags/
│   └── emissions_pipeline_dag.py     # Main Airflow DAG
├── scripts/
│   ├── ingest_raw_data.py            # Load data from local → MinIO
│   ├── clean_data.py                 # Spark cleaning logic
│   └── process_business_logic.py     # Business logic & aggregations
├── data_generator/
│   └── generate_emissions_data.py    # Synthetic data generator (1,000+ rows)
├── dbt/                              # (In-progress) dbt models
├── tests/                            # Unit and integration tests (WIP)
├── docker-compose.yml                # Local Docker setup for Airflow & MinIO
├── .env                              # Environment config for MinIO, etc.
└── README.md                         # Project documentation
```

---

## ⚙️ Technologies Used

| Component       | Tool/Stack                   |
| --------------- | ---------------------------- |
| Orchestration   | Apache Airflow               |
| Transformations | PySpark, (dbt – in progress) |
| Storage         | MinIO (S3-compatible)        |
| Infrastructure  | Docker                       |

---

## 📄 Data Schema

The input dataset includes:

- `date`: MM/DD/YYYY
- `domain`: e.g., `cnn.com`
- `format`: e.g., `banner`, `video`
- `country`: e.g., `US`, `FR`
- `ad_size`: e.g., `300x250`
- `device`: `desktop`, `mobile`
- `adSelectionEmissions`, `creativeDistributionEmissions`, `mediaDistributionEmissions`: grams CO₂
- `totalEmissions`: sum of above emissions
- `domainCoverage`: `"measured"` or `"modeled"`

Synthetic data includes **intentional anomalies** for data quality testing.

---

## 🧬 DAG: `emissions_pipeline_dag.py`

Main DAG manages:

1. **`generate_data`** – Optionally generates synthetic emissions data
2. **`ingest_to_minio`** – Upload raw CSV to MinIO
3. **`clean_data`** – Run PySpark cleaning job
4. **`process_business_logic`** – Run PySpark aggregations and output logic
5. **(Future)** – `dbt_transform` (placeholder for dbt model integration)

Each task includes error handling and retry logic for reliability.

---

## 📊 Business Logic Highlights

Implemented via `process_business_logic.py`:

- **Top 10 domains by total emissions**
- **% Contribution of each emission type**
- **Daily country-level emission trends**
- **Flagging outliers / anomalies**
- **(WIP)** Additional metrics & tests via dbt

Output stored as Parquet files in MinIO under `output/<dataset-name>/`.

---

## 🧪 Testing

Testing framework to be added under `/tests/` for:

- Data quality validations
- Schema and format checks
- Business logic validation

---

## 🐳 Local Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/nguyentranhoan/techad-de.git
cd techad-de
```

### 2. Start Docker Environment

```bash
docker-compose up --build
```

This spins up:

- Airflow (webserver, scheduler)
- MinIO (S3-compatible bucket)

### 3. Access Services

- **Airflow UI**: [http://localhost:8080](http://localhost:8080)
- **MinIO UI**: [http://localhost:9001](http://localhost:9001)
  Default creds: `minioadmin:minioadmin`

### 4. Trigger DAG

Once Airflow is up, trigger `emissions_pipeline_dag` manually from the UI.

---

## 🧭 Next Steps

- [ ] Finalize and test dbt models
- [ ] Add CI test automation
- [ ] Deploy to AWS: S3 + Glue + Athena (optional)
- [ ] Improve anomaly detection logic
- [ ] Add documentation for each PySpark script

---

## 📌 Credits & Disclaimer

This project is based on the Take-Home Assessment and uses their [starter repository](https://github.com//p39-sde-exercise) as a reference.
**Note:** This solution is intended strictly for evaluation purposes.

---

Would you like this written to `README.md` directly or added as a Markdown file?
