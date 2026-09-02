# 💳 Loan Credit Analysis — End-to-End Data Engineering Project

> A modern data engineering project for analyzing historical loan data through both **batch processing** and **streaming pipelines**, with a focus on credit-related patterns, data transformation, analytical modeling, and orchestration.

![Apache Spark](https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)
![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-29B5E8?logo=snowflake&logoColor=white)
![Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Language-Python-3776AB?logo=python&logoColor=white)

## 📌 Project Overview

The **Loan Credit Analysis** project explores historical lending data to identify patterns related to loan performance, borrower characteristics, hardship indicators, loan products, and credit-related outcomes.

The project is designed around two complementary processing modes:

- **Batch Processing** — historical datasets are transformed and loaded into analytical structures.
- **Streaming Processing** — Kafka is used to simulate continuous data ingestion and Spark/Python components process incoming events.

The original repository describes the project as a combination of batch and streaming processing in a modern data engineering pipeline. fileciteturn3file0

## 🏗️ Architecture

![Loan Credit Analysis Architecture](docs/pipeline-architecture.svg)

### End-to-End Flow

**Historical Data → Spark → Snowflake → BI/Analysis**

and for streaming:

**Events → Kafka → Streaming Processing → Analytical Storage**

**Airflow** is used as an orchestration layer to coordinate data engineering workflows.

The repository contains a Docker Compose environment with Airflow, PostgreSQL, Redis, Kafka/Zookeeper, and related services, supporting the project's orchestration and streaming setup. fileciteturn11file0

## 🧰 Technology Stack

| Technology | Role |
|---|---|
| Python | Data engineering and streaming logic |
| Apache Spark | Distributed batch/stream processing |
| Snowflake | Cloud analytical data warehouse |
| Apache Kafka | Event streaming / ingestion |
| Apache Airflow | Workflow orchestration |
| Docker Compose | Local multi-service environment |
| Jupyter Notebooks | Exploration, transformations, and loading workflows |
| Power BI / BI layer | Analytical consumption where applicable |

## 🧱 Data Warehouse Model

The repository includes a **Snowflake Model** diagram and notebooks dedicated to loading dimensions and fact data. fileciteturn7file0

The modeled entities include concepts such as:

- Borrower
- Second Borrower
- Loan Product
- Hardship
- Loan Fact

This dimensional approach separates descriptive borrower/product information from measurable loan business events, making the data easier to analyze and aggregate.

## 🔄 Batch Processing

The batch pipeline is implemented through a series of notebooks that transform and load historical loan data.

Examples include:

- `DIM_BORROWER_Loading.ipynb`
- `Dim_Hardship_load.ipynb`
- `Dim_LoanProduct_load.ipynb`
- `Dim_SecondBorrower_load.ipynb`
- `Fact_Table_Load.ipynb`
- `Transformations_2014_18.ipynb`
- `Transformations_2019_20.ipynb`

These notebooks show a practical progression from source transformation to dimensional and fact-table loading. fileciteturn7file0

## ⚡ Streaming Pipeline

The streaming component demonstrates how loan events can move through an event-driven architecture.

### Main Components

1. **Kafka Producer** — publishes loan-related events.
2. **Kafka** — acts as the event streaming platform.
3. **Streaming Processing** — consumes and transforms incoming events.
4. **Analytical Layer** — makes processed information available for downstream analysis.

The repository includes `kafka_producer.ipynb` and `load_to_DIM_Borrower_STREAMING.py` as part of the streaming implementation. fileciteturn7file0

## ⏱️ Orchestration with Airflow

Airflow is used to organize the pipeline into repeatable workflows rather than relying on manually executed scripts.

A production-style orchestration approach makes it possible to:

- Schedule recurring transformations
- Define dependencies between pipeline stages
- Monitor execution status
- Retry failed tasks
- Separate ingestion, transformation, and loading responsibilities

## 📊 Analytical Goals

The pipeline is designed to support analysis such as:

- Loan distribution by product and borrower characteristics
- Patterns in loan performance
- Hardship-related behavior
- Historical changes in lending activity
- Relationships between borrower attributes and credit outcomes
- Streaming ingestion of new borrower/loan events

## 📁 Repository Structure

```text
.
├── DIM_BORROWER_Loading.ipynb
├── Dim_Hardship_load.ipynb
├── Dim_LoanProduct_load.ipynb
├── Dim_SecondBorrower_load.ipynb
├── Fact_Table_Load.ipynb
├── Transformations_2014_18.ipynb
├── Transformations_2019_20.ipynb
├── kafka_producer.ipynb
├── load_to_DIM_Borrower_STREAMING.py
├── docker-compose.yml
├── docs/
│   └── pipeline-architecture.svg
└── README.md
```

The key notebooks and streaming files are present in the repository alongside the Docker Compose environment. fileciteturn7file0

## 🚀 Getting Started

### Prerequisites

- Docker Desktop
- Python 3.x
- Jupyter Notebook / JupyterLab
- Access to a Snowflake environment if the warehouse layer is required

### Start the Local Services

```bash
docker compose up -d
```

Check the Docker containers and service logs to confirm that the orchestration and streaming components are running.

### Run the Pipeline

A typical learning/demo sequence is:

1. Prepare the source loan data.
2. Run the transformation notebooks.
3. Load dimensions.
4. Load fact data.
5. Start the Kafka environment.
6. Run the Kafka producer.
7. Start the streaming consumer/processing logic.
8. Validate the resulting analytical data.

> **Security note:** The current Docker configuration contains development credentials and example secrets. Treat them as local-development values only and replace them with environment variables or a secrets manager before any production deployment. fileciteturn11file0

## 🎓 Learning Outcomes

This project is particularly useful for demonstrating practical knowledge of:

- Batch vs. streaming data engineering
- Dimensional data modeling
- Spark transformations
- Snowflake warehouse loading
- Kafka event-driven ingestion
- Airflow orchestration
- Dockerized data platforms
- Fact and dimension table design
- Building an end-to-end analytical pipeline

## 👨‍💻 Author

**Fady Elhosary** — Data Engineer

- LinkedIn: [Fady Elhosary](https://www.linkedin.com/in/fady-elhosary-68064a338/)
- Email: fadymohamed1@gmail.com

---

⭐ If this project helps you learn Data Engineering, feel free to star the repository.
