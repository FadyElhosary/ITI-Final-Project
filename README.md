# Loan Credit Analysis — End-to-End Data Engineering Project

An end-to-end Data Engineering project built around historical loan data. It combines batch processing, streaming ingestion, dimensional modeling, orchestration, and cloud data warehousing.

![Apache Spark](https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)
![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-29B5E8?logo=snowflake&logoColor=white)
![Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Language-Python-3776AB?logo=python&logoColor=white)

## About the project

This was my ITI Data Engineering final project. I wanted to build something that covered more than a single ETL script, so the project combines two common data-processing patterns: batch processing for historical data and streaming for new events.

The batch pipeline transforms historical loan data and loads it into a dimensional model in Snowflake. The streaming side uses Kafka to simulate incoming loan-related events. Airflow is used to organize and orchestrate the workflows.

The repository contains the transformation and loading notebooks, Kafka producer, streaming process, Docker environment, and warehouse model. fileciteturn7file0

## Architecture

![Loan Credit Analysis Architecture](docs/pipeline-architecture.svg)

The simplified flow is:

```text
Historical Data -> Spark -> Snowflake -> Analysis

New Events -> Kafka -> Streaming Processing -> Analytical Storage
                         ^
                         |
                      Airflow
```

The local Docker environment includes Airflow, PostgreSQL, Redis, Kafka, and Zookeeper services. fileciteturn11file0

## Data model

The warehouse separates descriptive entities from measurable loan events.

The main entities are:

- Borrower
- Second Borrower
- Loan Product
- Hardship
- Loan Fact

The repository includes a Snowflake model diagram and separate notebooks for loading dimensions and facts. fileciteturn7file0

## Batch processing

The batch pipeline is split across several notebooks. The general process is:

1. Prepare the historical source data.
2. Apply transformations and data cleaning.
3. Load the required dimensions.
4. Load the fact table.
5. Validate the resulting analytical data.

Main notebooks include:

- `Transformations_2014_18.ipynb`
- `Transformations_2019_20.ipynb`
- `DIM_BORROWER_Loading.ipynb`
- `Dim_Hardship_load.ipynb`
- `Dim_LoanProduct_load.ipynb`
- `Dim_SecondBorrower_load.ipynb`
- `Fact_Table_Load.ipynb`

## Streaming processing

The streaming part demonstrates a simple event-driven pipeline.

A producer publishes loan-related events to Kafka. A Python streaming process consumes the events, transforms them, and loads the resulting records into the analytical layer.

The main streaming files are `kafka_producer.ipynb` and `load_to_DIM_Borrower_STREAMING.py`. fileciteturn7file0

## Orchestration with Airflow

Airflow is used to turn the different processing steps into workflows with dependencies rather than relying only on manual execution.

This makes the pipeline easier to schedule, monitor, and retry when a task fails.

## Technology stack

| Technology | Role |
|---|---|
| Python | Pipeline and streaming logic |
| Apache Spark | Data transformation and processing |
| Snowflake | Analytical warehouse |
| Apache Kafka | Event streaming |
| Apache Airflow | Workflow orchestration |
| Docker | Local infrastructure |
| Jupyter | Development and data exploration |

## Running the project

### Requirements

- Docker Desktop
- Python 3.x
- Jupyter Notebook or JupyterLab
- A Snowflake environment if the warehouse layer is required

### Start the local services

```bash
docker compose up -d
```

Then run the notebooks and streaming components according to the pipeline flow above.

The Docker configuration contains development credentials. They should be moved to environment variables or a secrets manager before using this setup outside local development. fileciteturn11file0

## What I learned

The main lesson from this project was understanding how the individual tools fit together.

Spark handles data processing, Snowflake provides the analytical storage, Kafka handles event ingestion, and Airflow coordinates the workflows. The project helped me see the complete pipeline rather than learning each technology in isolation.

## Author

**Fady Elhosary**  
Data Engineer

[LinkedIn](https://www.linkedin.com/in/fady-elhossary-68064a338/) · fadymohamed1@gmail.com
