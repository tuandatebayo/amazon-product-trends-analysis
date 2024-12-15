# Big Data Project | Potential products: Ecomerce Product

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)
- [Getting Started](#getting-started)

## Introduction

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline. It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease of deployment and scalability.

## System Architecture

![System Architecture](https://github.com/tuandatebayo/bigdata20241-14/blob/main/Architecture.png)

The project is designed with the following components:

- **Data Source**: We use Amazon sales dataset from Kaggle that has been upload to HuggingFace for API to generate random data for our pipeline.
Kaggle: https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.
- **Superset**: Visualize for serving layer.

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Trino
- Superset
- Docker

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/tuandatebayo/bigdata20241-14
    ```

2. Navigate to the project directory:
    ```bash
    cd bigdata20241-14
    ```
3. Run Docker Compose to spin up the services:
    ```bash
    make start-docker 
    ```
4. Run speed layer and batch layer:
    ```bash
    make start-all
    ```
5. Remove container:
    ```bash
    make shutdown-docker
    ```
