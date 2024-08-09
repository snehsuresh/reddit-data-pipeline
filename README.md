# Trending Artists Pipeline with Reddit, Airflow, Celery, Postgres, S3, AWS Glue, Athena, and Redshift

This project provides a comprehensive data pipeline for extracting, transforming, and loading (ETL) and then analyzing Music data from the subreddit r/music into an Amazon Redshift data warehouse. The pipeline utilizes a combination of tools and services, including Apache Airflow, Celery, PostgreSQL, Amazon S3, AWS Glue, AWS Lambda, Amazon Athena, Quicksight and Amazon Redshift.

## Overview

The pipeline is designed to:

 - Extract data from a subreddit using its API.
 - Store the raw data in an S3 bucket using Airflow.
 - Transform the data using AWS Glue or Amazon Athena.
 - Load the transformed data into Amazon Redshift for analytics and querying.
 - Modify the file names using AWS Lambda to make them readable by Amazon QuickSight.
 - Update the dataset in Amazon QuickSight on every bucket activity.

## Components

 - **Reddit API:** Source of the data.
 - **Apache Airflow & Celery:** Orchestrate the ETL process and manage task distribution.
 - **PostgreSQL:** Temporary storage and metadata management.
 - **Amazon S3:** Raw data storage.
 - **AWS Glue:** Data cataloging and ETL jobs.
 - **AWS Lambda:** Functions to rename files and update QuickSight data.
 - **Amazon Athena:** SQL-based data transformation.
 - **Amazon Redshift:** Data warehousing and analytics.
 - **Amazon QuickSight:** Dashboard for visualization.

 ## Prerequisites

 - AWS Account with appropriate permissions for S3, Glue, Athena, Lambda, QuickSight, and Redshift.
 - Reddit API credentials.
 - Docker Installation.
 - Python 3.9 or higher.

 ## System Setup

 1. Clone the repository:
    ```bash
    git clone https://github.com/snehsuresh/reddit-data-pipeline.git
    ```
 2. Create and activate a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
 3. Install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```
 4. Create a config file and set up your credentials:
   - Database Configuration: database_host, database_name, database_port, database_username, database_password
   - File Paths: input_path, output_path, artists_data_path
   - API Keys: reddit_secret_key, reddit_client_id
   - AWS Credentials: aws_access_key_id, aws_secret_access_key, aws_session_token, aws_region, aws_bucket_name
   - ETL Settings: batch_size, error_handling, log_level
 5. Start the containers:
    ```bash
    docker-compose up -d
    ```
 6. Launch the Airflow web UI:
    ```bash
    Open [http://localhost:8080](http://localhost:8080)
    ```

 ## Summary

 After each DAG run in airflow, with the proper AWS credentials, your S3 bucket will be updated with a CSV file. You can refer to the script in the `aws` directory named `glue.py` for the Glue job. The Lambda functions can be found in the `lambda` directory within the `glue` directory.

 You will need two S3 triggers for your functions.
