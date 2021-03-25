## Description
This is an Apache Airflow application which exeuctes a data pipeline to load data AWS Redshift cluster.
The app processs and stores data used to analyze data regarding songs and user activity collected from 
a music streaming app. The data is sourced from AWS S3 and loaded into fact and dimension tables


## Software required
* AWS Redshift
* Apache Airflow

## Files/descriptions
* README.md - contains project/app details
* airflow/create_tables.sql - sql to create tables in Redshift
* airflow/dags/udac_example_dag.py - dag to be loaded in Airflow to run pipeline
* airflow/plugins/helpers/sql_queries.py - sql queries to used to insert table to fact and dimension
* airflow/plugins/operators/data_quality.py - operator to preform data quality check 
* airflow/plugins/operators/load_dimension.py - operator to load dimension tables 
* airflow/plugins/operators/load_fact.py - operator to load fact table
* airflow/plugins/operators/stage_redshift.py - operator to load staging tables



## Setup/Execution Instructions
* create AWS Redshift cluster
* download airflow dir to ~/airflow
* launch Airflow 
* under Admin/Connections create 2 connections 
  - create connection called aws_credentials and store aws key/access info there 
  - create connection called redshift and store redshift cluster details there 
* click on DAGS menu option in AirFlow; dag named: udac_example_dag showed showed
* click "OFF" icon to turn DAG on; DAG should begin execution




## Output Details - tables populated on Redshift cluster

* Staging Tables
  - staging_events - staging table which hold event details
  - staging_songs - staging table which holds songs details
  
* Fact Table
  - songplays - records in log data associated with song plays, partioned by year, month 

* Dimension Tables
  - users - stores users in the app

  - songs - stores song details, partitioned by year, artist

  - artists - stores artists info

  - time - records time dimension details, partitioned by year, month

## Creator

* Ed Flemister
    - [https://github.com/eflemist/airflowdatapipelines](https://github.com/eflemist/airflowdatapipelines)
 
 
