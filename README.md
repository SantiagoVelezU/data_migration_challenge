# data_migration_challenge
Provided above is the solution to Globant's data engineering technical challenge.

# Overview
This project aims to migrate data from three CSV files into a PostgreSQL database, providing a REST API for data ingestion and implementing backup and recovery functionalities using Avro format. The entire solution is containerized using Docker for easy deployment.

# Features
1) Load data from CSV files to PostgreSQL database
2) Provide a REST Api for inserting and retrieving data back. Also you can find endpoints to backup and recovery data using Avro.
3) Easily deployable with Docker.

## Installation
# Prerequisites
Docker
Docker compose
PostgreSQL

## Setup Instructions
1) Clone the repository: 
2) Execute command: docker-compose up --build 