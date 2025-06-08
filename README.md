# Inpost Senior Data Analyst Test Assignment - Ernest Nadosha

## How to Run

1. Press the green Code button -> codespaces -> inp -- this is the active codespace where the whole solution was developed
2. Run the ETL pipeline by executing `etl/main.py` 
3. Open and RunAll `/notebooks/main_report.ipynb` 

## Context

Hello and thank you for the chance to try this out. 

This repository contains the entire solution to calculating the metrics in the test assignment. 

Key requirements included:

* Scalability, handling large volumes of event and dimension data efficiently
* Calculating 4 metrics (avg delivery time, avg time in system, avg pickup time, total parcels)
* Using tools and formats appropriate for scalable, production-like data processing

To meet these requirements, the following technical choices were made:

* **PySpark** was chosen to emulate a Databricks environment and leverage distributed computing. This allows processing large datasets efficiently.
* **Parquet** file format is used for all final data tables, fact and dims. Parquet’s columnar storage leads to faster read/write speeds and efficient storage, which are critical for big data workloads.


## Repo Structure

* `/notebooks/`
  Contains the main Jupyter notebook `main_report.ipynb` which performs metric calculations and visualizes results.

* `/warehouse/`
  Stores all dimension and fact tables in Parquet format, ready for analysis.

* `/etl/`
  Includes the `main.py` script responsible for parsing raw source data, transforming it, and building the dimension and fact tables in the warehouse.



## Data Model

The data model closely follows the hierarchy and structure of the original JSON source files. It consists of one fact table and several dimension tables designed to enable efficient analysis of parcel shipping events.

![Inpost](https://github.com/user-attachments/assets/98960a65-0ecc-48c0-aebd-884309112feb)

### Key Tables:

* **FactShippingEvent:** The central fact table capturing individual shipping events with timestamps, event codes, and references to shipment IDs and dates.

* **DimShipping:** Contains shipment-level details, including client references, locations (collection and delivery), parcel metadata, and flags like replacement status.

* **DimClient:** Holds client-level information, such as brand codes and signing codes.

* **DimLocation:** Stores location details, distinguishing between collection and delivery points, along with country and agency codes.

* **DimState:** Represents various states and statuses a shipment can have, with codes and descriptive attributes.

* **DimDate:** Standard date dimension to support time-based analysis, with year, month, day, quarter, and weekday attributes.

Note: You will not see the actual parquet files with tables in the repo, only in the codespace
IMPORTANT: primary keys were generated with UUID


## Data Quality

The `main.py` script includes simple data quality (DQ) checks designed to ensure the basic validity of the data at key processing stages.

* They verify that each DataFrame is **not empty**.
* They check for **null values** in the first three columns of each DataFrame.
* Each check returns a pass/fail status along with details such as row counts or null counts.
* The results of these checks are collected and saved into the `metadata` folder every time the ETL script runs.
