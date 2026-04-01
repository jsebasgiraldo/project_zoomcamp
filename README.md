# Supplemental Poverty Measure (SPM) Data Pipeline with BRUIN

With the help of Bruin tool, this project implements an end-to-end batch data pipeline to ingest, transform, and visualize the **Supplemental Poverty Measure (SPM)** data provided by the **U.S. Census Bureau**. The goal is to analyze poverty trends across the United States, considering various socio-economic factors.

## 1. Project Objective
Build a robust, scalable pipeline that processes millions of records from the Census Bureau to identify poverty distributions by State, Education, Race, and Year. 

## 2. Dataset Description
The dataset is sourced from the [U.S. Census Bureau](https://www.census.gov/). It contains individual and household-level data regarding:
* **SPM Resources:** Total family resources used to calculate poverty status.
* **Demographics:** Age, Sex, Race, Hispanic origin, and Marital Status.
* **Economic Indicators:** Gross income (AGI), work expenses, and childcare expenses.
* **Geographic Data:** State-level identifiers.

The dataset inclues too much variables, but in this case, we are going to focus only in some variables of interest. 
---

## 3. Data Pipeline Architecture
The pipeline is orchestrated using **Bruin** and follows the **Medallion Architecture** (Bronze/Silver/Gold) within **Google BigQuery**. For this architecture, we are calling the Medallion architecture like Ingestion/Staging/Reports

### A. Ingestion Layer (Bronze)
* **Tool:** Python + Pandas + `dlt`.
* **Process:** * Dynamic download of `.dta` (Stata) files from Census FTP servers.
    * Handling of large-scale data (3M+ rows) using memory-efficient ingestion.
    * Loading a **State Dictionary** as a dimension table to map numeric codes to State names.
    * Data available from 2009 to 2023, except 2020 
* **Storage:** Raw tables in BigQuery.
* **States:** Creation of the states table with their ID's for later merge

**Clarification:** I had a problem with the pyspark timezone, so all the dates variables were uploaded like string 

### B. Staging Layer (Silver)
* **Tool:** Bruin SQL (BigQuery engine).
* **Process:** * **Data Typing:** Converting raw strings into proper formats (`DATE`, `INT64`, `FLOAT64`).
    * **Categorization:** Using `CASE WHEN` logic to transform numeric codes into human-readable categories (e.g., `0` -> `"In poverty"`).
    * **Joining:** Enriching the main dataset with State names through a `LEFT JOIN`.
    * **Optimization:** Implementing **Partitioning** by `FILEDATE` and **Clustering** by `State_name` and `poverty_status`.
    * **Filtering:** Taking only the data without error in the states

### C. Reporting Layer (Gold)
* **Tool:** Bruin SQL.
* **Process:** * Aggregating data at the State and Annual levels.
    * Calculating metrics such as `num_families` and `total_population`.
    * Extracting time-based dimensions (`reporting_year`) for trend analysis.
    * Extracting report of population characteristics and financial characteristics. 
---

## 4. Quality & Reliability
* **Schema Enforcement:** Strictly defined columns in Bruin assets.
* **Custom Checks:** Implementation of `custom_checks` in Bruin to ensure data integrity (e.g., `table_not_empty`).
* **Reproducibility:** The entire pipeline can be executed with a single command: `bruin run`.

## 5. Dashboard & Visualizations
The final analysis is presented in a **Looker Studio** dashboard, featuring:
1.  **Poverty Trends:** Line chart showing poverty rate by state.
2.  **Geographic Distribution:** A categorical analysis of poverty status across different U.S. States.
3.  **Demographic distribution:** Charts about the poverty distribution in kids, number of persons in the same family and average income

### 🔗 Visualization Link
> **https://lookerstudio.google.com/reporting/966eba1f-0578-4430-9fd4-e90a9ff6462c**

---

## 6. How to Run
1.  Ensure you have `bruin` installed.
2.  Configure your Google Cloud credentials in `.bruin.yml`.
3.  Run the full pipeline:
    ```bash
    bruin run --full-refresh
    ```
