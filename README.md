# Airflow Assignment 3 – Web Log Processing DAG

This repository contains the solution for **Assignment 3** of the _Management of Data Science_ course.  
The goal is to implement an Apache Airflow DAG that processes web server logs and sends a completion notification to a Discord channel.

## Project Overview

The main DAG is called `process_web_log`.  
It performs a small ETL pipeline on a web server log file:

1. `scan_for_log`: scan a folder for the latest log file.
2. `extract_data`: extract all IP addresses from the log.
3. `transform_data`: filter the log for a single target IP address.
4. `load_data`: archive the filtered result into an output file.
5. `notify_discord`: send a notification to a Discord channel once the DAG has completed successfully.

The notification task uses a **Discord incoming webhook** and the `requests` library.

## Requirements

- Python 3.12 (or compatible 3.10+)
- [Apache Airflow 2.x](https://airflow.apache.org/)
- `requests` (for the Discord webhook)

Packages can typically be installed with:

```bash
pip install "apache-airflow==2.*" requests
```

## Repository Structure

```bash
Assignement3/
├── dags/
│   └── process_web_log.py      # Main DAG definition
├── data/                       # Input and output log files (created by the DAG)
├── .venv/                      # Python virtual environment (not committed)
└── README.md
```

## Setup

Setup
```bash
python -m venv .venv
source .venv/bin/activate

export AIRFLOW_HOME="$(pwd)"
export AIRFLOW__CORE__XCOM_BACKEND="airflow.models.xcom.BaseXCom"
export AIRFLOW__CORE__AUTH_MANAGER_CLASS="airflow.www.auth.managers.fab.FabAuthManager"

# Initialize Airflow metadata database
airflow db init

# Create an admin user for the web UI (run once)
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```
Place the DAG file process_web_log.py inside the dags/ directory.

## Running Airflow

In two separate terminals (both with the virtual environment and environment variables activated):

### Terminal 1 – Scheduler

```bash
cd /path/to/Assignement3
source .venv/bin/activate

export AIRFLOW_HOME="$(pwd)"
export AIRFLOW__CORE__XCOM_BACKEND="airflow.models.xcom.BaseXCom"
export AIRFLOW__CORE__AUTH_MANAGER_CLASS="airflow.www.auth.managers.fab.FabAuthManager"

airflow scheduler
```

### Terminal 2 – Web Server

```bash
cd /path/to/Assignement3
source .venv/bin/activate

export AIRFLOW_HOME="$(pwd)"
export AIRFLOW__CORE__XCOM_BACKEND="airflow.models.xcom.BaseXCom"
export AIRFLOW__CORE__AUTH_MANAGER_CLASS="airflow.www.auth.managers.fab.FabAuthManager"

airflow webserver --port 8080
```

Then open the Airflow UI in a browser:
http://localhost:8080

Enable the process_web_log DAG using the toggle button.