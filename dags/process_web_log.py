from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import tarfile
import requests

# =========================
#  Global Variables
# =========================

LOG_FOLDER = "dags/the_logs"
LOG_FILE = "log.txt"
LOG_PATH = os.path.join(LOG_FOLDER, LOG_FILE)

EXTRACTED_FILE = os.path.join(LOG_FOLDER, "extracted_data.txt")
TRANSFORMED_FILE = os.path.join(LOG_FOLDER, "transformed_data.txt")
ARCHIVE_FILE = os.path.join(LOG_FOLDER, "weblog.tar")

BLOCKED_IP = "198.46.149.143"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1439610141912862841/oUohD11xeY7gnySx_FP3uOi4bAEo2wUqlvUwLqVHFwb2Qx29xrEUsIoRXJTnyAxhYSfh" 

# =========================
#  Task Functions
# =========================

def scan_for_log():
    """
    Checks if log.txt exists in the log folder.
    Raises an error if the file is missing.
    """
    print("Scanning for log file...")
    if os.path.exists(LOG_PATH):
        print(f"Log file found at: {LOG_PATH}")
    else:
        raise FileNotFoundError(f"Log file not found in folder: {LOG_FOLDER}")


def extract_data():
    """
    Extracts the IP address from each log entry and writes them
    into extracted_data.txt.
    """
    print("Extracting IP addresses...")

    os.makedirs(LOG_FOLDER, exist_ok=True)

    with open(LOG_PATH, "r") as log, open(EXTRACTED_FILE, "w") as output:
        for line in log:
            ip_part = line.split(" - - ")[0].strip()
            output.write(f"{ip_part}\n")

    print(f"IP addresses written to: {EXTRACTED_FILE}")


def transform_data():
    """
    Removes all occurrences of BLOCKED_IP from extracted_data.txt
    and writes the filtered result to transformed_data.txt.
    """
    print("Transforming data...")

    with open(EXTRACTED_FILE, "r") as extracted, open(TRANSFORMED_FILE, "w") as transformed:
        for line in extracted:
            ip = line.strip()
            if ip != BLOCKED_IP:
                transformed.write(f"{ip}\n")

    print(f"Filtered data written to: {TRANSFORMED_FILE}")


def load_data():
    """
    Archives the transformed data file into weblog.tar.
    """
    print("Creating tar archive...")

    os.makedirs(LOG_FOLDER, exist_ok=True)

    with tarfile.open(ARCHIVE_FILE, "w") as tar:
        tar.add(TRANSFORMED_FILE, arcname=os.path.basename(TRANSFORMED_FILE))

    print(f"Archive created at: {ARCHIVE_FILE}")

def notify_discord():
    """
    Sends a notification message to a Discord channel via webhook
    to indicate that the DAG has completed successfully.
    """
    from datetime import datetime

    print("Sending notification to Discord...")

    message = {
        "content": (
            f"✅ Airflow DAG 'process_web_log' completed successfully "
            f"for run at {datetime.utcnow().isoformat()} (UTC)."
        )
    }

    response = requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=10)
    # Raise an error if the request failed
    response.raise_for_status()

    print("Discord notification sent (status code:", response.status_code, ")")


# =========================
#  DAG Definition
# =========================

default_args = {
    "start_date": datetime(2025, 11, 10),
}

with DAG(
    dag_id="process_web_log",
    description="Daily processing of web server logs: scan, extract IPs, filter one IP, archive the result.",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["data_science"],
) as dag:

    scan_for_log_task = PythonOperator(
        task_id="scan_for_log",
        python_callable=scan_for_log,
    )

    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )
    notify_discord_task = PythonOperator(
        task_id="notify_discord",
        python_callable=notify_discord,
    )

    # Task sequence
    scan_for_log_task >> extract_data_task >> transform_data_task >> load_data_task >> notify_discord_task


