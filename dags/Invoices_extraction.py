from airflow import DAG         # Defines airflow worflow
from airflow.operators.python import PythonOperator     # Lets you run python functions as tasks
from datetime import datetime, timedelta        # For Scheduling
import pandas as pd
import os
import pdfplumber # Assist in reading pdf files
import re
from sqlalchemy import create_engine

# --------------------
# CONFIG
# --------------------

# Local invoices folder
LOCAL_FOLDER = "/opt/airflow/invoices"

# Postgres connection
PG_USER = "airflow"
PG_PASSWORD = "airflow"
PG_HOST = "postgres"
PG_PORT = "5432"
PG_DATABASE = "airflow"
TABLE_NAME = "invoices_data"

# --------------------
# ETL Functions
# --------------------
def extract_invoice_data(**context):
    """
    Extract invoice data from downloaded PDFs
    """
    records = []

    # Check if folder has any files
    if not os.listdir(LOCAL_FOLDER):
        print("No PDF files found in folder.")
        return "No data extracted"

    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name.lower().endswith(".pdf"):
            pdf_path = os.path.join(LOCAL_FOLDER, file_name)
            print(f"Processing {pdf_path}")

            data = {
                'Invoice ID': None,
                'Client': None,
                'Email': None,
                'Start Date': None,
                'End Date': None,
                'Amount': None
            }

            try:
                with pdfplumber.open(pdf_path) as pdf:
                    text = ""
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n"

                # Extract Invoice ID
                match = re.search(r"Invoice Number:\s*(\S+)", text)
                if match:
                    data['Invoice ID'] = match.group(1)

                # Extract Dates
                issue = re.search(r"Date of Issue:\s*([\d]{1,2}\s\w+\s\d{4})", text)
                due = re.search(r"Due Date:\s*([\d]{1,2}\s\w+\s\d{4})", text)
                if issue:
                    data['Start Date'] = issue.group(1)
                if due:
                    data['End Date'] = due.group(1)

                # Extract Client
                client = re.search(r"Bill To:\s*([^\n]+)", text)
                if client:
                    data['Client'] = client.group(1).strip()

                # Extract Email (prefer last email → usually client’s)
                emails = re.findall(r"[\w\.-]+@[\w\.-]+", text)
                if emails:
                    data['Email'] = emails[-1]

                # Extract Total Amount
                amount = re.search(r"TOTAL DUE:\s*NGN\s*([\d,]+\.\d{2})", text)
                if amount:
                    data['Amount'] = amount.group(1)

                # Add to results
                records.append(data)

            except Exception as e:
                print(f"Failed to process {pdf_path}: {e}")

    # Push to XCom
    context['ti'].xcom_push(key='extracted_data', value=records)
    print(f"Extracted {len(records)} invoices")
    return "Extract complete"

def transform_to_dataframe(**context):
    """
    Transform extracted invoice records into a Pandas DataFrame.
    """
    records = context['ti'].xcom_pull(key='extracted_data', task_ids='extract_task')
    if not records:
        print("No records extracted. Exiting transform task.")
        return "No data to transform"

    df = pd.DataFrame(records)

    context['ti'].xcom_push(key='transformed_data', value=df.to_json(orient="records"))
    print(f"Transformed {len(df)} records")
    return "Transform complete"

def load_data(**context):
    """Load transformed data into PostgreSQL"""
    # Pull transformed JSON from XCom
    transformed_json = context['ti'].xcom_pull(
        key='transformed_data', 
        task_ids='transform_task')
    if not transformed_json:
        print("No data to load.")
        return "No data loaded"
    
    # Convert back to DataFrame
    df = pd.read_json(transformed_json)

    # Create Postgres engine
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
        )
    
    # Load DataFrame into Postgres
    df.to_sql(TABLE_NAME, con=engine, if_exists="replace", index=False)

    print(f"Loaded {len(df)} rows into PostgreSQL table '{TABLE_NAME}'")
    return "Load complete"

# --------------------
# DAG Definition
# --------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Invoices_extraction",
    default_args=default_args,
    description="ETL pipeline: Google Drive → Transform → PostgreSQL",
    schedule="@daily",   # runs daily
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["etl", "postgres", "pdf"],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_invoice_data
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_to_dataframe
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task