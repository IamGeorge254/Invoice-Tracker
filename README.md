# Invoice Tracker – Airflow Workflow & Dashboard

## Objective
Build a workflow and dashboard to **automate invoice tracking**:
- Track invoices that are **overdue**  
- Highlight invoices that are **due within the next 7 days**  
- Provide a **dashboard** for visual insights into invoice status

## Project Overview
This project uses **Apache Airflow** to orchestrate workflows that:
1. **Extract invoice data** – client name, start date, end date, email, amount, and invoice ID  
2. **Load the data** into a **PostgreSQL database**  
3. **Visualize** invoices in a **dashboard** for easy monitoring  
