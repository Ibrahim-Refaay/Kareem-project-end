# -*- coding: utf-8 -*-
"""
Inventory Comparison ETL - Odoo vs Store
Fetches data from Odoo and Store API, compares inventory, and uploads to BigQuery.

Usage:
    cd /root/script
    . .venv/bin/activate
    python3 data.py
"""

import logging
import pandas as pd
from datetime import datetime, timezone
import sys
import os
from google.cloud import bigquery

# Import fetch functions from app
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from app import (
    CONFIG, 
    fetch_odoo_data, 
    fetch_store_data, 
    compare_inventories,
)

# ==============================================================================
# Configuration
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = CONFIG["BIGQUERY_PROJECT"]
DATASET_ID = CONFIG["BIGQUERY_DATASET"]
TABLE_ID = CONFIG["BIGQUERY_TABLE"]
STAGING_TABLE_ID = f"{TABLE_ID}_staging"
DESTINATION_TABLE = f"{DATASET_ID}.{TABLE_ID}"
STAGING_DESTINATION_TABLE = f"{DATASET_ID}.{STAGING_TABLE_ID}"

# ==============================================================================
# Main ETL Functions
# ==============================================================================

def get_comparison_dataframe():
    """Fetch data from Odoo and Store, compare, and return DataFrame."""
    import time
    start_time = time.time()
    
    logging.info("=== Starting Inventory Comparison ETL ===")
    
    # Step 1: Fetch Odoo data
    logging.info("Step 1: Fetching data from Odoo...")
    step_start = time.time()
    odoo_data = fetch_odoo_data()
    odoo_time = time.time() - step_start
    logging.info(f"✅ Fetched {len(odoo_data)} products from Odoo in {odoo_time:.2f}s")
    
    # Step 2: Fetch Store data
    logging.info("Step 2: Fetching data from Store API...")
    step_start = time.time()
    store_data = fetch_store_data()
    store_time = time.time() - step_start
    logging.info(f"✅ Fetched {len(store_data)} products from Store in {store_time:.2f}s")
    
    # Step 3: Compare inventories
    logging.info("Step 3: Comparing inventories...")
    step_start = time.time()
    comparison_results = compare_inventories(odoo_data, store_data)
    compare_time = time.time() - step_start
    logging.info(f"✅ Comparison complete: {len(comparison_results)} total records in {compare_time:.2f}s")
    
    # Step 4: Convert to DataFrame
    if not comparison_results:
        logging.warning("No comparison results to upload.")
        return pd.DataFrame()
    
    # Add timestamp
    timestamp = datetime.now(timezone.utc)
    
    df_data = []
    for row in comparison_results:
        df_data.append({
            "timestamp": timestamp,
            "barcode": row["barcode"],
            "odoo_name": row["odoo_name"],
            "odoo_qty": int(row["odoo_qty"]) if row["odoo_qty"] is not None else None,
            "store_name": row["store_name"] if row["store_name"] != "N/A" else None,
            "store_qty": int(row["store_qty"]) if isinstance(row["store_qty"], (int, float)) else None,
            "store_id": row["store_id"] if row["store_id"] != "N/A" else None,
            "status": row["status"],
            "difference": str(row["difference"]),
        })
    
    df = pd.DataFrame(df_data)
    
    # Ensure correct dtypes
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['odoo_qty'] = df['odoo_qty'].astype('Int64')  # Nullable integer
    df['store_qty'] = df['store_qty'].astype('Int64')
    
    total_time = time.time() - start_time
    logging.info(f"✅ DataFrame created with {len(df)} rows in {total_time:.2f}s total")
    return df

def load_dataframe(df, project_id, dataset_id, table_id, write_disposition):
    """Load a DataFrame into BigQuery with the provided write disposition."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

    logging.info(
        f"Loading {len(df)} rows into {table_ref} with disposition {write_disposition}..."
    )
    try:
        load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()  # Wait for job completion
        logging.info(f"✅ Successfully wrote {len(df)} rows to {table_ref}")
    except Exception as exc:
        logging.error(f"❌ Error loading data into {table_ref}: {exc}")
        raise


def upload_to_main_table(df, project_id):
    """Replace main BigQuery table contents with the provided DataFrame."""
    if df.empty:
        logging.warning("DataFrame is empty. Skipping upload to main table.")
        return

    load_dataframe(
        df=df,
        project_id=project_id,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

def create_staging_snapshot(df, project_id):
    """Create a snapshot in staging table with run_date."""
    if df.empty:
        logging.warning("DataFrame is empty. Skipping staging snapshot.")
        return
    
    # Add run_date column (today's date in UTC)
    run_date = datetime.now(timezone.utc).date()
    df_staging = df.copy()
    df_staging['run_date'] = run_date
    
    # Reorder columns to match staging schema
    columns_order = [
        'run_date', 'timestamp', 'barcode', 'odoo_name', 'odoo_qty',
        'store_name', 'store_qty', 'store_id', 'status', 'difference'
    ]
    df_staging = df_staging[columns_order]
    
    import time
    logging.info(f"Creating staging snapshot for {run_date} with {len(df_staging)} rows...")
    staging_start = time.time()
    try:
        load_dataframe(
            df=df_staging,
            project_id=project_id,
            dataset_id=DATASET_ID,
            table_id=STAGING_TABLE_ID,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        staging_time = time.time() - staging_start
        logging.info(f"✅ Staging snapshot created for {run_date} in {staging_time:.2f}s")
    except Exception as e:
        logging.error(f"❌ Error creating staging snapshot: {e}")
        raise

# ==============================================================================
# Main Execution
# ==============================================================================

if __name__ == "__main__":
    if not PROJECT_ID:
        logging.error("BIGQUERY_PROJECT is not configured. Please check CONFIG in app.py")
        sys.exit(1)
    
    try:
        # Step 1: Get comparison data as DataFrame
        comparison_df = get_comparison_dataframe()
        
        if comparison_df.empty:
            logging.warning("No data to upload. Exiting.")
            sys.exit(0)
        
        # Step 2: Upload to main table
        upload_to_main_table(comparison_df, PROJECT_ID)
        
        # Step 3: Create staging snapshot
        create_staging_snapshot(comparison_df, PROJECT_ID)
        
        logging.info("=== ✅ ETL Process Completed Successfully ===")
        
        # Print summary statistics
        matches = len(comparison_df[comparison_df['status'].str.contains('MATCH', na=False)])
        mismatches = len(comparison_df[comparison_df['status'].str.contains('MISMATCH', na=False)])
        not_found = len(comparison_df[comparison_df['status'].str.contains('NOT FOUND', na=False)])
        
        logging.info(f"Summary:")
        logging.info(f"  - Total records: {len(comparison_df)}")
        logging.info(f"  - Matches: {matches}")
        logging.info(f"  - Mismatches: {mismatches}")
        logging.info(f"  - Not Found: {not_found}")
        
    except Exception as e:
        logging.critical("=== ❌ ETL Process Failed ===", exc_info=True)
        sys.exit(1)
