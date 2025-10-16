# -*- coding: utf-8 -*-
"""
Inventory Comparison ETL - Odoo vs Store
Fetches data from Odoo and Store API, compares inventory, and uploads to BigQuery.

Standalone version - No app.py needed
"""

import logging
import pandas as pd
from datetime import datetime, timezone
import sys
import os
from google.cloud import bigquery
import xmlrpc.client
import requests

# ==============================================================================
# Configuration - Read from Environment Variables
# ==============================================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Configuration from environment variables
CONFIG = {
    "ODOO_URL": os.getenv("ODOO_URL", ""),
    "ODOO_DB": os.getenv("ODOO_DB", ""),
    "ODOO_USERNAME": os.getenv("ODOO_USERNAME", ""),
    "ODOO_PASSWORD": os.getenv("ODOO_PASSWORD", ""),
    "STORE_API_URL": os.getenv("STORE_API_URL", ""),
    "STORE_API_KEY": os.getenv("STORE_API_KEY", ""),
    "BIGQUERY_PROJECT": os.getenv("BIGQUERY_PROJECT", ""),
    "BIGQUERY_DATASET": os.getenv("BIGQUERY_DATASET", ""),
    "BIGQUERY_TABLE": os.getenv("BIGQUERY_TABLE", ""),
}

PROJECT_ID = CONFIG["BIGQUERY_PROJECT"]
DATASET_ID = CONFIG["BIGQUERY_DATASET"]
TABLE_ID = CONFIG["BIGQUERY_TABLE"]
STAGING_TABLE_ID = f"{TABLE_ID}_staging"

# ==============================================================================
# Data Fetching Functions
# ==============================================================================

def fetch_odoo_data():
    """
    Fetch product inventory data from Odoo.
    Returns: List of dictionaries with product data
    """
    logging.info(f"Connecting to Odoo: {CONFIG['ODOO_URL']}")
    logging.info(f"Database: {CONFIG['ODOO_DB']}")
    logging.info(f"Username: {CONFIG['ODOO_USERNAME']}")
    
    try:
        # Odoo XML-RPC setup
        common_url = f"{CONFIG['ODOO_URL']}/xmlrpc/2/common"
        logging.info(f"Common URL: {common_url}")
        common = xmlrpc.client.ServerProxy(common_url)
        
        # Try to get version first (to test connection)
        try:
            version_info = common.version()
            logging.info(f"Odoo version info: {version_info}")
        except Exception as ve:
            logging.error(f"Failed to connect to Odoo: {ve}")
            raise
        
        logging.info("Attempting authentication...")
        uid = common.authenticate(
            CONFIG['ODOO_DB'], 
            CONFIG['ODOO_USERNAME'], 
            CONFIG['ODOO_PASSWORD'], 
            {}
        )
        
        logging.info(f"Authentication result - UID: {uid}")
        
        if not uid:
            raise Exception(
                f"Authentication failed!\n"
                f"Database: {CONFIG['ODOO_DB']}\n"
                f"Username: {CONFIG['ODOO_USERNAME']}\n"
                f"Please check:\n"
                f"1. Database name is correct\n"
                f"2. Username and password are correct\n"
                f"3. User has API access enabled\n"
                f"4. Odoo instance allows external API access"
            )
        
        models = xmlrpc.client.ServerProxy(f"{CONFIG['ODOO_URL']}/xmlrpc/2/object")
        
        # Fetch products with barcodes
        products = models.execute_kw(
            CONFIG['ODOO_DB'], 
            uid, 
            CONFIG['ODOO_PASSWORD'],
            'product.product', 
            'search_read',
            [[['barcode', '!=', False]]],
            {
                'fields': ['barcode', 'name', 'qty_available']
                # Removed 'limit': None - XML-RPC doesn't accept None values
            }
        )
        
        # Transform to standard format
        odoo_data = []
        for product in products:
            odoo_data.append({
                'barcode': product.get('barcode', ''),
                'name': product.get('name', ''),
                'qty': product.get('qty_available', 0)
            })
        
        logging.info(f"Fetched {len(odoo_data)} products from Odoo")
        return odoo_data
        
    except Exception as e:
        logging.error(f"Error fetching Odoo data: {e}")
        raise


def fetch_store_data():
    """
    Fetch product inventory data from Store API.
    Returns: List of dictionaries with product data
    """
    logging.info(f"Connecting to Store API: {CONFIG['STORE_API_URL']}")
    
    try:
        headers = {
            "Authorization": f"Bearer {CONFIG['STORE_API_KEY']}",
            "Content-Type": "application/json"
        }
        
        # Adjust endpoint based on your API
        response = requests.get(
            f"{CONFIG['STORE_API_URL']}/products",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Transform to standard format
        store_data = []
        products = data.get('products', data)
        
        for product in products:
            store_data.append({
                'barcode': product.get('barcode', ''),
                'name': product.get('name', ''),
                'qty': product.get('quantity', 0),
                'id': product.get('id', '')
            })
        
        logging.info(f"Fetched {len(store_data)} products from Store API")
        return store_data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Store data: {e}")
        raise


def compare_inventories(odoo_data, store_data):
    """
    Compare inventory data from Odoo and Store.
    Returns: List of comparison results
    """
    # Create lookup dictionary for store data
    store_dict = {item['barcode']: item for item in store_data if item.get('barcode')}
    
    comparison_results = []
    
    for odoo_item in odoo_data:
        barcode = odoo_item['barcode']
        odoo_qty = odoo_item['qty']
        odoo_name = odoo_item['name']
        
        # Check if product exists in store
        if barcode in store_dict:
            store_item = store_dict[barcode]
            store_qty = store_item['qty']
            store_name = store_item['name']
            store_id = store_item['id']
            
            # Compare quantities
            if odoo_qty == store_qty:
                status = "MATCH"
                difference = "0"
            else:
                status = "MISMATCH"
                difference = str(odoo_qty - store_qty)
            
            comparison_results.append({
                "barcode": barcode,
                "odoo_name": odoo_name,
                "odoo_qty": odoo_qty,
                "store_name": store_name,
                "store_qty": store_qty,
                "store_id": store_id,
                "status": status,
                "difference": difference
            })
        else:
            # Product not found in store
            comparison_results.append({
                "barcode": barcode,
                "odoo_name": odoo_name,
                "odoo_qty": odoo_qty,
                "store_name": "N/A",
                "store_qty": "N/A",
                "store_id": "N/A",
                "status": "NOT FOUND IN STORE",
                "difference": "N/A"
            })
    
    return comparison_results

# ==============================================================================
# ETL Functions
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
    df['odoo_qty'] = df['odoo_qty'].astype('Int64')
    df['store_qty'] = df['store_qty'].astype('Int64')
    
    total_time = time.time() - start_time
    logging.info(f"✅ DataFrame created with {len(df)} rows in {total_time:.2f}s total")
    return df


def load_dataframe(df, project_id, dataset_id, table_id, write_disposition):
    """Load a DataFrame into BigQuery with the provided write disposition."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

    logging.info(f"Loading {len(df)} rows into {table_ref} with disposition {write_disposition}...")
    try:
        load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()
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
    
    # Add run_date column
    run_date = datetime.now(timezone.utc).date()
    df_staging = df.copy()
    df_staging['run_date'] = run_date
    
    # Reorder columns
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
        logging.error("BIGQUERY_PROJECT is not configured. Please check environment variables")
        sys.exit(1)
    
    try:
        # Get comparison data as DataFrame
        comparison_df = get_comparison_dataframe()
        
        if comparison_df.empty:
            logging.warning("No data to upload. Exiting.")
            sys.exit(0)
        
        # Upload to main table
        upload_to_main_table(comparison_df, PROJECT_ID)
        
        # Create staging snapshot
        create_staging_snapshot(comparison_df, PROJECT_ID)
        
        logging.info("=== ✅ ETL Process Completed Successfully ===")
        
        # Print summary
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
