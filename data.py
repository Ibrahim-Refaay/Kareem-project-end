"""Parallelized ETL script with location filtering for Odoo inventory.

Reads location IDs from an Excel file and fetches inventory only for those locations.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def _load_env_int(name: str, default: int) -> int:
    """Read an integer from environment variables with fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logging.warning("Invalid integer for %s=%s, using default %s", name, raw, default)
        return default


def _require_env(name: str) -> str:
    """Return a mandatory environment variable or exit with clear message."""
    value = os.getenv(name)
    if not value:
        logging.critical("Missing required environment variable: %s", name)
        sys.exit(1)
    return value


def load_location_ids(filepath: str) -> Set[int]:
    """Load location IDs from Excel file."""
    if not Path(filepath).exists():
        logging.error("Location file not found: %s", filepath)
        return set()
    
    try:
        df = pd.read_excel(filepath)
        if 'id' not in df.columns:
            logging.error("Column 'id' not found in %s", filepath)
            return set()
        
        location_ids = set(df['id'].dropna().astype(int).tolist())
        logging.info("Loaded %s location IDs from %s", len(location_ids), filepath)
        return location_ids
    except Exception as e:
        logging.error("Failed to load location IDs: %s", e)
        return set()


class InventoryETL:
    """Encapsulates inventory comparison logic with location filtering."""

    def __init__(self, location_ids: Optional[Set[int]] = None) -> None:
        self.odoo_url = _require_env("ODOO_URL").rstrip("/")
        self.odoo_db = _require_env("ODOO_DB")
        self.odoo_username = _require_env("ODOO_USERNAME")
        self.odoo_password = _require_env("ODOO_PASSWORD")
        self.store_api_url = _require_env("STORE_API_URL").rstrip("/")
        self.store_api_token = _require_env("STORE_API_KEY")
        self.bigquery_project = _require_env("BIGQUERY_PROJECT")
        self.dataset_id = _require_env("BIGQUERY_DATASET")
        self.table_id = _require_env("BIGQUERY_TABLE")
        self.staging_table_id = f"{self.table_id}_staging"
        self.odoo_batch = _load_env_int("ODOO_BATCH", 200)
        self.store_batch = _load_env_int("STORE_BATCH", 200)
        self.max_workers = _load_env_int("ETL_MAX_WORKERS", 5)
        self.location_ids = location_ids or set()
        
    # ------------------------------------------------------------------ Odoo --
    def _get_odoo_session(self) -> Dict:
        auth_url = f"{self.odoo_url}/web/session/authenticate"
        payload = {
            "jsonrpc": "2.0",
            "params": {
                "db": self.odoo_db,
                "login": self.odoo_username,
                "password": self.odoo_password,
            },
        }
        response = requests.post(auth_url, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()
        result = data.get("result", {})
        if not result.get("uid"):
            raise RuntimeError("Odoo authentication failed; no UID returned.")
        return response.cookies.get_dict()

    def _fetch_odoo_batch(self, session: Dict, offset: int) -> List[Dict]:
        """Fetch a single batch of Odoo products."""
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "product.product",
                "method": "search_read",
                "args": [[
                    ["type", "=", "product"],
                    ["barcode", "!=", False],
                    ["barcode", "!=", ""],
                ]],
                "kwargs": {
                    "fields": ["id", "display_name", "barcode"],
                    "limit": self.odoo_batch,
                    "offset": offset,
                },
            },
        }
        rpc_url = f"{self.odoo_url}/web/dataset/call_kw"
        response = requests.post(rpc_url, json=payload, cookies=session, timeout=60)
        response.raise_for_status()
        batch = response.json().get("result", [])
        logging.info("Fetched Odoo batch at offset %s: %s products", offset, len(batch))
        return batch

    def fetch_odoo_data(self) -> List[Dict]:
        """Fetch Odoo data with parallel batch requests and location filtering."""
        session = self._get_odoo_session()
        
        # First, get total count to determine number of batches
        initial_batch = self._fetch_odoo_batch(session, 0)
        all_products = initial_batch.copy()
        
        if len(initial_batch) == self.odoo_batch:
            # More batches needed - fetch remaining in parallel
            offsets = list(range(self.odoo_batch, self.odoo_batch * 100, self.odoo_batch))
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._fetch_odoo_batch, session, offset): offset 
                    for offset in offsets
                }
                
                for future in as_completed(futures):
                    try:
                        batch = future.result()
                        if batch:
                            all_products.extend(batch)
                        else:
                            break
                    except Exception as e:
                        offset = futures[future]
                        logging.error("Failed to fetch Odoo batch at offset %s: %s", offset, e)

        # Fetch stock quantities with location filtering
        stock_domain = [["location_id.usage", "=", "internal"]]
        
        if self.location_ids:
            # Filter by specific location IDs
            stock_domain.append(["location_id", "in", list(self.location_ids)])
            logging.info("Filtering stock by %s location IDs", len(self.location_ids))
        
        stock_payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "stock.quant",
                "method": "search_read",
                "args": [stock_domain],
                "kwargs": {
                    "fields": ["product_id", "quantity", "reserved_quantity", "location_id"],
                    "limit": 0,
                },
            },
        }
        rpc_url = f"{self.odoo_url}/web/dataset/call_kw"
        response = requests.post(rpc_url, json=stock_payload, cookies=session, timeout=120)
        response.raise_for_status()
        stock_quants = response.json().get("result", [])
        
        logging.info("Fetched %s stock quants from filtered locations", len(stock_quants))

        stock_map: Dict[int, Dict[str, float]] = {}
        for quant in stock_quants:
            product = quant.get("product_id")
            if isinstance(product, list):
                product_id = product[0]
            else:
                product_id = product
            
            # Verify location is in filter (if filter is active)
            if self.location_ids:
                location = quant.get("location_id")
                location_id = location[0] if isinstance(location, list) else location
                if location_id not in self.location_ids:
                    continue
            
            entry = stock_map.setdefault(product_id, {"onHand": 0.0, "reserved": 0.0})
            entry["onHand"] += quant.get("quantity", 0.0)
            entry["reserved"] += quant.get("reserved_quantity", 0.0)

        results: List[Dict] = []
        for product in all_products:
            product_id = product.get("id")
            stock_info = stock_map.get(product_id, {"onHand": 0.0, "reserved": 0.0})
            
            # Only include products with stock in filtered locations
            if self.location_ids and stock_info["onHand"] == 0.0:
                continue
                
            available = max(stock_info["onHand"] - stock_info["reserved"], 0.0)
            results.append({
                "barcode": product.get("barcode", ""),
                "name": product.get("display_name", ""),
                "quantity": int(available),
            })

        logging.info("Fetched %s products from Odoo (filtered by locations)", len(results))
        return results

    # ---------------------------------------------------------------- Store --
    def _create_store_session(self) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[502, 503, 504, 520],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def _fetch_store_page(self, page: int, session: Optional[requests.Session] = None) -> List[Dict]:
        """Fetch a single page from Store API."""
        if session is None:
            session = self._create_store_session()
            
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.store_api_token}",
        }
        
        url = f"{self.store_api_url}/products/limit/{self.store_batch}/page/{page}"
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                response = session.get(url, headers=headers, timeout=90)
                response.raise_for_status()
                payload = response.json()
                
                if payload.get("success") != 1:
                    return []
                    
                data = payload.get("data") or []
                logging.info("Fetched Store page %s: %s products", page, len(data))
                return data
                
            except requests.exceptions.ChunkedEncodingError:
                logging.warning("⚠️ Store API chunked error on page %s, attempt %s/%s", 
                              page, attempt + 1, max_retries)
                time.sleep(2 ** attempt)
            except requests.exceptions.RequestException as e:
                logging.error("❌ Store API request failed on page %s: %s", page, e)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return []
        
        return []

    def fetch_store_data(self) -> List[Dict]:
        """Fetch Store data with parallel page requests."""
        all_products: List[Dict] = []
        
        # Fetch first page to determine if more pages exist
        first_page = self._fetch_store_page(1)
        if not first_page:
            logging.warning("No products found in Store API")
            return []
        
        all_products.extend(first_page)
        
        # If first page is full, fetch more pages in parallel
        if len(first_page) == self.store_batch:
            page = 2
            empty_pages = 0
            max_empty = 3  # Stop after 3 consecutive empty pages
            
            while empty_pages < max_empty and page < 1000:  # Safety limit
                # Fetch next batch of pages in parallel
                pages_to_fetch = list(range(page, page + self.max_workers))
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {
                        executor.submit(self._fetch_store_page, p): p 
                        for p in pages_to_fetch
                    }
                    
                    for future in as_completed(futures):
                        page_num = futures[future]
                        try:
                            data = future.result()
                            if data:
                                all_products.extend(data)
                                empty_pages = 0
                            else:
                                empty_pages += 1
                        except Exception as e:
                            logging.error("Failed to fetch Store page %s: %s", page_num, e)
                            empty_pages += 1
                
                page += self.max_workers
                time.sleep(0.5)  # Small delay between batches

        logging.info("✅ Total fetched from Store API: %s products", len(all_products))

        normalized: List[Dict] = []
        for product in all_products:
            barcode = product.get("upc")
            description = product.get("product_description")
            name = description[0].get("name") if isinstance(description, list) and description else None
            normalized.append({
                "barcode": str(barcode).strip() if barcode else "",
                "name": name or "",
                "quantity": product.get("quantity", 0),
                "id": product.get("id", ""),
            })
        return normalized

    # ------------------------------------------------------------- Compare --
    @staticmethod
    def compare_inventories(odoo_products: List[Dict], store_products: List[Dict]) -> List[Dict]:
        """Compare inventories using parallel processing for large datasets."""
        store_map = {item["barcode"]: item for item in store_products if item.get("barcode")}
        
        def compare_product(odoo_product: Dict) -> Optional[Dict]:
            barcode = odoo_product.get("barcode")
            if not barcode:
                return None
                
            store_product = store_map.get(barcode)
            odoo_qty = odoo_product.get("quantity", 0)
            store_qty = store_product.get("quantity", 0) if store_product else None

            record = {
                "barcode": barcode,
                "odoo_name": odoo_product.get("name", ""),
                "odoo_qty": odoo_qty,
                "store_name": store_product.get("name") if store_product else None,
                "store_qty": store_qty,
                "store_id": store_product.get("id") if store_product else None,
            }

            if store_product is None:
                # Only report as NOT FOUND if Odoo quantity > 0
                if odoo_qty > 0:
                    record.update({
                        "status": "❌ NOT FOUND IN STORE",
                        "difference": "N/A",
                    })
                else:
                    # Skip products with 0 quantity in Odoo that aren't in store
                    return None
            elif odoo_qty == store_qty:
                record.update({
                    "status": "✅ MATCH",
                    "difference": 0,
                })
            else:
                record.update({
                    "status": "⚠ QUANTITY MISMATCH",
                    "difference": odoo_qty - store_qty,
                })

            return record

        # Parallel comparison for large datasets
        comparison: List[Dict] = []
        if len(odoo_products) > 1000:
            with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
                futures = [executor.submit(compare_product, product) for product in odoo_products]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        comparison.append(result)
        else:
            # Sequential for smaller datasets
            for product in odoo_products:
                result = compare_product(product)
                if result:
                    comparison.append(result)

        logging.info("Compared %s products", len(comparison))
        return comparison

    # -------------------------------------------------------------- BigQuery --
    def load_dataframe(self, df: pd.DataFrame, table: str, disposition: str) -> None:
        client = bigquery.Client(project=self.bigquery_project)
        table_ref = f"{self.bigquery_project}.{self.dataset_id}.{table}"
        job_config = bigquery.LoadJobConfig(write_disposition=disposition)
        logging.info("Loading %s rows into %s", len(df), table_ref)
        load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()
        logging.info("Load complete: %s", table_ref)

    # ---------------------------------------------------------------- Public --
    def run(self) -> None:
        start = time.time()
        filter_info = f"with {len(self.location_ids)} location filters" if self.location_ids else "without filters"
        logging.info("=== Starting Inventory ETL %s (workers=%s) ===", filter_info, self.max_workers)

        # Fetch data from both sources in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            odoo_future = executor.submit(self.fetch_odoo_data)
            store_future = executor.submit(self.fetch_store_data)
            
            odoo_data = odoo_future.result()
            store_data = store_future.result()

        comparison = self.compare_inventories(odoo_data, store_data)

        if not comparison:
            logging.warning("No comparison data produced; exiting.")
            return

        timestamp = datetime.now(timezone.utc)
        rows = []
        for item in comparison:
            rows.append({
                "timestamp": timestamp,
                "barcode": item.get("barcode"),
                "odoo_name": item.get("odoo_name"),
                "odoo_qty": item.get("odoo_qty"),
                "store_name": item.get("store_name"),
                "store_qty": item.get("store_qty"),
                "store_id": item.get("store_id"),
                "status": item.get("status"),
                "difference": str(item.get("difference")),
            })

        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["odoo_qty"] = pd.to_numeric(df["odoo_qty"], errors="coerce").astype("Int64")
        df["store_qty"] = pd.to_numeric(df["store_qty"], errors="coerce").astype("Int64")

        # Upload both tables in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            main_future = executor.submit(
                self.load_dataframe,
                df,
                table=self.table_id,
                disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            
            staging_df = df.copy()
            staging_df.insert(0, "run_date", timestamp.date())
            staging_future = executor.submit(
                self.load_dataframe,
                staging_df,
                table=self.staging_table_id,
                disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            
            main_future.result()
            staging_future.result()

        elapsed = time.time() - start
        logging.info("=== ✅ ETL completed in %.2fs ===", elapsed)


def main() -> None:
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logging.critical("GOOGLE_APPLICATION_CREDENTIALS must point to a service account key file.")
        sys.exit(1)

    # Load location IDs if file exists
    location_file = os.getenv("ODOO_LOCATIONS_FILE", "odoo_locations.xlsx")
    location_ids = load_location_ids(location_file)
    
    if not location_ids:
        logging.warning("No location filters loaded - will fetch ALL locations")
    
    etl = InventoryETL(location_ids=location_ids)
    etl.run()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("ETL process failed")
        sys.exit(1)
