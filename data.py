"""Parallelized ETL script with location filtering for Odoo inventory.

Reads location IDs from an Excel file and fetches inventory only for those locations.
Compares Odoo vs Store API and saves results as Excel files (no BigQuery).
"""

from __future__ import annotations

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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Embedded Odoo credentials (used only when the corresponding environment
# variables are not already set). WARNING: embedding secrets in source is
# insecure for production — prefer environment variables or a secrets manager.
os.environ.setdefault("ODOO_URL", "https://rahatystore.odoo.com")
os.environ.setdefault("ODOO_DB", "rahatystore-live-12723857")
os.environ.setdefault("ODOO_USERNAME", "Data.team@rahatystore.com")
os.environ.setdefault("ODOO_PASSWORD", "Rs.Data.team")

# Embedded Store API credentials (used only when the corresponding
# environment variables are not already set). WARNING: embedding secrets
# in source is insecure for production — prefer environment variables
# or a secrets manager.
os.environ.setdefault("STORE_API_URL", "https://rahatystore.com/api/rest_admin")
os.environ.setdefault("STORE_API_KEY", "eab456724eb0b65063423a00718d9630530e9058")


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
    """Load location IDs from Excel file (expects column 'id')."""
    if not Path(filepath).exists():
        logging.error("Location file not found: %s", filepath)
        return set()
    
    try:
        df = pd.read_excel(filepath)
        if "id" not in df.columns:
            logging.error("Column 'id' not found in %s", filepath)
            return set()
        
        location_ids = set(df["id"].dropna().astype(int).tolist())
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

        self.odoo_batch = _load_env_int("ODOO_BATCH", 200)
        self.store_batch = _load_env_int("STORE_BATCH", 100)  # Reduced from 200 for stability
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
        
        # First, get initial batch
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
                "product_id": product_id,  # Include product_id for location lookup
            })

        logging.info("Fetched %s products from Odoo (filtered by locations)", len(results))
        return results

    def fetch_stock_by_location(self, session: Dict) -> Dict[int, List[Dict]]:
        """Fetch detailed stock information grouped by product and location.
        
        Returns a dict mapping product_id to list of location records where
        each record has 'location_name' and 'available_qty'.
        """
        stock_domain = [["location_id.usage", "=", "internal"]]
        
        if self.location_ids:
            stock_domain.append(["location_id", "in", list(self.location_ids)])
        
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
        
        logging.info("Fetched %s stock quants for detailed location analysis", len(stock_quants))
        
        # Group by product_id
        stock_by_product: Dict[int, List[Dict]] = {}
        for quant in stock_quants:
            product = quant.get("product_id")
            product_id = product[0] if isinstance(product, list) else product
            
            location = quant.get("location_id")
            location_name = location[1] if isinstance(location, list) and len(location) > 1 else "Unknown"
            
            quantity = quant.get("quantity", 0.0)
            reserved = quant.get("reserved_quantity", 0.0)
            available = max(quantity - reserved, 0.0)
            
            if available > 0:  # Only store locations with available stock
                if product_id not in stock_by_product:
                    stock_by_product[product_id] = []
                stock_by_product[product_id].append({
                    "location_name": location_name,
                    "available_qty": available,
                })
        
        logging.info("Grouped stock by location for %s products", len(stock_by_product))
        return stock_by_product

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
                logging.warning(
                    "⚠️ Store API chunked error on page %s, attempt %s/%s",
                    page, attempt + 1, max_retries
                )
                time.sleep(2 ** attempt)
            except requests.exceptions.RequestException as e:
                logging.error("❌ Store API request failed on page %s: %s", page, e)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return []
        
        return []

    def fetch_store_data(self) -> List[Dict]:
        """Fetch Store data using a resilient paged fetcher provided by the user.

        This method pages through `/products/limit/{batch}/page/{page}` until
        no more data is returned. It implements retries for transient
        errors and stops after several consecutive page failures.
        """
        page = 1
        all_products: List[Dict] = []
        max_retries = 5  # Increased from 3 for better reliability
        consecutive_failures = 0
        max_consecutive_failures = 5  # Allow more failures before stopping

        logging.info("📦 Starting fetch of Store products from %s", self.store_api_url)

        while True:
            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    url = f"{self.store_api_url}/products/limit/{self.store_batch}/page/{page}"
                    headers = {
                        'accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Authorization': f"Bearer {self.store_api_token}",
                        'Connection': 'keep-alive'
                    }

                    response = requests.get(
                        url,
                        headers=headers,
                        timeout=60,
                        stream=False,
                        verify=True,
                    )
                    response.raise_for_status()
                    payload = response.json()

                    if payload.get('success') == 1 and payload.get('data') and len(payload['data']) > 0:
                        all_products.extend(payload['data'])
                        logging.info("📦 Store page %s: fetched %s products (total: %s)", page, len(payload['data']), len(all_products))
                        page += 1
                        consecutive_failures = 0
                        success = True
                        time.sleep(2)  # Increased delay to reduce server load
                    else:
                        logging.info("✅ No more store data at page %s", page)
                        # Normal termination
                        normalized: List[Dict] = []
                        for product in all_products:
                            barcode = product.get('upc')
                            description = product.get('product_description')
                            name = None
                            if isinstance(description, list) and description:
                                name = description[0].get('name')
                            normalized.append({
                                'barcode': str(barcode).strip() if barcode else '',
                                'name': name or '',
                                'quantity': product.get('quantity', 0),
                                'id': product.get('id', ''),
                            })
                        logging.info("✅ Total fetched from Store API: %s products", len(all_products))
                        return normalized

                except requests.exceptions.ChunkedEncodingError as e:
                    retry_count += 1
                    logging.warning("⚠️ Store API chunked error on page %s, attempt %s/%s: %s", page, retry_count, max_retries, e)
                    time.sleep(3 * retry_count)  # Longer backoff for chunked errors
                except requests.exceptions.ConnectionError as e:
                    retry_count += 1
                    logging.warning("⚠️ Store API connection error on page %s, attempt %s/%s: %s", page, retry_count, max_retries, e)
                    time.sleep(2 * retry_count)
                except requests.exceptions.Timeout as e:
                    retry_count += 1
                    logging.warning("⚠️ Store API timeout on page %s, attempt %s/%s: %s", page, retry_count, max_retries, e)
                    time.sleep(2 * retry_count)
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    logging.error("❌ Store API request failed on page %s (attempt %s/%s): %s", page, retry_count, max_retries, e)
                    time.sleep(2 * retry_count)
                except ValueError as e:
                    retry_count += 1
                    logging.error("⚠️ Failed to parse JSON from Store API on page %s (attempt %s/%s): %s", page, retry_count, max_retries, e)
                    time.sleep(2 * retry_count)
                except Exception as e:
                    retry_count += 1
                    logging.exception("⚠️ Unexpected error fetching Store page %s (attempt %s/%s): %s", page, retry_count, max_retries, e)
                    time.sleep(2 * retry_count)

            if not success:
                consecutive_failures += 1
                logging.error("❌ Failed to fetch Store page %s after %s attempts", page, max_retries)

                if consecutive_failures >= max_consecutive_failures:
                    logging.error("⚠️ Stopping after %s consecutive page failures", consecutive_failures)
                    break

                # Try the next page after a longer pause
                page += 1
                time.sleep(5)  # Longer pause before trying next page

        # If we exit due to failures, still return whatever we collected
        normalized: List[Dict] = []
        for product in all_products:
            barcode = product.get('upc')
            description = product.get('product_description')
            name = None
            if isinstance(description, list) and description:
                name = description[0].get('name')
            normalized.append({
                'barcode': str(barcode).strip() if barcode else '',
                'name': name or '',
                'quantity': product.get('quantity', 0),
                'id': product.get('id', ''),
            })
        logging.info("✅ Returning %s products fetched from Store API (partial) due to errors", len(all_products))
        return normalized

    # ------------------------------------------------------------- Compare --
    @staticmethod
    def compare_inventories(
        odoo_products: List[Dict], 
        store_products: List[Dict],
        stock_by_location: Optional[Dict[int, List[Dict]]] = None
    ) -> List[Dict]:
        """Compare inventories using parallel processing for large datasets.
        
        Args:
            odoo_products: List of Odoo products with barcode, name, quantity, and product_id
            store_products: List of Store products
            stock_by_location: Optional dict mapping product_id to list of location records
        """
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
                    # Find locations with available qty > 3
                    locations_with_stock = []
                    if stock_by_location and odoo_product.get("product_id"):
                        product_id = odoo_product["product_id"]
                        location_list = stock_by_location.get(product_id, [])
                        locations_with_stock = [
                            f"{loc['location_name']} ({int(loc['available_qty'])} units)"
                            for loc in location_list
                            if loc["available_qty"] > 3
                        ]
                    
                    record.update({
                        "status": "❌ NOT FOUND IN STORE",
                        "difference": "N/A",
                        "locations_with_stock": "; ".join(locations_with_stock) if locations_with_stock else "No location with qty > 3",
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

        comparison: List[Dict] = []
        if len(odoo_products) > 1000:
            with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
                futures = [executor.submit(compare_product, product) for product in odoo_products]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        comparison.append(result)
        else:
            for product in odoo_products:
                result = compare_product(product)
                if result:
                    comparison.append(result)

        logging.info("Compared %s products", len(comparison))
        return comparison

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

        # Fetch detailed stock by location for NOT FOUND analysis
        logging.info("📍 Fetching detailed stock by location...")
        session = self._get_odoo_session()
        stock_by_location = self.fetch_stock_by_location(session)

        comparison = self.compare_inventories(odoo_data, store_data, stock_by_location)

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
                "difference": item.get("difference"),
                "locations_with_stock": item.get("locations_with_stock", ""),
            })

        df = pd.DataFrame(rows)

        # Ensure consistent dtypes
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["odoo_qty"] = pd.to_numeric(df["odoo_qty"], errors="coerce").fillna(0).astype("int64")
        df["store_qty"] = pd.to_numeric(df["store_qty"], errors="coerce")
        # store_qty may be NaN when not found in store, keep as float
        df["barcode"] = df["barcode"].astype(str)
        df["odoo_name"] = df["odoo_name"].astype(str)
        df["store_name"] = df["store_name"].fillna("").astype(str)
        df["store_id"] = df["store_id"].fillna("").astype(str)
        df["status"] = df["status"].astype(str)
        df["difference"] = df["difference"].astype(str)
        df["locations_with_stock"] = df["locations_with_stock"].fillna("").astype(str)

        logging.info("Prepared %s records for Excel export", len(df))
        logging.info("Timestamp range: %s to %s", df["timestamp"].min(), df["timestamp"].max())
        logging.info("DataFrame dtypes:\n%s", df.dtypes)

        # ---------------------------------------------------------
        # Ensure timestamps are timezone-naive for Excel export
        # Excel does not support timezone-aware datetimes; convert to naive UTC
        try:
            if getattr(df["timestamp"].dtype, "tz", None) is not None:
                df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
                logging.info("Converted timezone-aware timestamps to UTC naive datetimes for Excel export")
        except Exception:
            # Fallback: remove tzinfo per cell (works even if some rows are naive)
            df["timestamp"] = df["timestamp"].apply(
                lambda t: t.replace(tzinfo=None) if getattr(t, "tzinfo", None) is not None else t
            )

        # ---------------------------------------------------------
        # Save results locally as Excel files
        # ---------------------------------------------------------
        output_dir = os.getenv("ETL_OUTPUT_DIR", "etl_outputs")
        os.makedirs(output_dir, exist_ok=True)

        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # 1) Full comparison file with all columns and conditions
        full_path = os.path.join(output_dir, f"inventory_comparison_{timestamp_str}.xlsx")
        df.to_excel(full_path, index=False)
        logging.info("📁 Saved full comparison Excel file: %s", full_path)

        # 2) Optional per-status summary files
        matched = df[df["status"] == "✅ MATCH"]
        mismatch = df[df["status"] == "⚠ QUANTITY MISMATCH"]
        not_found = df[df["status"] == "❌ NOT FOUND IN STORE"]

        if not matched.empty:
            matched_path = os.path.join(output_dir, f"matched_{timestamp_str}.xlsx")
            matched.to_excel(matched_path, index=False)
            logging.info("📊 Saved MATCHED Excel file: %s", matched_path)

        if not mismatch.empty:
            mismatch_path = os.path.join(output_dir, f"mismatch_{timestamp_str}.xlsx")
            mismatch.to_excel(mismatch_path, index=False)
            logging.info("📊 Saved MISMATCH Excel file: %s", mismatch_path)

        if not not_found.empty:
            not_found_path = os.path.join(output_dir, f"not_found_{timestamp_str}.xlsx")
            not_found.to_excel(not_found_path, index=False)
            logging.info("📊 Saved NOT FOUND Excel file: %s", not_found_path)

        elapsed = time.time() - start
        logging.info("=== ✅ ETL completed in %.2fs ===", elapsed)


def main() -> None:
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
 
