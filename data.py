"""Standalone ETL script for running inventory comparison in CI/CD.

Reads configuration from environment variables so it can be executed inside
GitHub Actions or any other automation environment without depending on the
Flask app configuration module.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
import requests
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


class InventoryETL:
    """Encapsulates inventory comparison logic."""

    def __init__(self) -> None:
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

    def fetch_odoo_data(self) -> List[Dict]:
        session = self._get_odoo_session()
        offset = 0
        all_products: List[Dict] = []

        while True:
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
            if not batch:
                break
            all_products.extend(batch)
            offset += self.odoo_batch

        # Fetch stock quantities
        stock_payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "stock.quant",
                "method": "search_read",
                "args": [[["location_id.usage", "=", "internal"]]],
                "kwargs": {
                    "fields": ["product_id", "quantity", "reserved_quantity"],
                    "limit": 0,
                },
            },
        }
        rpc_url = f"{self.odoo_url}/web/dataset/call_kw"
        response = requests.post(rpc_url, json=stock_payload, cookies=session, timeout=60)
        response.raise_for_status()
        stock_quants = response.json().get("result", [])

        stock_map: Dict[int, Dict[str, float]] = {}
        for quant in stock_quants:
            product = quant.get("product_id")
            if isinstance(product, list):
                product_id = product[0]
            else:
                product_id = product
            entry = stock_map.setdefault(product_id, {"onHand": 0.0, "reserved": 0.0})
            entry["onHand"] += quant.get("quantity", 0.0)
            entry["reserved"] += quant.get("reserved_quantity", 0.0)

        results: List[Dict] = []
        for product in all_products:
            product_id = product.get("id")
            stock_info = stock_map.get(product_id, {"onHand": 0.0, "reserved": 0.0})
            available = max(stock_info["onHand"] - stock_info["reserved"], 0.0)
            results.append({
                "barcode": product.get("barcode", ""),
                "name": product.get("display_name", ""),
                "quantity": int(available),
            })

        logging.info("Fetched %s products from Odoo", len(results))
        return results

    # ---------------------------------------------------------------- Store --
    def fetch_store_data(self) -> List[Dict]:
        page = 1
        all_products: List[Dict] = []

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.store_api_token}",
        }

        while True:
            url = f"{self.store_api_url}/products/limit/{self.store_batch}/page/{page}"
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            payload = response.json()
            if payload.get("success") != 1:
                break
            data = payload.get("data") or []
            if not data:
                break
            all_products.extend(data)
            page += 1

        logging.info("Fetched %s products from Store API", len(all_products))
        normalized: List[Dict] = []
        for product in all_products:
            barcode = product.get("upc")
            name = None
            description = product.get("product_description")
            if isinstance(description, list) and description:
                name = description[0].get("name")
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
        store_map = {item["barcode"]: item for item in store_products if item.get("barcode")}
        comparison: List[Dict] = []

        for odoo_product in odoo_products:
            barcode = odoo_product.get("barcode")
            if not barcode:
                continue
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
                record.update({
                    "status": "❌ NOT FOUND IN STORE",
                    "difference": "N/A",
                })
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

            comparison.append(record)

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
        logging.info("=== Starting Inventory ETL ===")

        odoo_data = self.fetch_odoo_data()
        store_data = self.fetch_store_data()
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

        # Upload main table and staging snapshot
        self.load_dataframe(
            df,
            table=self.table_id,
            disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        staging_df = df.copy()
        staging_df.insert(0, "run_date", timestamp.date())
        self.load_dataframe(
            staging_df,
            table=self.staging_table_id,
            disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        elapsed = time.time() - start
        logging.info("=== ETL completed in %.2fs ===", elapsed)


def main() -> None:
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logging.critical("GOOGLE_APPLICATION_CREDENTIALS must point to a service account key file.")
        sys.exit(1)

    etl = InventoryETL()
    etl.run()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("ETL process failed")
        sys.exit(1)
