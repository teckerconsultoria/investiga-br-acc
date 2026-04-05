#!/usr/bin/env python3
"""Download CEAF (expelled servants) data from Portal da Transparencia API.

The government migrated from static ZIP downloads to a paginated REST API.
This script fetches all pages via the API using parallel requests for
performance and produces a flat CSV compatible with the existing ETL pipeline.

Usage:
    python etl/scripts/download_ceaf_api.py
    python etl/scripts/download_ceaf_api.py --output-dir ./data/ceaf
"""

from __future__ import annotations

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click
import httpx
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
PAGE_SIZE = 15  # API returns exactly 15 records per page (fixed)
MAX_PAGES = 10000  # safety limit
CONCURRENT_REQUESTS = 10  # parallel fetch pages


def _flatten_record(rec: dict) -> dict:
    """Flatten nested API record into flat CSV columns expected by the ETL pipeline."""
    pessoa = rec.get("pessoa") or {}
    punicao = rec.get("punicao") or {}
    tipo = rec.get("tipoPunicao") or {}
    orgao = rec.get("orgaoLotacao") or {}
    uf_obj = orgao.get("uf") or {}

    return {
        "cpf": (pessoa.get("cpfFormatado") or punicao.get("cpfPunidoFormatado") or "").strip(),
        "nome": (pessoa.get("nome") or punicao.get("nomePunido") or "").strip(),
        "cargo_efetivo": (rec.get("cargoEfetivo") or "").strip(),
        "tipo_punicao": (tipo.get("descricao") or "").strip(),
        "data_publicacao": (rec.get("dataPublicacao") or "").strip(),
        "portaria": (punicao.get("portaria") or "").strip(),
        "uf": (uf_obj.get("sigla") or "").strip(),
    }


def _fetch_page(api_key: str, pagina: int) -> list[dict] | None:
    """Fetch a single page from the API."""
    url = f"{API_BASE}/ceaf"
    params = {"pagina": pagina}
    headers = {"chave-api-dados": api_key, "Accept": "application/json"}

    try:
        resp = httpx.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as e:
        logger.error("API request failed (page %d): %s", pagina, e)
        return None


def _fetch_all_pages_parallel(api_key: str) -> list[dict]:
    """Fetch all pages using parallel requests for performance."""
    # Phase 1: fetch first page to check if data exists
    first_page = _fetch_page(api_key, 1)
    if not first_page:
        logger.error("Failed to fetch first page")
        return []

    all_records: list[dict] = list(first_page)
    logger.info("Page 1: fetched %d records (total: %d)", len(first_page), len(all_records))

    if len(first_page) < PAGE_SIZE:
        return all_records

    # Phase 2: estimate total pages by exponential search
    low, high = 2, 256
    while True:
        high_data = _fetch_page(api_key, high)
        if not high_data or len(high_data) < PAGE_SIZE or high >= MAX_PAGES:
            break
        low = high
        high = min(high * 2, MAX_PAGES)

    # Phase 3: fetch remaining pages in parallel batches
    pages_to_fetch = list(range(2, high + 1))
    results: dict[int, list[dict] | None] = {}

    with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
        future_to_page = {
            executor.submit(_fetch_page, api_key, p): p
            for p in pages_to_fetch
        }

        completed_count = 0
        total = len(pages_to_fetch)
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                data = future.result()
                results[page] = data
                completed_count += 1

                if completed_count % 50 == 0 or completed_count == total:
                    logger.info("Fetched %d/%d pages (%d records so far)", completed_count, total, len(all_records))
            except Exception as e:
                logger.error("Page %d failed: %s", page, e)
                results[page] = None

    # Phase 4: assemble results in order
    for page_num in sorted(results.keys()):
        data = results[page_num]
        if data:
            all_records.extend(data)
        else:
            break  # stop at first empty/failed page

    return all_records


@click.command()
@click.option(
    "--api-key",
    envvar="PORTAL_API_KEY",
    required=True,
    help="Portal da Transparencia API key (or set PORTAL_API_KEY env var)",
)
@click.option("--output-dir", default="./data/ceaf", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip if ceaf.csv exists")
@click.option(
    "--parallel/--sequential",
    default=True,
    help="Use parallel requests (much faster for large datasets)",
)
def main(api_key: str, output_dir: str, skip_existing: bool, parallel: bool) -> None:
    """Download CEAF expelled servants data via API."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    output_path = out / "ceaf.csv"
    if skip_existing and output_path.exists():
        logger.info("Skipping (exists): %s", output_path)
        return

    start_time = time.time()

    if parallel:
        logger.info("Fetching CEAF data from API (parallel mode)...")
        records = _fetch_all_pages_parallel(api_key)
    else:
        logger.info("Fetching CEAF data from API (sequential mode)...")
        # fallback to sequential if needed
        from download_ceaf_api import _fetch_all_pages as _fetch_seq  # type: ignore
        records = _fetch_seq(api_key)

    elapsed = time.time() - start_time

    if not records:
        logger.error("No records fetched from API")
        sys.exit(1)

    logger.info("Flattening %d records...", len(records))
    flat_records = [_flatten_record(r) for r in records]

    df = pd.DataFrame(flat_records)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Wrote %d rows to %s in %.1fs", len(df), output_path, elapsed)
    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
