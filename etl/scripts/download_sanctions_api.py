#!/usr/bin/env python3
"""Download CEIS/CNEP sanctions data from Portal da Transparencia API.

The government migrated from static ZIP downloads to a paginated REST API.
This script fetches all pages for both CEIS and CNEP using parallel requests
for performance and produces flat CSVs compatible with the existing ETL pipeline.

Usage:
    python etl/scripts/download_sanctions_api.py
    python etl/scripts/download_sanctions_api.py --output-dir ./data/sanctions
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


def _flatten_ceis(rec: dict) -> dict:
    """Flatten CEIS API record into flat CSV columns."""
    sancionado = rec.get("sancionado") or {}
    tipo = rec.get("tipoSancao") or {}
    fundamentacoes = rec.get("fundamentacao") or []

    motivo = ""
    if fundamentacoes:
        motivo = fundamentacoes[0].get("codigo", "").strip()

    return {
        "cpf_cnpj": (sancionado.get("codigoFormatado") or "").strip(),
        "nome": (sancionado.get("nome") or "").strip(),
        "tipo_sancao": (tipo.get("descricaoResumida") or tipo.get("descricaoPortal") or "").strip(),
        "data_inicio": (rec.get("dataInicioSancao") or "").strip(),
        "data_fim": (rec.get("dataFimSancao") or "").strip(),
        "motivo": motivo,
    }


def _flatten_cnep(rec: dict) -> dict:
    """Flatten CNEP API record into flat CSV columns."""
    pessoa = rec.get("pessoa") or {}
    punido = rec.get("punido") or {}
    tipo = rec.get("tipoPunicao") or {}
    fundamentacoes = rec.get("fundamentacao") or []

    motivo = ""
    if fundamentacoes:
        motivo = fundamentacoes[0].get("codigo", "").strip()

    doc = (pessoa.get("cpfFormatado") or pessoa.get("cnpjFormatado") or
           punido.get("cpfFormatado") or punido.get("cnpjFormatado") or "").strip()
    nome = (pessoa.get("nome") or pessoa.get("razaoSocialReceita") or
            punido.get("nome") or "").strip()

    return {
        "cpf_cnpj": doc,
        "nome": nome,
        "tipo_sancao": (tipo.get("descricao") or tipo.get("descricaoResumida") or "").strip(),
        "data_inicio": (rec.get("dataInicioSancao") or rec.get("dataPublicacao") or "").strip(),
        "data_fim": (rec.get("dataFimSancao") or "").strip(),
        "motivo": motivo,
    }


def _fetch_page(endpoint: str, api_key: str, pagina: int) -> list[dict] | None:
    """Fetch a single page from the API."""
    url = f"{API_BASE}/{endpoint}"
    params = {"pagina": pagina}
    headers = {"chave-api-dados": api_key, "Accept": "application/json"}

    try:
        resp = httpx.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as e:
        logger.error("API request failed for %s (page %d): %s", endpoint, pagina, e)
        return None


def _fetch_all_pages_parallel(endpoint: str, api_key: str) -> list[dict]:
    """Fetch all pages using parallel requests for performance."""
    # Phase 1: fetch first page
    first_page = _fetch_page(endpoint, api_key, 1)
    if not first_page:
        logger.error("Failed to fetch first page for %s", endpoint)
        return []

    all_records: list[dict] = list(first_page)
    logger.info("%s page 1: fetched %d records (total: %d)", endpoint.upper(), len(first_page), len(all_records))

    if len(first_page) < PAGE_SIZE:
        return all_records

    # Phase 2: exponential search to find end
    low, high = 2, 256
    while True:
        high_data = _fetch_page(endpoint, api_key, high)
        if not high_data or len(high_data) < PAGE_SIZE or high >= MAX_PAGES:
            break
        low = high
        high = min(high * 2, MAX_PAGES)

    # Phase 3: fetch remaining pages in parallel
    pages_to_fetch = list(range(2, high + 1))
    results: dict[int, list[dict] | None] = {}

    with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
        future_to_page = {
            executor.submit(_fetch_page, endpoint, api_key, p): p
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

                if completed_count % 100 == 0 or completed_count == total:
                    logger.info("%s: fetched %d/%d pages (%d records so far)",
                               endpoint.upper(), completed_count, total, len(all_records))
            except Exception as e:
                logger.error("Page %d failed: %s", page, e)
                results[page] = None

    # Phase 4: assemble results in order
    for page_num in sorted(results.keys()):
        data = results[page_num]
        if data:
            all_records.extend(data)
        else:
            break

    return all_records


@click.command()
@click.option(
    "--api-key",
    envvar="PORTAL_API_KEY",
    required=True,
    help="Portal da Transparencia API key (or set PORTAL_API_KEY env var)",
)
@click.option("--output-dir", default="./data/sanctions", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip if CSVs exist")
@click.option(
    "--parallel/--sequential",
    default=True,
    help="Use parallel requests (much faster for large datasets)",
)
def main(api_key: str, output_dir: str, skip_existing: bool, parallel: bool) -> None:
    """Download CEIS and CNEP sanctions data via API."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    success_count = 0

    for dataset, flatten_fn, filename in [
        ("ceis", _flatten_ceis, "ceis.csv"),
        ("cnep", _flatten_cnep, "cnep.csv"),
    ]:
        output_path = out / filename
        if skip_existing and output_path.exists():
            logger.info("Skipping (exists): %s", output_path)
            success_count += 1
            continue

        start_time = time.time()

        if parallel:
            logger.info("=== Fetching %s (parallel mode) ===", dataset.upper())
            records = _fetch_all_pages_parallel(dataset, api_key)
        else:
            # Fallback sequential
            logger.info("=== Fetching %s (sequential mode) ===", dataset.upper())
            records = []
            pagina = 1
            while True:
                data = _fetch_page(dataset, api_key, pagina)
                if not data or not data:
                    break
                records.extend(data)
                logger.info("%s page %d: fetched %d records (total: %d)",
                           dataset.upper(), pagina, len(data), len(records))
                if len(data) < PAGE_SIZE:
                    break
                pagina += 1
                time.sleep(0.5)

        elapsed = time.time() - start_time

        if not records:
            logger.warning("No records fetched for %s", dataset)
            continue

        logger.info("Flattening %d %s records...", len(records), dataset)
        flat_records = [flatten_fn(r) for r in records]

        df = pd.DataFrame(flat_records)
        df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info("Wrote %d rows to %s in %.1fs", len(df), output_path, elapsed)
        success_count += 1

    logger.info("=== Done: %d/2 datasets downloaded ===", success_count)
    if success_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
