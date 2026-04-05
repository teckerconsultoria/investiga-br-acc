#!/usr/bin/env python3
"""Download CEAF (expelled servants) data from Portal da Transparencia API.

The government migrated from static ZIP downloads to a paginated REST API.
This script fetches all pages via the API and produces a flat CSV compatible
with the existing ETL pipeline.

Usage:
    python etl/scripts/download_ceaf_api.py
    python etl/scripts/download_ceaf_api.py --output-dir ./data/ceaf
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import click
import httpx
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
PAGE_SIZE = 15  # API default page size
REQUEST_DELAY = 0.5  # seconds between requests (rate limiting)
MAX_PAGES = 10000  # safety limit


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


def _fetch_all_pages(api_key: str) -> list[dict]:
    """Fetch all pages from the CEAF API endpoint."""
    url = f"{API_BASE}/ceaf"
    all_records: list[dict] = []
    pagina = 1

    while True:
        params = {"pagina": pagina}
        headers = {"chave-api-dados": api_key, "Accept": "application/json"}

        try:
            resp = httpx.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("API request failed (page %d): %s", pagina, e)
            break

        data = resp.json()
        if not data:
            break

        all_records.extend(data)
        logger.info("Page %d: fetched %d records (total: %d)", pagina, len(data), len(all_records))

        if len(data) < PAGE_SIZE or pagina >= MAX_PAGES:
            break

        pagina += 1
        time.sleep(REQUEST_DELAY)

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
def main(api_key: str, output_dir: str, skip_existing: bool) -> None:
    """Download CEAF expelled servants data via API."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    output_path = out / "ceaf.csv"
    if skip_existing and output_path.exists():
        logger.info("Skipping (exists): %s", output_path)
        return

    logger.info("Fetching CEAF data from API...")
    records = _fetch_all_pages(api_key)

    if not records:
        logger.error("No records fetched from API")
        sys.exit(1)

    logger.info("Flattening %d records...", len(records))
    flat_records = [_flatten_record(r) for r in records]

    df = pd.DataFrame(flat_records)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Wrote %d rows to %s", len(df), output_path)
    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
