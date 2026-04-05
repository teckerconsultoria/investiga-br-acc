#!/usr/bin/env python3
"""Download PGFN (Procuradoria-Geral da Fazenda Nacional) divida ativa data.

Files are published quarterly at dadosabertos.pgfn.gov.br.
Only the 'Nao_Previdenciario' (Sistema SIDA) ZIP is downloaded — this contains
the arquivo_lai_SIDA_*_*.csv files consumed by the ETL pipeline.

Usage:
    python etl/scripts/download_pgfn.py
    python etl/scripts/download_pgfn.py --year 2025 --quarter 1
    python etl/scripts/download_pgfn.py --year 2024 --quarter 4 --output-dir ./data/pgfn
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent))
from _download_utils import download_file, extract_zip

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://dadosabertos.pgfn.gov.br"


def _zip_url(year: int, quarter: int) -> str:
    return f"{BASE_URL}/{year}_trimestre_{quarter:02d}/Dados_abertos_Nao_Previdenciario.zip"


@click.command()
@click.option("--year", type=int, default=2025, help="Year of the quarterly dataset")
@click.option("--quarter", type=click.Choice(["1", "2", "3", "4"]), default="1", help="Quarter (1-4)")
@click.option("--output-dir", default="./data/pgfn", help="Output directory (CSVs placed here)")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip if CSVs already exist")
@click.option("--timeout", type=int, default=600, help="Download timeout in seconds")
def main(year: int, quarter: str, output_dir: str, skip_existing: bool, timeout: int) -> None:
    """Download PGFN divida ativa (Sistema SIDA) for a given quarter."""
    q = int(quarter)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Check for existing SIDA CSVs
    existing = list(out.glob("arquivo_lai_SIDA_*.csv"))
    if skip_existing and existing:
        logger.info("Found %d existing SIDA CSV(s) in %s — skipping download", len(existing), out)
        return

    raw_dir = out / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    zip_name = f"pgfn_{year}_trimestre_{q:02d}_Nao_Previdenciario.zip"
    zip_path = raw_dir / zip_name
    url = _zip_url(year, q)

    logger.info("=== PGFN download: %d Q%d ===", year, q)
    logger.info("URL: %s", url)

    if skip_existing and zip_path.exists():
        logger.info("Skipping (exists): %s", zip_name)
    else:
        if not download_file(url, zip_path, timeout=timeout):
            logger.error("Failed to download PGFN ZIP: %s", url)
            sys.exit(1)

    logger.info("Extracting to %s ...", out)
    extracted = extract_zip(zip_path, out)

    if not extracted:
        logger.error("ZIP extraction failed or empty")
        sys.exit(1)

    sida_files = [f for f in extracted if "SIDA" in f.name and f.suffix.lower() == ".csv"]
    logger.info("Extracted %d SIDA CSV file(s): %s", len(sida_files), [f.name for f in sida_files])

    if not sida_files:
        logger.warning("No arquivo_lai_SIDA_*.csv found — check ZIP contents: %s", [f.name for f in extracted])

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
