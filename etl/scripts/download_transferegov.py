#!/usr/bin/env python3
"""Download TransfereGov emendas parlamentares data from Portal da Transparencia.

Usage:
    python etl/scripts/download_transferegov.py
    python etl/scripts/download_transferegov.py --output-dir ./data/transferegov
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

# UNICO = full historical dataset (all years combined)
DOWNLOAD_URL = "https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/UNICO"

EXPECTED_FILES = [
    "EmendasParlamentares.csv",
    "EmendasParlamentares_PorFavorecido.csv",
    "EmendasParlamentares_Convenios.csv",
]


@click.command()
@click.option("--output-dir", default="./data/transferegov", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip if CSVs already exist")
@click.option("--timeout", type=int, default=600, help="Download timeout in seconds")
def main(output_dir: str, skip_existing: bool, timeout: int) -> None:
    """Download TransfereGov emendas parlamentares (UNICO ZIP)."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Check if all expected CSVs already exist
    if skip_existing and all((out / f).exists() for f in EXPECTED_FILES):
        logger.info("All CSVs already exist in %s — skipping download", out)
        return

    raw_dir = out / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    zip_path = raw_dir / "emendas-parlamentares-UNICO.zip"

    logger.info("=== TransfereGov download: emendas-parlamentares UNICO ===")

    if skip_existing and zip_path.exists():
        logger.info("Skipping (exists): %s", zip_path.name)
    else:
        if not download_file(DOWNLOAD_URL, zip_path, timeout=timeout):
            logger.error("Failed to download emendas-parlamentares ZIP")
            sys.exit(1)

    logger.info("Extracting to %s ...", out)
    extracted = extract_zip(zip_path, out)

    if not extracted:
        logger.error("ZIP extraction failed or empty")
        sys.exit(1)

    found = [f.name for f in extracted]
    logger.info("Extracted %d files: %s", len(found), found)

    missing = [f for f in EXPECTED_FILES if not (out / f).exists()]
    if missing:
        logger.warning("Expected files not found after extraction: %s", missing)
    else:
        logger.info("All expected CSVs present: %s", EXPECTED_FILES)

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
