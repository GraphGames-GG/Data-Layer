from __future__ import annotations
from pathlib import Path

from pipeline.source.downloader import download_raw_dataset
from pipeline.bronze.core import run as bronze_run
from pipeline.silver.core import run as silver_run
from pipeline.gold.core import run as gold_run
from infra.bootstrap_database import main as bootstrap_main

def data_layer_root() -> Path:
    return Path(__file__).resolve().parent

def main():
    root = data_layer_root()
    pipeline = root / "pipeline"

    # 1) RAW
    raw_result = download_raw_dataset()
    raw_csv = raw_result.raw_csv_path
    print(f"[orc] RAW: {raw_csv} (source={raw_result.source}, fallback={raw_result.used_fallback})")

    # 2) BRONZE
    bronze_csv = pipeline / "bronze" / "games_bronze.csv"
    bronze_run(raw_csv, bronze_csv)
    print(f"[orc] BRONZE: {bronze_csv}")

    # 3) SILVER
    silver_csv = pipeline / "silver" / "games_silver.csv"
    silver_run(bronze_csv, silver_csv)
    print(f"[orc] SILVER: {silver_csv}")

    # 4) GOLD
    gold_dir = pipeline / "gold" / "artifacts"
    artifacts = gold_run(silver_csv, gold_dir)
    print(f"[orc] GOLD: {artifacts}")

    # 5) DOCKER + LOAD DB
    bootstrap_main()
    print("[orc] Pipeline completa ✅")

if __name__ == "__main__":
    main()