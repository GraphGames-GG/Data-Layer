from __future__ import annotations
from pathlib import Path
import pandas as pd

COLUMNS_WANTED = [
    "appid",
    "name",
    "release_date",
    "required_age",
    "price",
    "dlc_count",
    "detailed_description",
    "about_the_game",
    "short_description",
    "categories",
]

def run(input_csv: Path, output_csv: Path) -> Path:
    df = pd.read_csv(input_csv)

    # garante colunas
    missing = [c for c in COLUMNS_WANTED if c not in df.columns]
    if missing:
        raise ValueError(f"Bronze: colunas faltando: {missing}")

    df = df[COLUMNS_WANTED].copy()

    # tipos básicos
    df["appid"] = pd.to_numeric(df["appid"], errors="coerce")
    df["required_age"] = pd.to_numeric(df["required_age"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["dlc_count"] = pd.to_numeric(df["dlc_count"], errors="coerce")

    # strings: strip
    for col in ["name", "detailed_description", "about_the_game", "short_description", "categories"]:
        df[col] = df[col].astype("string").fillna("").str.strip()

    # release_date como string “limpa” (parse oficial fica na silver)
    df["release_date"] = df["release_date"].astype("string").fillna("").str.strip()

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    return output_csv