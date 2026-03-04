from __future__ import annotations
from pathlib import Path
import pandas as pd

def _parse_release_date(series: pd.Series) -> pd.Series:
    # tenta converter; se falhar vira NaT
    dt = pd.to_datetime(series, errors="coerce", utc=False)
    # mantém só a data (DATE)
    return dt.dt.date

def run(input_csv: Path, output_csv: Path) -> Path:
    df = pd.read_csv(input_csv)

    # remove registros inválidos
    df["appid"] = pd.to_numeric(df["appid"], errors="coerce")
    df = df.dropna(subset=["appid", "name"])
    df["appid"] = df["appid"].astype("int64")

    # nulos numéricos -> defaults razoáveis
    for col, default in [("required_age", 0), ("dlc_count", 0), ("price", 0)]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)

    # correções básicas
    df["required_age"] = df["required_age"].clip(lower=0).astype("int64")
    df["dlc_count"] = df["dlc_count"].clip(lower=0).astype("int64")
    df["price"] = df["price"].clip(lower=0)

    # outliers de preço via IQR (cap, não drop)
    q1 = df["price"].quantile(0.25)
    q3 = df["price"].quantile(0.75)
    iqr = q3 - q1
    upper = q3 + 1.5 * iqr
    if pd.notna(upper):
        df["price"] = df["price"].clip(upper=upper)

    # release_date -> date
    df["release_date"] = _parse_release_date(df["release_date"].astype("string"))

    # dedup por appid (se tiver duplicado, mantém o primeiro)
    df = df.drop_duplicates(subset=["appid"], keep="first")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    return output_csv