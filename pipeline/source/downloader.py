import os
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

DATASET_SLUG = "artermiloff/steam-games-dataset"
DATASET_FOLDER_NAME = "steam-games-dataset"
TARGET_CSV = "games_march2025_cleaned.csv"


@dataclass(frozen=True)
class DownloadResult:
    raw_csv_path: Path
    used_fallback: bool


def data_layer_root() -> Path:
    """
    Estrutura esperada:
    Data-Layer/
      pipeline/
        source/
          downloader.py  <- este arquivo

    Então o root do Data-Layer é 2 níveis acima de pipeline/source.
    """
    return Path(__file__).resolve().parents[2]


def has_kaggle_credentials() -> bool:
    """
    Kaggle API procura credenciais via:
    - env vars: KAGGLE_USERNAME e KAGGLE_KEY
    - arquivo: ~/.kaggle/kaggle.json (ou %USERPROFILE%\.kaggle\kaggle.json)
    """
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return True

    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    return kaggle_json.exists()


def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("rb") as r, dst.open("wb") as w:
        while True:
            chunk = r.read(1024 * 1024)
            if not chunk:
                break
            w.write(chunk)


def download_zip_with_kaggle(out_dir: Path) -> Path:
    """
    Baixa o ZIP do dataset via Kaggle API.
    Observação: a Kaggle API baixa o dataset como um .zip completo.
    """
    from kaggle.api.kaggle_api_extended import KaggleApi

    out_dir.mkdir(parents=True, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(DATASET_SLUG, path=str(out_dir), quiet=False)

    zips = sorted(out_dir.glob("*.zip"))
    if not zips:
        raise FileNotFoundError(f"Download parece ter falhado: nenhum .zip em {out_dir}")
    return max(zips, key=lambda p: p.stat().st_size)


def extract_only_target_csv(zip_path: Path, out_dir: Path, target_csv: str) -> Path:
    """
    Extrai apenas o CSV desejado do zip para out_dir.
    Não extrai os outros arquivos.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    target_path = out_dir / target_csv

    # Já existe? não refaz.
    if target_path.exists() and target_path.stat().st_size > 0:
        return target_path

    with zipfile.ZipFile(zip_path, "r") as z:
        members = z.namelist()
        match = next((m for m in members if m.endswith("/" + target_csv) or m == target_csv), None)
        if not match:
            raise FileNotFoundError(
                f"Não encontrei {target_csv} dentro do zip. "
                f"Exemplos de arquivos: {members[:20]}{'...' if len(members) > 20 else ''}"
            )

        with z.open(match) as src, target_path.open("wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)

    return target_path


def download_raw_dataset() -> DownloadResult:
    """
    Garante que pipeline/raw/steam-games-dataset/games_march2025_cleaned.csv exista.
    Se não tiver Kaggle creds, copia do sample.
    """
    root = data_layer_root()
    pipeline_dir = root / "pipeline"

    raw_dir = pipeline_dir / "raw" / DATASET_FOLDER_NAME
    raw_csv_path = raw_dir / TARGET_CSV

    sample_path = pipeline_dir / "source" / "sample" / TARGET_CSV

    # Se já existe na raw, não faz nada (idempotente)
    if raw_csv_path.exists() and raw_csv_path.stat().st_size > 0:
        return DownloadResult(raw_csv_path=raw_csv_path, used_fallback=False)

    if has_kaggle_credentials():
        zip_path = download_zip_with_kaggle(raw_dir)
        raw_csv_path = extract_only_target_csv(zip_path, raw_dir, TARGET_CSV)
        print(f"[downloader] Kaggle OK. RAW CSV: {raw_csv_path}")
        return DownloadResult(raw_csv_path=raw_csv_path, used_fallback=False)

    # Sem Kaggle -> sample
    if not sample_path.exists():
        raise FileNotFoundError(
            "Sem credenciais do Kaggle e não encontrei o sample.\n"
            f"Esperado em: {sample_path}\n"
            "Coloque um CSV sample aí para permitir rodar offline."
        )

    copy_file(sample_path, raw_csv_path)
    print(f"[downloader] Sem Kaggle creds. Usando SAMPLE: {raw_csv_path}")
    return DownloadResult(raw_csv_path=raw_csv_path, used_fallback=True)


def main() -> int:
    try:
        result = download_raw_dataset()
        print(f"[downloader] used_fallback={result.used_fallback}")
        return 0
    except Exception as e:
        print(f"[downloader] ERRO: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())