import os
import sys
import json
import zipfile
from dataclasses import dataclass
from pathlib import Path

DATASET_SLUG = "artermiloff/steam-games-dataset"
DATASET_FOLDER_NAME = "steam-games-dataset"
TARGET_CSV = "games_march2025_cleaned.csv"

# Seu sample atual (nome do arquivo)
SAMPLE_FILE_NAME = "games_march2025_cleaned_sample.csv"


@dataclass(frozen=True)
class DownloadResult:
    raw_csv_path: Path
    used_fallback: bool
    source: str  # "kaggle" | "sample" | "raw_cached"


def data_layer_root() -> Path:
    """
    Estrutura esperada:
    Data-Layer/
      pipeline/
        source/
          downloader.py

    Então o root do Data-Layer é 2 níveis acima de pipeline/source.
    """
    return Path(__file__).resolve().parents[2]


def find_credentials_in_project() -> Path | None:
    """
    Procura credenciais dentro do Data-Layer.
    Prioridade:
    1) Data-Layer/.env
    2) qualquer .env abaixo
    3) qualquer kaggle.json abaixo
    """
    root = data_layer_root()

    dotenv_root = root / ".env"
    if dotenv_root.exists():
        return dotenv_root

    dotenv_any = next(root.rglob(".env"), None)
    if dotenv_any:
        return dotenv_any

    kaggle_json_any = next(root.rglob("kaggle.json"), None)
    if kaggle_json_any:
        return kaggle_json_any

    return None


def load_env_file(dotenv_path: Path) -> None:
    """
    Parser simples de .env:
    - ignora comentários e linhas vazias
    - suporta KEY=VALUE (com ou sem aspas)
    """
    for raw in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")

        # Só seta se não existir (não sobrescreve env do sistema)
        os.environ.setdefault(key, val)


def ensure_kaggle_env() -> None:
    """
    Garante que KAGGLE_USERNAME e KAGGLE_KEY estejam definidos:
    - Se já estiverem no ambiente, ok.
    - Senão, tenta carregar de .env ou kaggle.json dentro do Data-Layer.
    """
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return

    creds_path = find_credentials_in_project()
    if not creds_path:
        return

    if creds_path.name == ".env":
        load_env_file(creds_path)
        return

    if creds_path.name == "kaggle.json":
        creds = json.loads(creds_path.read_text(encoding="utf-8"))
        os.environ.setdefault("KAGGLE_USERNAME", creds.get("username", ""))
        os.environ.setdefault("KAGGLE_KEY", creds.get("key", ""))
        return


def has_kaggle_credentials() -> bool:
    """
    True se houver alguma chance realista de autenticar:
    - env vars
    - credenciais no projeto
    - ~/.kaggle/kaggle.json
    """
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return True

    if find_credentials_in_project():
        return True

    return (Path.home() / ".kaggle" / "kaggle.json").exists()


def authenticate_kaggle():
    """
    Autentica a Kaggle API.
    Observação: Import do KaggleApi fica dentro da função para evitar erro antes
    de injetarmos KAGGLE_USERNAME / KAGGLE_KEY.
    """
    ensure_kaggle_env()

    # Agora sim importa
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    return api


def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("rb") as r, dst.open("wb") as w:
        while True:
            chunk = r.read(1024 * 1024)
            if not chunk:
                break
            w.write(chunk)


def download_zip_with_kaggle(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    api = authenticate_kaggle()
    api.dataset_download_files(DATASET_SLUG, path=str(out_dir), quiet=False)

    zips = sorted(out_dir.glob("*.zip"))
    if not zips:
        raise FileNotFoundError(f"Download parece ter falhado: nenhum .zip em {out_dir}")
    return max(zips, key=lambda p: p.stat().st_size)


def extract_only_target_csv(zip_path: Path, out_dir: Path, target_csv: str) -> Path:
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


def get_sample_path(pipeline_dir: Path) -> Path | None:
    """
    Procura sample nos padrões mais prováveis do seu projeto.
    """
    candidates = [
        pipeline_dir / "sample" / SAMPLE_FILE_NAME,  # seu layout atual
        pipeline_dir / "sample" / TARGET_CSV,         # se você decidir usar o mesmo nome
        pipeline_dir / "source" / "sample" / SAMPLE_FILE_NAME,  # legado
        pipeline_dir / "source" / "sample" / TARGET_CSV,         # legado
    ]
    return next((p for p in candidates if p.exists()), None)


def download_raw_dataset() -> DownloadResult:
    root = data_layer_root()
    pipeline_dir = root / "pipeline"

    raw_dir = pipeline_dir / "raw" / DATASET_FOLDER_NAME
    raw_csv_path = raw_dir / TARGET_CSV

    # Se já existe na raw, não faz nada
    if raw_csv_path.exists() and raw_csv_path.stat().st_size > 0:
        return DownloadResult(raw_csv_path=raw_csv_path, used_fallback=False, source="raw_cached")

    # 1) Tenta Kaggle (se tiver qualquer indício de credenciais)
    if has_kaggle_credentials():
        try:
            zip_path = download_zip_with_kaggle(raw_dir)
            raw_csv_path = extract_only_target_csv(zip_path, raw_dir, TARGET_CSV)
            print(f"[downloader] Kaggle OK. RAW CSV: {raw_csv_path}")
            return DownloadResult(raw_csv_path=raw_csv_path, used_fallback=False, source="kaggle")
        except Exception as e:
            print(f"[downloader] Kaggle falhou ({e}). Caindo para sample...")

    # 2) Fallback: sample
    sample_path = get_sample_path(pipeline_dir)
    if not sample_path:
        raise FileNotFoundError(
            "Sem credenciais válidas do Kaggle e não encontrei o sample.\n"
            "Procurei em:\n"
            f"- {pipeline_dir / 'sample' / SAMPLE_FILE_NAME}\n"
            f"- {pipeline_dir / 'sample' / TARGET_CSV}\n"
            f"- {pipeline_dir / 'source' / 'sample' / SAMPLE_FILE_NAME}\n"
            f"- {pipeline_dir / 'source' / 'sample' / TARGET_CSV}\n"
            "\nCrie um sample em pipeline/sample/ para permitir rodar offline."
        )

    copy_file(sample_path, raw_csv_path)
    print(f"[downloader] Usando SAMPLE: {sample_path} -> {raw_csv_path}")
    return DownloadResult(raw_csv_path=raw_csv_path, used_fallback=True, source="sample")


def main() -> int:
    try:
        result = download_raw_dataset()
        print(f"[downloader] source={result.source} used_fallback={result.used_fallback}")
        return 0
    except Exception as e:
        print(f"[downloader] ERRO: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())