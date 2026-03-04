from __future__ import annotations
import os
import time
import subprocess
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

def data_layer_root() -> Path:
    return Path(__file__).resolve().parents[1]  # infra/.. = Data-Layer

def _load_env_if_needed():
    # carrega .env simples (pra não depender de python-dotenv)
    env_path = data_layer_root() / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def docker_up():
    root = data_layer_root()
    infra = root / "infra"
    subprocess.run(["docker", "compose", "-f", str(infra / "docker-compose.yml"), "up", "-d"], check=True)

def wait_db(engine, timeout_sec: int = 60):
    start = time.time()
    while True:
        try:
            with engine.connect() as c:
                c.execute(text("SELECT 1"))
            return
        except Exception:
            if time.time() - start > timeout_sec:
                raise
            time.sleep(2)

def load_gold_to_postgres(gold_dir: Path):
    _load_env_if_needed()

    db = os.getenv("POSTGRES_DB", "graphgames")
    user = os.getenv("POSTGRES_USER", "graphgames")
    pw = os.getenv("POSTGRES_PASSWORD", "graphgames")
    port = os.getenv("POSTGRES_PORT", "5432")

    url = f"postgresql+psycopg2://{user}:{pw}@localhost:{port}/{db}"
    engine = create_engine(url)

    wait_db(engine)

    games_csv = gold_dir / "games.csv"
    categories_csv = gold_dir / "categories.csv"
    game_categories_csv = gold_dir / "game_categories.csv"

    if not games_csv.exists():
        raise FileNotFoundError(f"Gold games.csv não encontrado: {games_csv}")

    games = pd.read_csv(games_csv)
    categories = pd.read_csv(categories_csv) if categories_csv.exists() else pd.DataFrame({"name": []})
    bridge = pd.read_csv(game_categories_csv) if game_categories_csv.exists() else pd.DataFrame(columns=["steam_appid","category_name"])

    # valida schema mínimo esperado
    if "steam_appid" not in games.columns:
        raise ValueError("games.csv precisa ter coluna 'steam_appid' (gerada na GOLD).")

    with engine.begin() as conn:
        # limpa (FK)
        conn.execute(text("TRUNCATE TABLE game_categories RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE categories RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE games RESTART IDENTITY CASCADE"))

        # 1) insere games (id é gerado no banco)
        # (não envie coluna 'id' no CSV)
        games.to_sql("games", conn, if_exists="append", index=False, method="multi", chunksize=1000)

        # 2) insere categories
        if not categories.empty:
            categories[["name"]].to_sql("categories", conn, if_exists="append", index=False, method="multi", chunksize=1000)

        # 3) monta mapeamentos reais do banco
        games_map = pd.read_sql("SELECT id, steam_appid FROM games", conn)
        cats_map = pd.read_sql("SELECT id, name FROM categories", conn)

        steam_to_game_id = dict(zip(games_map["steam_appid"], games_map["id"]))
        catname_to_id = dict(zip(cats_map["name"], cats_map["id"]))

        # 4) resolve ponte steam_appid + category_name -> game_id + category_id
        if not bridge.empty:
            # normaliza
            bridge["steam_appid"] = pd.to_numeric(bridge["steam_appid"], errors="coerce")
            bridge["category_name"] = bridge["category_name"].astype("string").fillna("").str.strip()

            bridge = bridge.dropna(subset=["steam_appid"])
            bridge["steam_appid"] = bridge["steam_appid"].astype("int64")

            bridge["game_id"] = bridge["steam_appid"].map(steam_to_game_id)
            bridge["category_id"] = bridge["category_name"].map(catname_to_id)

            fixed = bridge.dropna(subset=["game_id", "category_id"]).copy()
            fixed["game_id"] = fixed["game_id"].astype("int64")
            fixed["category_id"] = fixed["category_id"].astype("int64")
            fixed = fixed[["game_id", "category_id"]].drop_duplicates()

            if not fixed.empty:
                fixed.to_sql("game_categories", conn, if_exists="append", index=False, method="multi", chunksize=5000)
                
def main():
    root = data_layer_root()
    gold_dir = root / "pipeline" / "gold" / "artifacts"
    docker_up()
    load_gold_to_postgres(gold_dir)
    print("[bootstrap] Postgres up + dados carregados com sucesso.")

if __name__ == "__main__":
    main()