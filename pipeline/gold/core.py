from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import ast
import pandas as pd


@dataclass(frozen=True)
class GoldArtifacts:
    games_csv: Path
    categories_csv: Path
    game_categories_csv: Path


def _parse_categories(value: str) -> list[str]:
    """
    Suporta:
    - "['Action', 'RPG']"
    - "Action, RPG"
    - "" / NaN
    """
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            arr = ast.literal_eval(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass

    return [x.strip() for x in s.split(",") if x.strip()]


def run(input_csv: Path, output_dir: Path) -> GoldArtifacts:
    df = pd.read_csv(input_csv)
    output_dir.mkdir(parents=True, exist_ok=True)

    # GAMES: renomeia appid -> steam_appid
    # Observação: o id incremental (PK) é gerado no banco, não no CSV.
    games = (
        df.rename(columns={"appid": "steam_appid"})[
            [
                "steam_appid",
                "name",
                "release_date",
                "required_age",
                "price",
                "dlc_count",
                "detailed_description",
                "about_the_game",
                "short_description",
            ]
        ]
        .copy()
    )

    games_csv = output_dir / "games.csv"
    games.to_csv(games_csv, index=False)

    # CATEGORIES + BRIDGE (por steam_appid + category_name)
    pairs: list[tuple[int, str]] = []
    all_cats: set[str] = set()

    for _, row in df[["appid", "categories"]].iterrows():
        # garante int
        try:
            steam_appid = int(row["appid"])
        except Exception:
            continue

        for c in _parse_categories(row.get("categories", "")):
            all_cats.add(c)
            pairs.append((steam_appid, c))

    categories = pd.DataFrame({"name": sorted(all_cats)})
    categories_csv = output_dir / "categories.csv"
    categories.to_csv(categories_csv, index=False)

    game_categories = (
        pd.DataFrame(pairs, columns=["steam_appid", "category_name"])
        .drop_duplicates()
    )
    game_categories_csv = output_dir / "game_categories.csv"
    game_categories.to_csv(game_categories_csv, index=False)

    return GoldArtifacts(
        games_csv=games_csv,
        categories_csv=categories_csv,
        game_categories_csv=game_categories_csv,
    )