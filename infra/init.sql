CREATE TABLE IF NOT EXISTS games (
  id BIGSERIAL PRIMARY KEY,
  steam_appid BIGINT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  release_date DATE NULL,
  required_age INT NOT NULL DEFAULT 0,
  price NUMERIC(10,2) NOT NULL DEFAULT 0,
  dlc_count INT NOT NULL DEFAULT 0,
  detailed_description TEXT NULL,
  about_the_game TEXT NULL,
  short_description TEXT NULL
);

CREATE TABLE IF NOT EXISTS categories (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS game_categories (
  game_id BIGINT NOT NULL REFERENCES games(id) ON DELETE CASCADE,
  category_id BIGINT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
  PRIMARY KEY (game_id, category_id)
);