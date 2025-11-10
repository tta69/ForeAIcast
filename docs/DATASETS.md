# Datasets (local, not versioned)

Large raw datasets are kept **outside** the repo to avoid GitHub’s 100MB limit.

## WorldCities
- Local path on server: `/srv/data/worldcities.csv` and `/srv/data/worldcities.sql`
- Not tracked in git (see `.gitignore`).

## Import
Use the helper script to load cities into Postgres:

```bash
./scripts/import_worldcities.sh \
  --db "postgresql://USER:PASS@localhost:5432/ForeAIcast" \
  --csv "/srv/data/worldcities.csv"


### c) `scripts/import_worldcities.sh`
```bash
cat > scripts/import_worldcities.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

DB_URL=""
CSV_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db) DB_URL="$2"; shift 2;;
    --csv) CSV_PATH="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

if [[ -z "${DB_URL}" || -z "${CSV_PATH}" ]]; then
  echo "Usage: $0 --db postgresql://USER:PASS@HOST:PORT/DB --csv /path/worldcities.csv"
  exit 1
fi

echo "[*] Creating staging table if not exists..."
psql "${DB_URL}" -v ON_ERROR_STOP=1 <<'SQL'
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.worldcities_raw(
  city                 text,
  city_ascii           text,
  lat                  double precision,
  lng                  double precision,
  country              text,
  iso2                 text,
  iso3                 text,
  admin_name           text,
  capital              text,
  population           numeric,
  id                   bigint,
  population_proper    numeric,
  density              numeric,
  ranking              integer
);
TRUNCATE staging.worldcities_raw;
SQL

echo "[*] Importing CSV..."
psql "${DB_URL}" -v ON_ERROR_STOP=1 -c "\copy staging.worldcities_raw FROM '${CSV_PATH}' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8', QUOTE '\"', NULL '', FORCE_NULL(lat,lng,density,population,population_proper,ranking))"

echo "[*] Basic sanity checks..."
psql "${DB_URL}" -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) AS rows_in_staging FROM staging.worldcities_raw;"
psql "${DB_URL}" -v ON_ERROR_STOP=1 -c "SELECT country, COUNT(*) AS c FROM staging.worldcities_raw GROUP BY 1 ORDER BY c DESC LIMIT 10;"

echo "[✓] Done."
