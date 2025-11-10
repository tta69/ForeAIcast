# db_utils.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional
from error_notifier import notify_error

# ---- DSN csak ENV-ből (DATABASE_URL). Ha hiányzik: Telegram + kivétel. ----
def _dsn_from_env() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        notify_error("DATABASE_URL hiányzik az ENV-ben.", context="db_utils._dsn_from_env")
        raise RuntimeError("DATABASE_URL environment variable is required")
    return url

def _fetchall(sql: str, params: Optional[dict] = None):
    dsn = _dsn_from_env()
    try:
        with psycopg2.connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()
    except Exception as e:
        notify_error(e, context="db_utils._fetchall")
        raise

# ---- Városok megyék szerint (opció: min. népesség, limit per megye) ----
def get_cities_grouped_by_county(
    limit_per_county: int | None = None,
    min_population: int | None = None
) -> dict[str, list[dict]]:
    """
    Visszatérés: { 'Baranya': [ {city, lat, lon, slug, is_county_seat, population}, ... ], ... }
    - limit_per_county=None => összes város
    - min_population => csak a megadott lakosságszám fölöttiek
    """
    sql = """
    WITH hu AS (SELECT id FROM public.countries WHERE iso2='HU')
    SELECT
      co.name_hu                          AS county_name,
      ci.name_hu                          AS city,
      ci.lat, ci.lon, ci.slug,
      ci.is_county_seat::bool             AS is_county_seat,
      NULLIF(ci.population, 0)            AS population
    FROM public.cities ci
    JOIN public.counties co ON co.id = ci.county_id
    WHERE ci.country_id = (SELECT id FROM hu)
    {pop_filter}
    ORDER BY county_name, (NOT ci.is_county_seat), ci.name_hu;
    """
    pop_filter = ""
    params = {}
    if min_population is not None:
        pop_filter = "AND COALESCE(ci.population, 0) >= %(minpop)s"
        params["minpop"] = int(min_population)

    rows = _fetchall(sql.format(pop_filter=pop_filter), params)
    grouped: dict[str, list[dict]] = {}
    for r in rows:
        grouped.setdefault(r["county_name"], []).append({
            "city": r["city"],
            "lat": float(r["lat"]), "lon": float(r["lon"]),
            "slug": r["slug"],
            "is_county_seat": bool(r["is_county_seat"]),
            "population": r.get("population"),
        })
    if limit_per_county is not None and limit_per_county > 0:
        for k in list(grouped.keys()):
            grouped[k] = grouped[k][:limit_per_county]
    return grouped

# ---- Régiók és kiválogatásuk (marad a korábbi logika) ----
REGIONS = {
    "Budapest és agglomeráció": ["Budapest", "Pest"],
    "Balaton": ["Veszprém", "Somogy", "Zala"],
    "Alföld": ["Bács-Kiskun", "Békés", "Csongrád-Csanád", "Hajdú-Bihar", "Jász-Nagykun-Szolnok", "Szabolcs-Szatmár-Bereg"],
    "Északi-középhegység": ["Borsod-Abaúj-Zemplén", "Heves", "Nógrád"],
    "Mecsek": ["Baranya"],
}

def get_cities_by_regions(all_by_county: dict[str, list[dict]], per_county_cap: int = 3) -> dict[str, list[dict]]:
    """
    Régiónként összegyűjti a városokat a megadott megyékből.
    per_county_cap: megyénként legfeljebb ennyi várost vesz figyelembe a régió-aggregálásban.
    """
    try:
        result: dict[str, list[dict]] = {}
        for region_name, counties in REGIONS.items():
            coll: list[dict] = []
            for co in counties:
                cities = all_by_county.get(co, [])
                seats = [c for c in cities if c.get("is_county_seat")]
                rest  = [c for c in cities if not c.get("is_county_seat")]
                picked = (seats + rest)[:per_county_cap]
                coll.extend(picked)

            # deduplikálás név alapján
            seen = set()
            uniq = []
            for x in coll:
                if x["city"] in seen:
                    continue
                seen.add(x["city"])
                uniq.append(x)
            result[region_name] = uniq
        return result
    except Exception as e:
        notify_error(e, context="db_utils.get_cities_by_regions")
        raise
