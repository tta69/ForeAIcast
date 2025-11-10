# main.py
import os
from dotenv import load_dotenv

from typing import Optional
from fastapi import FastAPI, Query, HTTPException
import psycopg2
import psycopg2.extras
import argparse
import uvicorn

from cities import CITIES
from services.open_meteo import get_open_meteo_daily
from services.openweather import get_openweather_daily, OpenWeatherError
from aggregator import consensus

# ==== ENV ====
load_dotenv()
LANG = os.getenv("DEFAULT_LANG", "hu")
UNITS = os.getenv("DEFAULT_UNITS", "metric")
DATABASE_URL = os.getenv("DATABASE_URL")  # pl. postgresql+psycopg2://user:pw@localhost:5432/ForeAIcast


# ==== HELPERS ====
def fmt_num(x, unit=""):
    try:
        if isinstance(x, (int, float)):
            return f"{x:.1f}{unit}" if unit else f"{x:.1f}"
        return str(x)
    except Exception:
        return str(x)


def get_conn():
    """
    psycopg2 kapcsolat. A DATABASE_URL-ben lévő '+psycopg2' részt levágjuk,
    hogy a natív psycopg2-nek jó legyen.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set in .env")
    dsn = DATABASE_URL.replace("+psycopg2", "")
    return psycopg2.connect(dsn)


def fetch_city_by_slug(slug: str, iso2: str = "HU"):
    """Visszaadja a város metaadatát DB-ből slug alapján (lat/lon, név, megye)."""
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ci.id, ci.name_hu, ci.slug, ci.lat, ci.lon,
                   ci.is_capital, ci.is_county_seat,
                   co.name_hu AS county_name
            FROM cities ci
            JOIN countries c  ON c.id = ci.country_id
            JOIN counties  co ON co.id = ci.county_id
            WHERE c.iso2 = %s AND ci.slug = %s
            LIMIT 1;
            """,
            (iso2.upper(), slug),
        )
        row = cur.fetchone()
        return row


# ==== CLI FUTTATÁS (ahogy eddig) ====
def run_cli():
    print("== Holnapi előrejelzés – kettős forrás és konszenzus ==\n")

    for c in CITIES:
        name, lat, lon = c["name"], c["lat"], c["lon"]

        # 1) Open-Meteo – ez legyen az alap (stabil és ingyenes)
        try:
            om = get_open_meteo_daily(lat, lon, lang=LANG)
        except Exception as e:
            print(f"❌ {name}: Open-Meteo hiba: {e}\n")
            continue

        # 2) OpenWeather – opcionális
        ow = None
        try:
            ow = get_openweather_daily(lat, lon, units=UNITS, lang=LANG)
        except OpenWeatherError as e:
            print(f"⚠️  {name}: OpenWeather kihagyva: {e}")
        except Exception as e:
            print(f"⚠️  {name}: OpenWeather váratlan hiba: {e}")

        # 3) Kiírás
        date_str = om["date"]

        if ow:
            con = consensus(om, ow)
            print(f"🌍 {name} — {date_str}")
            print(
                "  Open-Meteo:  "
                f"Tmax {fmt_num(om['tmax'], '°C')}, "
                f"Tmin {fmt_num(om['tmin'], '°C')}, "
                f"Csapadék {fmt_num(om['precip_mm'], ' mm')}"
            )
            print(
                "  OpenWeather: "
                f"Tmax {fmt_num(ow['tmax'], '°C')}, "
                f"Tmin {fmt_num(ow['tmin'], '°C')}, "
                f"Csapadék {fmt_num(ow['precip_mm'], ' mm')}"
            )
            print(
                "  ➜ Konszenzus: "
                f"Tmax {fmt_num(con['tmax_c'], '°C')}, "
                f"Tmin {fmt_num(con['tmin_c'], '°C')}, "
                f"Csapadék {fmt_num(con['precip_mm'], ' mm')} "
                f"(ΔTmax {fmt_num(con['deltas']['tmax_delta'])}, "
                f"ΔTmin {fmt_num(con['deltas']['tmin_delta'])})\n"
            )
        else:
            print(f"🌍 {name} — {date_str}")
            print(
                "  Open-Meteo (fallback): "
                f"Tmax {fmt_num(om['tmax'], '°C')}, "
                f"Tmin {fmt_num(om['tmin'], '°C')}, "
                f"Csapadék {fmt_num(om['precip_mm'], ' mm')}\n"
            )


# ==== FASTAPI APP ====
app = FastAPI(title="ForeAIcast API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/countries/{iso2}/counties")
def list_counties(iso2: str):
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT co.id, co.name_hu, co.slug
            FROM counties co
            JOIN countries c ON c.id = co.country_id
            WHERE c.iso2 = %s
            ORDER BY co.name_hu;
            """,
            (iso2.upper(),),
        )
        return {"items": cur.fetchall()}


@app.get("/countries/{iso2}/search")
def search_city(iso2: str, q: str = Query(..., min_length=1)):
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, name_hu, slug, county_name, lat, lon, is_capital, is_county_seat
            FROM search_city
            WHERE country_id = (SELECT id FROM countries WHERE iso2=%s)
              AND q ILIKE unaccent(lower('%%'||%s||'%%'))
            ORDER BY is_capital DESC, is_county_seat DESC, rank ASC
            LIMIT 20;
            """,
            (iso2.upper(), q),
        )
        return {"items": cur.fetchall()}


@app.get("/countries/{iso2}/cities")
def list_cities_in_county(
    iso2: str,
    county: str = Query(..., description="Megye slug (pl. 'pest', 'csongrad-csanad')"),
    limit: int = Query(200, ge=1, le=1000),
):
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ci.id, ci.name_hu, ci.slug, ci.lat, ci.lon, ci.is_capital, ci.is_county_seat
            FROM cities ci
            JOIN counties co  ON co.id = ci.county_id
            JOIN countries c  ON c.id = ci.country_id
            WHERE c.iso2 = %s AND co.slug = %s
            ORDER BY ci.is_county_seat DESC, ci.rank ASC, ci.name_hu
            LIMIT %s;
            """,
            (iso2.upper(), county, limit),
        )
        return {"items": cur.fetchall()}


@app.get("/forecast/by-coords")
def forecast_by_coords(
    lat: float,
    lon: float,
    lang: Optional[str] = None,
    units: Optional[str] = None,
):
    lang = (lang or LANG)
    units = (units or UNITS)

    # Open-Meteo
    try:
        om = get_open_meteo_daily(lat, lon, lang=lang)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo error: {e}")

    # OpenWeather (opcionális)
    ow = None
    try:
        ow = get_openweather_daily(lat, lon, units=units, lang=lang)
    except OpenWeatherError as e:
        ow = None  # kulcs hiány vagy kvóta: nem fatal
    except Exception as e:
        ow = None

    if ow:
        con = consensus(om, ow)
        return {
            "source": {"open_meteo": om, "openweather": ow},
            "consensus": con,
            "date": om.get("date"),
            "coords": {"lat": lat, "lon": lon},
        }
    else:
        return {
            "source": {"open_meteo": om},
            "consensus": None,
            "date": om.get("date"),
            "coords": {"lat": lat, "lon": lon},
            "note": "OpenWeather not used",
        }


@app.get("/forecast/by-slug/{slug}")
def forecast_by_slug(slug: str, iso2: str = "HU", lang: Optional[str] = None, units: Optional[str] = None):
    city = fetch_city_by_slug(slug=slug, iso2=iso2)
    if not city:
        raise HTTPException(status_code=404, detail="City not found")

    lat, lon = float(city["lat"]), float(city["lon"])
    lang = (lang or LANG)
    units = (units or UNITS)

    # Open-Meteo
    try:
        om = get_open_meteo_daily(lat, lon, lang=lang)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo error: {e}")

    # OpenWeather (opcionális)
    ow = None
    try:
        ow = get_openweather_daily(lat, lon, units=units, lang=lang)
    except OpenWeatherError:
        ow = None
    except Exception:
        ow = None

    if ow:
        con = consensus(om, ow)
        return {
            "city": {"name": city["name_hu"], "slug": city["slug"], "county": city["county_name"]},
            "source": {"open_meteo": om, "openweather": ow},
            "consensus": con,
            "date": om.get("date"),
            "coords": {"lat": lat, "lon": lon},
        }
    else:
        return {
            "city": {"name": city["name_hu"], "slug": city["slug"], "county": city["county_name"]},
            "source": {"open_meteo": om},
            "consensus": None,
            "date": om.get("date"),
            "coords": {"lat": lat, "lon": lon},
            "note": "OpenWeather not used",
        }


# ==== ENTRYPOINT ====
def main():
    parser = argparse.ArgumentParser(description="ForeAIcast – CLI vagy API mód")
    parser.add_argument("--api", action="store_true", help="API mód indítása (FastAPI + Uvicorn)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.api:
        uvicorn.run("main:app", host=args.host, port=args.port, reload=True)
    else:
        run_cli()


if __name__ == "__main__":
    main()
