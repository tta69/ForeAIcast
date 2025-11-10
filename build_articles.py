# build_articles.py
import os
from datetime import date, timedelta
from dotenv import load_dotenv

from db_utils import get_cities_grouped_by_county, get_cities_by_regions
from services.open_meteo import get_open_meteo_daily
from services.openweather import get_openweather_daily
from aggregator import consensus
from writer import (
    make_slug, make_title, make_lead, make_article,
    make_national_slug, make_national_title, make_national_article,
)
from error_notifier import notify_error, wrap_with_notify

load_dotenv()
LANG  = os.getenv("DEFAULT_LANG", "hu")
UNITS = os.getenv("DEFAULT_UNITS", "metric")

OUTDIR = "out"
os.makedirs(OUTDIR, exist_ok=True)

def _safe_consensus(lat: float, lon: float) -> dict:
    """Open-Meteo + OpenWeather konszenzus, OW hiba esetén riaszt + OM fallback."""
    om = get_open_meteo_daily(lat, lon, lang=LANG)
    try:
        ow = get_openweather_daily(lat, lon, units=UNITS, lang=LANG)
        return consensus(om, ow)
    except Exception as e:
        notify_error(e, context=f"build_articles._safe_consensus lat={lat}, lon={lon}")
        # puha fallback: OM
        return {"tmax_c": om["tmax"], "tmin_c": om["tmin"], "precip_mm": om["precip_mm"]}

def _write(path: str, content: str):
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        notify_error(e, context=f"build_articles._write path={path}")
        raise

@wrap_with_notify
def build():
    target = date.today() + timedelta(days=1)
    print(f"== Cikkek generálása holnapra: {target.isoformat()} ==")

    # 1) Városok DB-ből (>=10k lakos; ÖSSZES város megyénként)
    cities_by_county = get_cities_grouped_by_county(limit_per_county=None, min_population=10000)
    # 2) Régiók
    regions = get_cities_by_regions(cities_by_county, per_county_cap=3)

    # ===== Országos blokk =====
    # Országos átlag a minden város konszenzusából (egyszerű átlag)
    country_rows = []
    for county, cities in cities_by_county.items():
        for c in cities:
            con = _safe_consensus(c["lat"], c["lon"])
            country_rows.append({"county": county, "city": c["city"], **con})

    if not country_rows:
        notify_error("Nincs országos aggregálható adat (country_rows üres).", context="build_articles.build")
        raise RuntimeError("No data to aggregate")

    avg_tmax = sum(r["tmax_c"] for r in country_rows) / len(country_rows)
    avg_tmin = sum(r["tmin_c"] for r in country_rows) / len(country_rows)
    max_pr   = max(r["precip_mm"] for r in country_rows)

    # régiók aggregálása
    region_rows = []
    for reg_name, reg_cities in regions.items():
        if not reg_cities:
            continue
        vals = []
        cities_preview = []
        for c in reg_cities:
            con = _safe_consensus(c["lat"], c["lon"])
            vals.append(con)
            cities_preview.append({
                "city": c["city"],
                "tmax": con["tmax_c"], "tmin": con["tmin_c"], "pr": con["precip_mm"]
            })
        rtmax = sum(v["tmax_c"] for v in vals) / len(vals)
        rtmin = sum(v["tmin_c"] for v in vals) / len(vals)
        rpr   = max(v["precip_mm"] for v in vals)
        region_rows.append((reg_name, {
            "tmax_c": rtmax, "tmin_c": rtmin, "precip_mm": rpr, "cities": cities_preview
        }))

    nat_slug  = make_national_slug(target)
    nat_title = make_national_title(target)
    nat_body  = make_national_article(
        target,
        {"tmax_c": avg_tmax, "tmin_c": avg_tmin, "precip_mm": max_pr},
        region_rows,
        alerts=None  # ha lesz riasztásforrás, itt add át
    )
    _write(os.path.join(OUTDIR, f"{nat_slug}.md"), nat_body)
    _write(os.path.join(OUTDIR, f"{nat_slug}.txt"), nat_title + "\n\n" + nat_body)

    # ===== Megyénként =====
    for megye, cities in cities_by_county.items():
        per_city_rows = []
        agg_tmax, agg_tmin, agg_pr = [], [], []

        for c in cities:
            try:
                om = get_open_meteo_daily(c["lat"], c["lon"], lang=LANG)
                try:
                    ow = get_openweather_daily(c["lat"], c["lon"], units=UNITS, lang=LANG)
                    con = consensus(om, ow)
                except Exception as e:
                    notify_error(e, context=f"OW hiba: {megye} / {c['city']}")
                    con = {"tmax_c": om["tmax"], "tmin_c": om["tmin"], "precip_mm": om["precip_mm"]}
            except Exception as e:
                notify_error(e, context=f"OM hiba: {megye} / {c['city']}")
                # ha OM is elbukik, eseti default (ne álljon le az egész megye)
                con = {"tmax_c": 0.0, "tmin_c": 0.0, "precip_mm": 0.0}

            per_city_rows.append({
                "city": c["city"],
                "cons_tmax": con["tmax_c"], "cons_tmin": con["tmin_c"], "cons_pr": con["precip_mm"],
            })
            agg_tmax.append(con["tmax_c"]); agg_tmin.append(con["tmin_c"]); agg_pr.append(con["precip_mm"])

        if not per_city_rows:
            notify_error(f"Nincs város a megyében: {megye}", context="build_articles.build")
            continue

        avg_tmax = sum(agg_tmax)/len(agg_tmax)
        avg_tmin = sum(agg_tmin)/len(agg_tmin)
        max_pr   = max(agg_pr)

        slug  = make_slug(megye, target)
        title = make_title(megye, target)
        lead  = make_lead(avg_tmax, avg_tmin, max_pr, [c["city"] for c in cities])
        body  = make_article(megye, per_city_rows, {"tmax_c": avg_tmax, "tmin_c": avg_tmin, "precip_mm": max_pr})

        md = f"# {title}\n\n**Líd:** {lead}\n\n{body}\n"
        _write(os.path.join(OUTDIR, f"{slug}.md"), md)

        # Telegram-barát sima TXT: cím + üzenet (a send_telegram most a .txt-ket küldi)
        txt = f"{title}\n\n{lead}\n\n{body}\n"
        _write(os.path.join(OUTDIR, f"{slug}.txt"), txt)

        print(f"✅ {megye}: out/{slug}.md + .txt")

if __name__ == "__main__":
    build()
