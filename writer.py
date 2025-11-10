# writer.py
from __future__ import annotations
from datetime import date
import html, unicodedata

HU_WEEKDAYS = ["Hétfő", "Kedd", "Szerda", "Csütörtök", "Péntek", "Szombat", "Vasárnap"]

def _weekday_hu(d: date) -> str:
    return HU_WEEKDAYS[d.weekday()]

def _dow_and_date(d: date) -> str:
    # pl.: "hétfő, 2025-11-03"
    return f"{_weekday_hu(d).lower()}, {d.isoformat()}"

def _deg(x: float) -> str:
    return f"{round(float(x), 1):.1f} °C"

def _mm(x: float) -> str:
    v = float(x)
    return "0 mm" if v < 0.05 else f"{round(v, 1):.1f} mm"

def _emoji_rain(mm: float) -> str:
    if mm >= 10: return "🌧️"
    if mm >= 1:  return "🌦️"
    return "☀️"

def _safe(s: str) -> str:
    return html.escape(s, quote=False)

def _slugify(text: str) -> str:
    s = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    s = s.lower().replace(" ", "-")
    while "--" in s:
        s = s.replace("--", "-")
    return s

# ----- közös meta -----

def make_slug(megye: str, target: date) -> str:
    return f"milyen_idolesz_holnap-{_slugify(megye)}ben-{target.isoformat()}"

def make_title(megye: str, target: date) -> str:
    return f"Milyen idő lesz holnap {megye} vármegyében? – {_weekday_hu(target).lower()}, {target.isoformat()}"

def make_lead(avg_tmax: float, avg_tmin: float, max_pr: float, city_names: list[str]) -> str:
    em = _emoji_rain(float(max_pr))
    return (
        f"{em} Napközben a maximum elérheti a {_deg(avg_tmax)} értéket, hajnalban {_deg(avg_tmin)} várható. "
        f"Csapadék összességében {_mm(max_pr)} körül valószínű a modellek szerint."
    )

# ----- országos blokk -----

def make_national_slug(target: date) -> str:
    return f"000_orszagos-elorejelzes-{target.isoformat()}"

def make_national_title(target: date) -> str:
    return f"🌦️ Országos előrejelzés – {_weekday_hu(target).lower()}, {target.isoformat()}"

def _maybe_wind_line(max_wind_kmh: float | None) -> str:
    if max_wind_kmh is None:
        return ""
    v = round(float(max_wind_kmh))
    if v < 35:
        return "- Szél: jelentős szél nem várható\n"
    return f"- Szél: erősödő széllökések, max ~{v} km/h\n"

def _alerts_block(alerts: list[str] | None) -> str:
    if alerts and any(a.strip() for a in alerts):
        uniq = sorted({a.strip() for a in alerts if a.strip()})
        return "🆘 Riasztások:\n" + "\n".join(f"- {a}" for a in uniq) + "\n\n"
    return "🆘 Jelenleg nincs érvényben riasztás a holnapi napra.\n\n"

def make_national_article(target: date,
                          country_daily: dict,
                          regions_rows: list[tuple[str, dict]],
                          alerts: list[str] | None) -> str:
    tmax = country_daily["tmax_c"]; tmin = country_daily["tmin_c"]; pr = country_daily["precip_mm"]
    wind = country_daily.get("wind_kmh")
    title = make_national_title(target)
    lead  = f"{_emoji_rain(pr)} Napközben országosan átlagosan {_deg(tmax)}, hajnalban {_deg(tmin)}. A csapadék összességében {_mm(pr)} körül alakulhat."

    summary = (
        "Országos összefoglaló:\n\n"
        f"- Átlagos csúcs: {_deg(tmax)}\n"
        f"- Átlagos minimum: {_deg(tmin)}\n"
        f"- Csapadék (maximum): {_mm(pr)}\n"
        f"{_maybe_wind_line(wind)}"
    ).rstrip() + "\n\n"

    reg_blocks = []
    for reg_name, reg in regions_rows:
        rtmax = reg["tmax_c"]; rtmin = reg["tmin_c"]; rpr = reg["precip_mm"]; rwind = reg.get("wind_kmh")
        head = f"**{reg_name}** — csúcs: {_deg(rtmax)}, min: {_deg(rtmin)}, csapadék (max): {_mm(rpr)}"
        mw = _maybe_wind_line(rwind).strip()
        if mw:
            head += f"  |  {mw}"
        city_lines = []
        for c in reg.get("cities", [])[:8]:
            city_lines.append(f"- {c['city']}: {_deg(c['tmax'])}/{_deg(c['tmin'])}, eső {_mm(c['pr'])}")
        reg_blocks.append(head + "\n" + ("\n".join(city_lines) if city_lines else "") + "\n")

    content = f"# {title}\n\n**Líd:** {lead}\n\n" + summary + _alerts_block(alerts) + "\n".join(reg_blocks) + \
              "\nForrások: Open-Meteo, OpenWeather (One Call 3.0)\n"
    return content

def make_telegram_national(target: date, country_daily: dict, regions_rows: list[tuple[str, dict]], alerts: list[str] | None) -> str:
    tmax = country_daily["tmax_c"]; tmin = country_daily["tmin_c"]; pr = country_daily["precip_mm"]
    wind = country_daily.get("wind_kmh")
    header = f"🌦️ Országos előrejelzés – {_dow_and_date(target)}\n"
    summary = (
        f"• Átlag csúcs: {_deg(tmax)} | min: {_deg(tmin)}\n"
        f"• Csapadék (max): {_mm(pr)}\n"
    )
    if wind is not None:
        summary += ("• Szél: jelentős nem várható\n" if float(wind) < 35 else f"• Szél: max ~{round(float(wind))} km/h\n")

    reg_lines = []
    for reg_name, reg in regions_rows:
        reg_lines.append(f"— {reg_name}: {_deg(reg['tmax_c'])}/{_deg(reg['tmin_c'])}, {_mm(reg['precip_mm'])}")

    alert_line = "🆘 Nincs érvényes riasztás." if not (alerts and any(a.strip() for a in alerts)) else "🆘 Van érvényben riasztás (részletek a weben)."
    msg = f"{header}{summary}\n" + "\n".join(reg_lines[:8]) + f"\n\n{alert_line}\nForrás: Open-Meteo, OpenWeather"
    return msg[:3800]

# ----- megyei (Telegram + MD) -----

def make_telegram(megye: str,
                  per_city_rows: list[dict],
                  daily: dict,
                  alerts: list[str] | None = None,
                  *,
                  target: date | None = None) -> str:
    """
    Ha 'target' meg van adva, a fejlécben megjelenik a nap neve + dátum is.
    """
    tmax = float(daily["tmax_c"])
    tmin = float(daily["tmin_c"])
    pr   = float(daily["precip_mm"])
    wind = daily.get("wind_kmh")  # opcionális

    header = f"🌦️ Milyen idő lesz holnap {megye} vármegyében?"
    if target:
        header += f" – {_dow_and_date(target)}"
    header += "\n"

    summary = f"• Átlag csúcs: {_deg(tmax)}  |  min: {_deg(tmin)}\n• Csapadék (max): {_mm(pr)}\n"
    if wind is not None:
        summary += "• Szél: jelentős nem várható\n" if float(wind) < 35 else f"• Szél: max ~{round(float(wind))} km/h\n"

    city_lines = []
    for r in per_city_rows:
        # _deg() már tartalmazza a "°C"-t, ezért NEM teszünk mögé még egyet
        city_lines.append(f"🏙️ {r['city']}: {_deg(r['cons_tmax'])}/{_deg(r['cons_tmin'])}, eső {_mm(r['cons_pr'])}")

    alert_line = "🆘 Nincs holnapi riasztás." if not (alerts and any(a.strip() for a in alerts)) else "🆘 Van érvényben riasztás."
    footer = "Források: Open-Meteo, OpenWeather (One Call 3.0)"

    msg = f"{header}{summary}\n" + "\n".join(city_lines) + f"\n\n{alert_line}\n{footer}"
    return msg[:3800]

def make_article(megye: str, per_city_rows: list[dict], daily: dict, alerts: list[str] | None = None) -> str:
    tmax = daily["tmax_c"]; tmin = daily["tmin_c"]; pr = daily["precip_mm"]
    wind = daily.get("wind_kmh")
    parts = []
    parts.append("Megyei összefoglaló:\n\n")
    parts.append(f"- Átlagos csúcs: {_deg(tmax)}\n")
    parts.append(f"- Átlagos minimum: {_deg(tmin)}\n")
    parts.append(f"- Csapadék (maximum): {_mm(pr)}\n")
    wl = _maybe_wind_line(wind).strip()
    if wl:
        parts.append(f"- {wl}\n")
    parts.append("\n")
    parts.append("🆘 " + ("Jelenleg nincs érvényben riasztás a holnapi napra.\n\n" if not (alerts and any(a.strip() for a in alerts)) else "Van érvényben riasztás.\n\n"))
    parts.append(f"{megye} kiemelt települései holnapi várható időjárása:\n\n")
    for r in per_city_rows:
        parts.append(f"- {r['city']}: maximum/minimum {_deg(r['cons_tmax'])} / {_deg(r['cons_tmin'])}, eső {_mm(r['cons_pr'])}\n")
    parts.append("\nForrások: Open-Meteo, OpenWeather (One Call 3.0)\n")
    return "".join(parts)
