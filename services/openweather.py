# services/openweather.py
import os
import requests


class OpenWeatherError(RuntimeError):
    pass


def get_openweather_daily(lat: float, lon: float, *, units: str = "metric", lang: str = "hu") -> dict:
    """
    OpenWeather One Call 3.0 – napi (holnapi index = 1).
    Hozzuk: tmax, tmin, csapadék (rain+snow), szél (wind_speed), és ha van: alerts.
    Visszatérés:
      {
        "tmax": float, "tmin": float, "precip_mm": float, "wind_max": float,
        "alerts": [ {"event": str, "sender": str} , ... ]  # ha van
      }
    """
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise OpenWeatherError("OPENWEATHER_API_KEY nincs beállítva (.env)!")

    # alerts benne marad – csak minutely,hourly,current exclude
    url = (
        "https://api.openweathermap.org/data/3.0/onecall"
        f"?lat={lat}&lon={lon}"
        "&exclude=minutely,hourly,current"
        f"&units={units}&lang={lang}&appid={api_key}"
    )
    r = requests.get(url, timeout=25)
    if r.status_code == 401:
        raise OpenWeatherError("OpenWeather 401 – rossz/hiányzó API kulcs.")
    r.raise_for_status()
    js = r.json()

    d = js["daily"][1]
    precip = float(d.get("rain", 0.0)) + float(d.get("snow", 0.0))
    wind = float(d.get("wind_speed", 0.0))

    # alerts (ha jön)
    alerts_list = []
    for a in js.get("alerts", []) or []:
        alerts_list.append({
            "event": a.get("event") or "Riasztás",
            "sender": a.get("sender_name") or "",
        })

    return {
        "tmax": float(d["temp"]["max"]),
        "tmin": float(d["temp"]["min"]),
        "precip_mm": precip,
        "wind_max": wind,
        "alerts": alerts_list,
    }
