# services/open_meteo.py
import requests


def get_open_meteo_daily(lat: float, lon: float, *, lang: str = "hu") -> dict:
    """
    Open-Meteo napi előrejelzés (holnapi index = 1).
    Hozzuk: tmax, tmin, csapadék (összeg), szél (napi max 10 m-en).
    Visszatérés: {"tmax": float, "tmin": float, "precip_mm": float, "wind_max": float}
    """
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
        "&timezone=Europe/Budapest"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    daily = r.json()["daily"]

    return {
        "tmax": float(daily["temperature_2m_max"][1]),
        "tmin": float(daily["temperature_2m_min"][1]),
        "precip_mm": float(daily["precipitation_sum"][1]),
        "wind_max": float(daily["windspeed_10m_max"][1]),
    }
