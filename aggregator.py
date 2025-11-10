# aggregator.py
from typing import Dict

def normalize_record(name: str, lat: float, lon: float, src: Dict):
    """Egységes rekord a kimenethez."""
    return {
        "city": name,
        "lat": lat,
        "lon": lon,
        "date": src["date"],
        "tmax_c": src["tmax"],
        "tmin_c": src["tmin"],
        "precip_mm": src["precip_mm"],
        "provider": src["provider"],
    }

def consensus(open_meteo: Dict, openweather: Dict):
    """
    Egyszerű konszenzus: átlagoljuk a hőmérsékleteket, csapadéknál max-ot veszünk.
    Visszaadjuk a különbségeket is ellenőrzéshez.
    """
    avg_tmax = round((open_meteo["tmax"] + openweather["tmax"]) / 2.0, 1)
    avg_tmin = round((open_meteo["tmin"] + openweather["tmin"]) / 2.0, 1)
    precip = round(max(open_meteo["precip_mm"], openweather["precip_mm"]), 1)

    deltas = {
        "tmax_delta": round(open_meteo["tmax"] - openweather["tmax"], 1),
        "tmin_delta": round(open_meteo["tmin"] - openweather["tmin"], 1),
        "precip_delta": round(open_meteo["precip_mm"] - openweather["precip_mm"], 1),
    }
    return {
        "tmax_c": avg_tmax,
        "tmin_c": avg_tmin,
        "precip_mm": precip,
        "deltas": deltas,
    }
