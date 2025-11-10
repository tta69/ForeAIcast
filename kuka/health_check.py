# health_check.py
import os
import requests
from dotenv import load_dotenv
from services.open_meteo import get_open_meteo_daily
from services.openweather import get_openweather_daily, OpenWeatherError
from telegram import Bot

load_dotenv()

print("== Rendszerellenőrzés – Milyenidoleszholnap.hu ==")

def check_api_open_meteo():
    print("🌤 Open-Meteo teszt: ", end="")
    try:
        data = get_open_meteo_daily(47.4979, 19.0402)  # Budapest
        assert "tmax" in data
        print("✅ rendben (adat érkezett)")
    except Exception as e:
        print(f"❌ hiba: {e}")

def check_api_openweather():
    print("☁️  OpenWeather teszt: ", end="")
    try:
        data = get_openweather_daily(47.4979, 19.0402)
        assert "tmax" in data
        print("✅ rendben (adat érkezett)")
    except OpenWeatherError as e:
        print(f"⚠️  figyelmeztetés: {e}")
    except Exception as e:
        print(f"❌ hiba: {e}")

def check_env():
    print("🔑 Környezeti változók: ", end="")
    missing = []
    for var in ["OPENWEATHER_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if not os.getenv(var):
            missing.append(var)
    if missing:
        print(f"⚠️  hiányzik: {', '.join(missing)}")
    else:
        print("✅ minden megvan")

def check_telegram():
    print("📨 Telegram teszt: ", end="")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("⚠️  nincs beállítva token vagy chat_id")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": "✅ Tesztüzenet: minden működik a milyenidoleszholnap rendszerben."},
            timeout=20,
        )
        if r.ok:
            print("✅ üzenet elküldve a Telegram csatornára")
        else:
            print(f"❌ hiba: {r.status_code} {r.text}")
    except Exception as e:
        print(f"❌ hiba: {e}")


def check_output_dir():
    print("📂 Cikkmentés teszt: ", end="")
    if os.path.isdir("out"):
        files = [f for f in os.listdir("out") if f.endswith(".md")]
        print(f"✅ {len(files)} cikk található az 'out' mappában")
    else:
        print("⚠️  nincs 'out' mappa – lehet, hogy még nem futott a build_articles.py")

# === Futtatás ===
check_env()
check_api_open_meteo()
check_api_openweather()
check_output_dir()
check_telegram()

print("\n✅ Ellenőrzés befejezve.")
