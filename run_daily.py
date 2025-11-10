# run_daily.py
import os, glob, time
from datetime import date, timedelta
from dotenv import load_dotenv
import requests

import build_articles  # a korábban létrehozott generátor

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
assert TOKEN and CHAT_ID, "Hiányzik TELEGRAM_BOT_TOKEN vagy TELEGRAM_CHAT_ID a .env-ben"

API_URL = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

def send_text(text: str, parse_mode: str | None = None):
    # Telegram üzenet limit ~4096 karakter → daraboljuk 3500-as blokkokra
    CHUNK = 3500
    parts = [text[i:i+CHUNK] for i in range(0, len(text), CHUNK)] or [text]
    for idx, p in enumerate(parts, 1):
        data = {"chat_id": CHAT_ID, "text": p}
        if parse_mode:
            data["parse_mode"] = parse_mode
        r = requests.post(API_URL, data=data, timeout=30)
        if not r.ok:
            raise RuntimeError(f"Telegram hiba: {r.status_code} {r.text}")
        # pici késleltetés flood elkerülésre
        time.sleep(0.4)

def main():
    target = (date.today() + timedelta(days=1)).isoformat()

    # 1) Cikkek legenerálása holnapra
    build_articles.build()  # az out/ mappába ír 19 db .md-t

    # 2) Országos nyitó-üzenet
    header = (
        f"**Milyen idő lesz holnap? – {target}**\n"
        f"Az alábbiakban megyénként küldjük a rövid előrejelzést. "
        f"Források: Open-Meteo + OpenWeather (konszenzus).\n"
    )
    send_text(header, parse_mode="Markdown")

    # 3) Megyénként küldés (slug alapján a holnapi fájlok)
    files = sorted(glob.glob(f"out/*-{target}.md"))
    if not files:
        send_text("⚠️ Nincs holnapi cikk az out/ mappában.")
        return

    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read().strip()
        # Markdown hossz csökkentése: cím marad félkövér, a többi sima
        # (ha marad a teljes tartalom, a chunkolás úgyis elvégzi a darabolást)
        send_text(txt, parse_mode=None)  # nyers szöveg, biztos kompatibilis
    send_text("✅ Kiküldés kész.")

if __name__ == "__main__":
    main()
