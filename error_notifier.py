# error_notifier.py
import os
import requests
import traceback
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# A Telegram bot adatai
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# Ha nincs külön megadva, az általad kért fix ID-t használjuk (-3104033408)
TELEGRAM_ALERT_CHAT_ID = os.getenv("TELEGRAM_ALERT_CHAT_ID", "-3104033408")

def notify_error(error: str | Exception, context: str | None = None) -> None:
    """
    Hibajelentés küldése Telegramra.
    Automatikusan formázza az üzenetet, és a stack trace-t is elküldi (ha van).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_ALERT_CHAT_ID:
        print("⚠️ Nincs Telegram token vagy chat ID, nem tudok hibát küldeni.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    # Stack trace hozzáadása (ha Exception objektum)
    if isinstance(error, Exception):
        trace = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        text = f"🟥 Hiba történt {context or ''}\n\n{error}\n\nTraceback:\n{trace}"
    else:
        text = f"🟥 Hiba: {error}\n\n{f'({context})' if context else ''}"

    # Biztonság: max 4000 karakter
    text = text[:4000]

    try:
        requests.post(url, data={
            "chat_id": TELEGRAM_ALERT_CHAT_ID,
            "text": text
        }, timeout=20)
        print(f"🚨 Hiba jelentve Telegramra ({datetime.now().strftime('%H:%M:%S')})")
    except Exception as e:
        print(f"⚠️ Nem sikerült elküldeni a hibát Telegramra: {e}")

def wrap_with_notify(func):
    """
    Dekorátor – automatikusan értesít Telegramon, ha a függvény kivételt dob.
    Példa:
        @wrap_with_notify
        def main():
            ...
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            notify_error(e, context=f"{func.__name__}()")
            raise
    return wrapper
