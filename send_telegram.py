# send_telegram.py
import os, glob, re, argparse, asyncio
from datetime import datetime
from typing import List

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Bot
from telegram.error import RetryAfter, TimedOut, NetworkError, Forbidden

# --- ENV ----------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
assert DATABASE_URL, "Hiányzik a DATABASE_URL!"
assert TOKEN, "Hiányzik a TELEGRAM_BOT_TOKEN!"

# --- DB segédek ----------------------------------------------------------
def db_fetchall(sql: str, params=None):
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()

def get_active_recipients(test_chat: int | None = None) -> List[int]:
    if test_chat:
        return [int(test_chat)]
    sql = """
    SELECT chat_id
    FROM public.telegram_users
    WHERE paused_until IS NULL OR paused_until < NOW();
    """
    rows = db_fetchall(sql)
    return [int(r["chat_id"]) for r in rows]

# --- Fájlgyűjtés: csak a legfrissebb dátum ------------------------------
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

def _extract_date(path: str) -> datetime | None:
    m = DATE_RE.search(os.path.basename(path))
    if not m: 
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d")
    except ValueError:
        return None

def pick_latest_date() -> str | None:
    candidates = glob.glob("out/*.txt")
    dates = [_extract_date(p) for p in candidates]
    dates = [d for d in dates if d is not None]
    if not dates:
        return None
    latest = max(dates)
    return latest.strftime("%Y-%m-%d")

def build_file_list(only: str | None) -> List[str]:
    latest = pick_latest_date()
    if not latest:
        return []
    files = []

    # Országos (ha létezik az adott napra)
    nat = sorted(glob.glob(f"out/000_orszagos-*-{latest}.txt"))
    if nat:
        files.extend(nat)

    # Megyék adott napra
    county_files = sorted(glob.glob(f"out/*-{latest}.txt"))
    # szűrés: zárjuk ki az országos fájl(oka)t, amit már hozzáadtunk
    county_files = [p for p in county_files if not os.path.basename(p).startswith("000_orszagos-")]

    if only:
        wants = {w.strip().lower() for w in only.split(",")}
        def _match(p: str) -> bool:
            base = os.path.basename(p).lower()
            return any(w in base for w in wants)
        county_files = [p for p in county_files if _match(p)]

        # „Országos” kulcsszó: ha kéri, de még nem adtuk hozzá nat-ot (pl. nincs az adott napra),
        # akkor ne tegyünk semmit; ha van, már benn van a lista elején.
        # (Semmi extra teendő.)
    files.extend(county_files)
    return files

# --- Küldés --------------------------------------------------------------
async def send_text(bot: Bot, chat_id: int, text: str):
    # 4096 Telegram limit – hagyjunk pár karakter tartalékot
    chunk = text[:4090]
    while True:
        try:
            await bot.send_message(chat_id=chat_id, text=chunk)
            return
        except RetryAfter as e:
            wait = int(getattr(e, "retry_after", 40))
            print(f"⏳ Flood control – várok {wait} mp-et… (chat={chat_id})")
            await asyncio.sleep(wait)
        except TimedOut:
            print(f"⚠️ Timed out – újrapróbálom 5 mp múlva (chat={chat_id})")
            await asyncio.sleep(5)
        except (NetworkError,) as e:
            print(f"⚠️ Hálózati hiba: {e} – újrapróbálom 5 mp múlva (chat={chat_id})")
            await asyncio.sleep(5)

async def run_async(only: str | None, test_chat: int | None):
    bot = Bot(TOKEN)
    recipients = get_active_recipients(test_chat=test_chat)
    if not recipients:
        print("ℹ️ Nincs aktív címzett (paused_until lehet beállítva mindenkinek).")
        return

    files = build_file_list(only=only)
    if not files:
        print("ℹ️ Nincs küldhető .txt az out/ mappában (ellenőrizd a buildet és a fájldátumokat).")
        return

    sent_count = 0
    for chat_id in recipients:
        # országos fájlok menjenek előre: már így építettük a listát
        for path in files:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    text = f.read()
                await send_text(bot, chat_id, text)
                tag = "Országos elküldve" if os.path.basename(path).startswith("000_orszagos-") \
                      else "Megye elküldve"
                print(f"✅ {tag} → {chat_id}: {os.path.basename(path)}")
                sent_count += 1
                await asyncio.sleep(0.8)  # óvatosan a rate limittel
            except Forbidden:
                print(f"🚫 A felhasználó letiltotta a botot (chat={chat_id}) – kihagyom.")
                break
            except Exception as e:
                print(f"❌ Hiba ({chat_id}, {os.path.basename(path)}): {e}")
                # megyünk a következő fájlra / címzettre

    print(f"🎉 Kész: {len(recipients)} címzettnek összesen {sent_count} üzenet ment ki.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help='Csak ezek a megyék/“Országos” (vesszővel): pl. "Országos, Zala, Baranya"', default=None)
    ap.add_argument("--test-chat", type=int, help="Felülírja a címzetteket, ide küld tesztként", default=None)
    args = ap.parse_args()
    asyncio.run(run_async(only=args.only, test_chat=args.test_chat))

if __name__ == "__main__":
    main()
