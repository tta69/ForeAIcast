# bot.py
import os
import logging
import re
from datetime import date, timedelta, datetime, timezone

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler
)

# projektmodulok
from services.open_meteo import get_open_meteo_daily
from services.openweather import get_openweather_daily
from aggregator import consensus
from writer import _emoji_rain as emoji_rain, _deg as deg, _mm as mm, _weekday_hu as weekday_hu

# ==== ENV & LOG ====
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ERROR_CHAT = os.getenv("TELEGRAM_ERROR_CHAT_ID", "-3104033408")
DATABASE_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("milh-bot")

# ==== ÁLLAPOTOK ====
ASK_NAME, MAIN = range(2)

WELCOME = (
    "Üdv a Milyen idő lesz holnap? világában! 🌤️\n"
    "Mostantól neked is van egy személyre szabott időjárás-előrejelződ.\n\n"
    "Írd meg, hogyan szólíthatlak, aztán felmérem, hogy mire van igényed, "
    "hogy személyre szabottan tudjak segíteni neked."
)

# ==== DB SEGÉDEK ====

def db_exec(sql: str, params=None, fetchone=False):
    if not DATABASE_URL:
        raise RuntimeError("Hiányzik a DATABASE_URL a környezetből.")
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or {})
        if fetchone:
            return cur.fetchone()
        conn.commit()

def ensure_users_table():
    sql = """
    CREATE TABLE IF NOT EXISTS public.telegram_users (
        user_id    BIGINT PRIMARY KEY,
        chat_id    BIGINT NOT NULL,
        username   TEXT,
        name       TEXT,
        lang       TEXT DEFAULT 'hu',
        paused_until TIMESTAMPTZ,         -- felfüggesztés végéig
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ
    );
    """
    db_exec(sql)
    # régi táblák migrációja finoman
    db_exec("ALTER TABLE public.telegram_users ADD COLUMN IF NOT EXISTS paused_until TIMESTAMPTZ;")
    logger.info("✅ telegram_users tábla ellenőrizve / létrehozva")

def upsert_user(user_id: int, chat_id: int, name: str | None, username: str | None, lang: str | None):
    sql = """
    INSERT INTO public.telegram_users (user_id, chat_id, name, username, lang, created_at)
    VALUES (%(user_id)s, %(chat_id)s, %(name)s, %(username)s, COALESCE(%(lang)s,'hu'), NOW())
    ON CONFLICT (user_id) DO UPDATE
    SET chat_id    = EXCLUDED.chat_id,
        name       = EXCLUDED.name,
        username   = EXCLUDED.username,
        lang       = EXCLUDED.lang,
        updated_at = NOW();
    """
    db_exec(sql, {
        "user_id": user_id,
        "chat_id": chat_id,
        "name": name,
        "username": username,
        "lang": lang or "hu",
    })

def get_user(user_id: int):
    sql = "SELECT user_id, chat_id, name, username, lang, paused_until FROM public.telegram_users WHERE user_id=%(id)s;"
    return db_exec(sql, {"id": user_id}, fetchone=True)

def delete_user(user_id: int):
    db_exec("DELETE FROM public.telegram_users WHERE user_id=%(id)s;", {"id": user_id})

def set_pause(user_id: int, hours: int):
    db_exec(
        "UPDATE public.telegram_users SET paused_until = (NOW() AT TIME ZONE 'utc') + (%(h)s || ' hours')::interval, updated_at=NOW() WHERE user_id=%(id)s;",
        {"h": hours, "id": user_id}
    )

def clear_pause(user_id: int):
    db_exec("UPDATE public.telegram_users SET paused_until = NULL, updated_at=NOW() WHERE user_id=%(id)s;", {"id": user_id})

def is_paused(row: dict) -> bool:
    pu = row.get("paused_until")
    if not pu:
        return False
    # ha a múltban van, tekintsük aktívnak és nullázzuk
    if pu < datetime.now(timezone.utc):
        clear_pause(row["user_id"])
        return False
    return True

# ==== HIBAÉRTESÍTÉS ====
async def notify_error(context: ContextTypes.DEFAULT_TYPE, where: str, err: Exception):
    try:
        msg = f"🚨 Hiba a botban ({where}): {type(err).__name__}: {err}"
        await context.bot.send_message(chat_id=int(ERROR_CHAT), text=msg[:4090])
    except Exception as e:
        logger.exception("notify_error sikertelen: %s", e)

# ==== HELPER: VÁROS / ELŐREJELZÉS ====
def find_city_hu(name: str):
    sql = """
    WITH hu AS (SELECT id FROM public.countries WHERE iso2='HU')
    SELECT ci.name_hu AS city, co.name_hu AS county,
           COALESCE(ci.lat, ST_Y(ci.geom))::float8 AS lat,
           COALESCE(ci.lon, ST_X(ci.geom))::float8 AS lon
    FROM public.cities ci
    JOIN public.counties co ON co.id = ci.county_id
    WHERE ci.country_id = (SELECT id FROM hu)
      AND (ci.name_hu ILIKE %(q)s OR ci.slug ILIKE %(qslug)s)
    ORDER BY ci.is_county_seat DESC, ci.population DESC NULLS LAST, ci.name_hu
    LIMIT 1;
    """
    q = f"{name.strip()}%"
    qslug = f"{name.strip().lower().replace(' ', '-') }%"
    return db_exec(sql, {"q": q, "qslug": qslug}, fetchone=True)

def forecast_city(city_row: dict, when: str = "holnap") -> dict:
    lat, lon = city_row["lat"], city_row["lon"]
    offset = 0 if when == "ma" else 1
    target = date.today() + timedelta(days=offset)
    om = get_open_meteo_daily(lat, lon, lang="hu")
    try:
        ow = get_openweather_daily(lat, lon, lang="hu")
        con = consensus(om, ow)
    except Exception:
        con = {"tmax_c": om["tmax"], "tmin_c": om["tmin"], "precip_mm": om["precip_mm"]}
    pr = float(con["precip_mm"])
    return {
        "tmax": float(con["tmax_c"]),
        "tmin": float(con["tmin_c"]),
        "pr":   pr,
        "emoji": emoji_rain(pr),
        "target_date": target
    }

# ==== BOT KEZELŐK ====

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Első indításkor bekérjük a nevet"""
    try:
        ensure_users_table()
        tg_user = update.effective_user
        tg_chat = update.effective_chat
        user_row = get_user(tg_user.id)
        if user_row and user_row.get("name"):
            await update.message.reply_text(f"Üdv újra, {user_row['name']}! 🌤️\nÍrj egy várost (pl. „Szeged holnap”).")
            return MAIN
        # legalább az alapadatokat rögzítsük
        upsert_user(
            user_id=tg_user.id,
            chat_id=tg_chat.id,
            name=None,
            username=(tg_user.username or None),
            lang=(tg_user.language_code or "hu"),
        )
        await update.message.reply_text(WELCOME)
        return ASK_NAME
    except Exception as e:
        await notify_error(context, "start", e)
        raise

async def ask_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Név megadása"""
    name = (update.message.text or "").strip()
    if len(name) < 2 or len(name) > 50:
        await update.message.reply_text("Kérlek, írj be egy valódi nevet (2–50 karakter között).")
        return ASK_NAME

    tg_user = update.effective_user
    tg_chat = update.effective_chat
    lang = tg_user.language_code or "hu"

    upsert_user(
        user_id=tg_user.id,
        chat_id=tg_chat.id,
        name=name,
        username=(tg_user.username or None),
        lang=lang,
    )
    await update.message.reply_text(f"Köszönöm, {name}! 🌞\nMost már küldhetsz várost (pl. „Pécs holnap”).")
    return MAIN

CITY_RE = re.compile(r"^\s*([A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű\-\s]+)(?:\s+(ma|holnap))?\s*$")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Normál üzenet: város + nap"""
    try:
        tg_user = update.effective_user
        tg_chat = update.effective_chat
        row_before = get_user(tg_user.id)

        upsert_user(
            user_id=tg_user.id,
            chat_id=tg_chat.id,
            name=(row_before["name"] if row_before else None),
            username=(tg_user.username or None),
            lang=(tg_user.language_code or "hu"),
        )

        txt = (update.message.text or "").strip()
        m = CITY_RE.match(txt)
        if not m:
            await update.message.reply_text("Írd be így: „Szeged holnap” vagy „Debrecen ma”.\nParancsok: /pause 48, /resume, /stop")
            return MAIN

        city_query = m.group(1).strip()
        when = (m.group(2) or "holnap").lower()
        row = find_city_hu(city_query)
        if not row:
            await update.message.reply_text("Nem találtam ilyen települést. Próbáld pontosabban/ékezetekkel.")
            return MAIN

        fc = forecast_city(row, when)
        dow = weekday_hu(fc["target_date"]).lower()
        msg = (
            f"{fc['emoji']} {row['city']} ({row['county']}) – {when} ({dow}, {fc['target_date'].isoformat()})\n"
            f"• Csúcs: {deg(fc['tmax'])} | Min: {deg(fc['tmin'])}\n"
            f"• Csapadék (max): {mm(fc['pr'])}\n"
            f"Forrás: Open-Meteo + OpenWeather (konszenzus)"
        )

        # ha felfüggesztés alatt van, jelezzük (interaktív válasz ettől még mehet)
        row_after = get_user(tg_user.id)
        if row_after and is_paused(row_after):
            until = row_after["paused_until"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            msg += f"\n\n⏸️ Megjegyzés: a push értesítéseid {until}-ig fel vannak függesztve. (/resume)"

        await update.message.reply_text(msg)
    except Exception as e:
        logger.exception("text_handler hiba")
        await notify_error(context, "text_handler", e)
        await update.message.reply_text("Bocsi, valami hiba történt. Jelentettük, nézem!")
    return MAIN

# ---- STOP (adatok törlése, dupla megerősítés) ----
async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.user_data.get("stop_confirm"):
            context.user_data["stop_confirm"] = True
            await update.message.reply_text(
                "Biztos vagy benne, hogy törölni akarod az adataidat?\n"
                "Ha igen, írd be újra: /stop"
            )
            return
        # másodszor is megjött -> törlünk
        tg_user = update.effective_user
        delete_user(tg_user.id)
        context.user_data.clear()
        await update.message.reply_text("✅ Minden adatodat töröltük. Sajnálom, hogy elmész! Bármikor visszatérhetsz a /start paranccsal.")
    except Exception as e:
        await notify_error(context, "stop_cmd", e)
        await update.message.reply_text("Hiba történt a törlés közben. Jelentettem, megnézem.")

# ---- PAUSE / RESUME ----
async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args or []
        hours = 48
        if args and args[0].isdigit():
            hours = int(args[0])
            if hours not in (24, 48, 72, 96):
                hours = 48
        tg_user = update.effective_user
        set_pause(tg_user.id, hours)
        until = db_exec("SELECT paused_until FROM public.telegram_users WHERE user_id=%(id)s;", {"id": tg_user.id}, fetchone=True)["paused_until"]
        until_s = until.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        await update.message.reply_text(f"⏸️ A push értesítéseket felfüggesztettem {hours} órára (eddig: {until_s}).\nBármikor vissza: /resume")
    except Exception as e:
        await notify_error(context, "pause_cmd", e)
        await update.message.reply_text("Nem sikerült beállítani a felfüggesztést.")

async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        tg_user = update.effective_user
        clear_pause(tg_user.id)
        await update.message.reply_text("▶️ Felfüggesztés feloldva. Ismét küldünk push értesítéseket.")
    except Exception as e:
        await notify_error(context, "resume_cmd", e)
        await update.message.reply_text("Nem sikerült feloldani a felfüggesztést.")

# ==== GLOBÁLIS HIBAKEZELŐ ====
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Globális hiba: %s", context.error)
    await notify_error(context, "global", context.error)

# ==== FŐ FUTÁS ====
def main():
    if not TOKEN:
        raise RuntimeError("Hiányzik TELEGRAM_BOT_TOKEN.")
    if not DATABASE_URL:
        raise RuntimeError("Hiányzik DATABASE_URL (postgresql://user:pass@host/db).")
    ensure_users_table()

    app = ApplicationBuilder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_name)],
            MAIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler)],
        },
        fallbacks=[CommandHandler("start", start)],
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("pause", pause_cmd))
    app.add_handler(CommandHandler("resume", resume_cmd))

    app.add_error_handler(on_error)
    logger.info("🤖 Bot indul… (polling mód)")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
