# bot.py
import os
import logging
import asyncio
import regex as re  # Unicode-képes regex
from datetime import date, timedelta, datetime, timezone

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler
)

import openai  # AI szöveghez

# projektmodulok
from services.open_meteo import get_open_meteo_daily
from services.openweather import get_openweather_daily
from aggregator import consensus
from writer import _emoji_rain as emoji_rain, _deg as deg, _mm as mm

# ==== ENV & LOG ====
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ERROR_CHAT = os.getenv("TELEGRAM_ERROR_CHAT_ID", "-3104033408")
DATABASE_URL = os.getenv("DATABASE_URL")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL_WEATHER = os.getenv("OPENAI_MODEL_WEATHER", "gpt-5-mini")

if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

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

# ==== I18N ALAPOK ====

WEEKDAYS = {
    "hu": ["hétfő", "kedd", "szerda", "csütörtök", "péntek", "szombat", "vasárnap"],
    "en": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
    "ru": ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"],
}

MESSAGES = {
    "hu": {
        "usage": "Írd be így: „Szeged holnap” vagy „Debrecen ma”.\nParancsok: /pause 48, /resume, /stop, /lang hu",
        "not_found": "Nem találtam ilyen települést. Próbáld pontosabban / ékezetekkel.",
        "error_generic": "Bocsi, valami hiba történt. Jelentettük, nézem!",
        "pause_set": "⏸️ A push értesítéseket felfüggesztettem {hours} órára (eddig: {until}).\nBármikor vissza: /resume",
        "pause_fail": "Nem sikerült beállítani a felfüggesztést.",
        "resume_ok": "▶️ Felfüggesztés feloldva. Ismét küldünk push értesítéseket.",
        "resume_fail": "Nem sikerült feloldani a felfüggesztést.",
        "stop_confirm": "Biztos vagy benne, hogy törölni akarod az adataidat?\nHa igen, írd be újra: /stop",
        "stop_done": "✅ Minden adatodat töröltük. Sajnálom, hogy elmész! Bármikor visszatérhetsz a /start paranccsal.",
        "lang_set": "✅ Alap nyelv mostantól: {lang_name}.",
        "lang_invalid": "Ismert nyelvek: hu, en, ru. Használat: /lang hu",
    },
    "en": {
        "usage": "Type like: \"London tomorrow\" or \"Paris today\".\nCommands: /pause 48, /resume, /stop, /lang en",
        "not_found": "I couldn't find that place. Please try more precisely / with accents.",
        "error_generic": "Sorry, something went wrong. I've logged it.",
        "pause_set": "⏸️ Push notifications paused for {hours} hours (until: {until}).\nUse /resume to turn them back on.",
        "pause_fail": "Failed to set pause.",
        "resume_ok": "▶️ Pause removed. We will send push notifications again.",
        "resume_fail": "Failed to remove pause.",
        "stop_confirm": "Are you sure you want to delete all your data?\nIf yes, type /stop again.",
        "stop_done": "✅ All your data has been deleted. Sorry to see you go! You can come back anytime with /start.",
        "lang_set": "✅ Default language is now: {lang_name}.",
        "lang_invalid": "Supported languages: hu, en, ru. Usage: /lang en",
    },
    "ru": {
        "usage": "Напиши так: «Москва завтра» или «Будапешт сегодня».\nКоманды: /pause 48, /resume, /stop, /lang ru",
        "not_found": "Не нашёл такой населённый пункт. Попробуй точнее / с правильными буквами.",
        "error_generic": "Извини, что-то пошло не так. Я уже сообщил об ошибке.",
        "pause_set": "⏸️ Push-уведомления приостановлены на {hours} ч (до: {until}).\nВернуть: /resume",
        "pause_fail": "Не удалось включить паузу.",
        "resume_ok": "▶️ Пауза снята. Снова отправляем уведомления.",
        "resume_fail": "Не удалось снять паузу.",
        "stop_confirm": "Ты уверен, что хочешь удалить свои данные?\nЕсли да — набери /stop ещё раз.",
        "stop_done": "✅ Все твои данные удалены. Мне жаль, что ты уходишь! В любой момент можно вернуться с /start.",
        "lang_set": "✅ Язык по умолчанию теперь: {lang_name}.",
        "lang_invalid": "Поддерживаемые языки: hu, en, ru. Пример: /lang ru",
    },
}

LANG_NAMES = {
    "hu": "magyar",
    "en": "English",
    "ru": "русский",
}


def normalize_lang(code: str | None) -> str:
    if not code:
        return "hu"
    c = code.lower()
    if len(c) >= 2:
        c = c[:2]
    if c not in ("hu", "en", "ru"):
        return "hu"
    return c


def msg(lang: str, key: str, **kw) -> str:
    lang = normalize_lang(lang)
    base = MESSAGES.get(lang, MESSAGES["hu"]).get(key, "")
    return base.format(**kw)


def weekday_name(lang: str, dt: date) -> str:
    lang = normalize_lang(lang)
    arr = WEEKDAYS.get(lang, WEEKDAYS["hu"])
    return arr[dt.weekday()]

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
        user_id        BIGINT PRIMARY KEY,
        chat_id        BIGINT NOT NULL,
        username       TEXT,
        name           TEXT,
        lang           TEXT DEFAULT 'hu',
        preferred_lang TEXT,
        paused_until   TIMESTAMPTZ,
        created_at     TIMESTAMPTZ DEFAULT NOW(),
        updated_at     TIMESTAMPTZ
    );
    """
    db_exec(sql)
    db_exec("ALTER TABLE public.telegram_users ADD COLUMN IF NOT EXISTS paused_until TIMESTAMPTZ;")
    db_exec("ALTER TABLE public.telegram_users ADD COLUMN IF NOT EXISTS preferred_lang TEXT;")
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
        "lang": normalize_lang(lang or "hu"),
    })


def set_preferred_lang(user_id: int, lang: str):
    db_exec(
        "UPDATE public.telegram_users "
        "SET preferred_lang = %(lang)s, updated_at = NOW() "
        "WHERE user_id = %(id)s;",
        {"lang": normalize_lang(lang), "id": user_id}
    )


def get_user(user_id: int):
    sql = """
    SELECT user_id, chat_id, name, username, lang, preferred_lang, paused_until
    FROM public.telegram_users
    WHERE user_id=%(id)s;
    """
    return db_exec(sql, {"id": user_id}, fetchone=True)


def delete_user(user_id: int):
    db_exec("DELETE FROM public.telegram_users WHERE user_id=%(id)s;", {"id": user_id})


def set_pause(user_id: int, hours: int):
    db_exec(
        "UPDATE public.telegram_users "
        "SET paused_until = (NOW() AT TIME ZONE 'utc') + (%(h)s || ' hours')::interval, "
        "    updated_at=NOW() "
        "WHERE user_id=%(id)s;",
        {"h": hours, "id": user_id}
    )


def clear_pause(user_id: int):
    db_exec(
        "UPDATE public.telegram_users "
        "SET paused_until = NULL, updated_at=NOW() "
        "WHERE user_id=%(id)s;",
        {"id": user_id}
    )


def is_paused(row: dict) -> bool:
    pu = row.get("paused_until")
    if not pu:
        return False
    if pu < datetime.now(timezone.utc):
        clear_pause(row["user_id"])
        return False
    return True


def get_country_default_lang(iso2: str | None) -> str | None:
    if not iso2:
        return None
    row = db_exec(
        "SELECT default_lang FROM public.countries WHERE iso2=%(iso2)s;",
        {"iso2": iso2},
        fetchone=True,
    )
    if row and row.get("default_lang"):
        return normalize_lang(row["default_lang"])
    return None


def decide_lang(user_row: dict | None, country_iso2: str | None) -> str:
    # 1) felhasználó beállított nyelve
    if user_row and user_row.get("preferred_lang"):
        return normalize_lang(user_row["preferred_lang"])
    # 2) ország alapnyelve
    c_lang = get_country_default_lang(country_iso2)
    if c_lang:
        return c_lang
    # 3) Telegram UI nyelv
    if user_row and user_row.get("lang"):
        return normalize_lang(user_row["lang"])
    # fallback
    return "hu"

# ==== HIBAÉRTESÍTÉS ====


async def notify_error(context: ContextTypes.DEFAULT_TYPE, where: str, err: Exception):
    try:
        msg_txt = f"🚨 Hiba a botban ({where}): {type(err).__name__}: {err}"
        await context.bot.send_message(chat_id=int(ERROR_CHAT), text=msg_txt[:4090])
    except Exception as e:
        logger.exception("notify_error sikertelen: %s", e)

# ==== HELPER: VÁROS / ELŐREJELZÉS ====


def _slugify(s: str) -> str:
    return re.sub(r"\s+", "-", (s or "").strip().lower())


def find_city_any(name: str):
    """
    Világszintű keresés a public.cities táblában.
    Magyar találat előnyben, majd megyeszékhely, aztán lakosság szerint.
    """
    sql = """
    SELECT
      ci.name_hu AS city,
      co.name_en AS country,
      cn.name_hu AS county,
      COALESCE(ci.lat, ST_Y(ci.geom))::float8 AS lat,
      COALESCE(ci.lon, ST_X(ci.geom))::float8 AS lon,
      co.iso2     AS iso2
    FROM public.cities ci
    JOIN public.countries co ON co.id = ci.country_id
    LEFT JOIN public.counties  cn ON cn.id = ci.county_id
    WHERE (ci.name_hu ILIKE %(q)s OR ci.slug ILIKE %(qslug)s)
    ORDER BY
      (co.iso2 = 'HU') DESC,
      ci.is_county_seat DESC NULLS LAST,
      COALESCE(ci.population, 0) DESC,
      ci.name_hu
    LIMIT 1;
    """
    q = f"{name.strip()}%"
    qslug = f"{_slugify(name)}%"
    return db_exec(sql, {"q": q, "qslug": qslug}, fetchone=True)


def forecast_city(city_row: dict, when: str, lang: str) -> dict:
    lat, lon = city_row["lat"], city_row["lon"]
    offset = 0 if when == "ma" else 1
    target = date.today() + timedelta(days=offset)

    lang = normalize_lang(lang)
    om = get_open_meteo_daily(lat, lon, lang=lang)
    try:
        ow = get_openweather_daily(lat, lon, lang=lang)
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

# ==== AI SZÖVEG GENERÁLÁS ====


def _build_ai_messages(lang: str, row: dict, fc: dict, when_token: str):
    """System + user üzenetek az AI-nak."""
    lang = normalize_lang(lang)
    dt = fc["target_date"]
    dow = weekday_name(lang, dt)

    place_parts = [p for p in [row.get("county"), row.get("country")] if p]
    place = ", ".join(place_parts) if place_parts else row.get("country") or ""
    loc = f"{row['city']} ({place})" if place else row["city"]

    when_label_map = {
        "hu": {"ma": "ma", "holnap": "holnap"},
        "en": {"ma": "today", "holnap": "tomorrow"},
        "ru": {"ma": "сегодня", "holnap": "завтра"},
    }
    wl = when_label_map.get(lang, when_label_map["hu"])
    when_label = wl.get(when_token, when_token)

    # rövid instrukciók a modellnek
    if lang == "hu":
        system_msg = (
            "Te egy rövid, közérthető időjárás-előrejelzést írsz magyarul. "
            "Legyen maximum 3 mondat. Ne írj semmi extrát, csak a szöveget."
        )
        user_msg = (
            f"Hely: {loc}\n"
            f"Dátum: {dt.isoformat()} ({dow}, {when_label})\n"
            f"Nappali csúcs: {fc['tmax']:.1f} °C\n"
            f"Éjszakai minimum: {fc['tmin']:.1f} °C\n"
            f"Várható csapadék maximum: {fc['pr']:.1f} mm\n\n"
            "Fogalmazz természetes, emberi hangon, néhány szóban utalj rá, "
            "hogy esernyőre vagy kabátra szükség lehet-e."
        )
    elif lang == "en":
        system_msg = (
            "You write a short, clear weather forecast in English. "
            "Maximum 3 sentences. Output only the text, nothing else."
        )
        user_msg = (
            f"Location: {loc}\n"
            f"Date: {dt.isoformat()} ({dow}, {when_label})\n"
            f"Daytime high: {fc['tmax']:.1f} °C\n"
            f"Nighttime low: {fc['tmin']:.1f} °C\n"
            f"Expected max precipitation: {fc['pr']:.1f} mm\n\n"
            "Write in a natural tone and mention briefly if an umbrella or a jacket might be needed."
        )
    else:  # ru
        system_msg = (
            "Ты пишешь короткий, понятный прогноз погоды на русском языке. "
            "Не более 3 предложений. Никаких лишних комментариев, только текст."
        )
        user_msg = (
            f"Место: {loc}\n"
            f"Дата: {dt.isoformat()} ({dow}, {when_label})\n"
            f"Дневной максимум: {fc['tmax']:.1f} °C\n"
            f"Ночной минимум: {fc['tmin']:.1f} °C\n"
            f"Ожидаемые осадки (максимум): {fc['pr']:.1f} мм\n\n"
            "Напиши естественным тоном и упомяни, понадобится ли зонт или тёплая одежда."
        )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


def generate_ai_forecast_text(lang: str, row: dict, fc: dict, when_token: str) -> str | None:
    """Szinchr. wrapper az OpenAI híváshoz. Hiba esetén None."""
    if not OPENAI_API_KEY:
        return None
    try:
        messages = _build_ai_messages(lang, row, fc, when_token)
        # klasszikus ChatCompletion API-t használunk
        resp = openai.ChatCompletion.create(
            model=OPENAI_MODEL_WEATHER,
            messages=messages,
            temperature=0.5,
            max_tokens=300,
        )
        text = resp["choices"][0]["message"]["content"].strip()
        return text
    except Exception as e:
        logger.exception("AI forecast hiba: %s", e)
        return None


def format_fallback_message(lang: str, row: dict, fc: dict, when_token: str) -> str:
    """Régi sablon – AI hiba esetén használjuk."""
    lang = normalize_lang(lang)
    dt = fc["target_date"]
    dow = weekday_name(lang, dt)

    when_label_map = {
        "hu": {"ma": "ma", "holnap": "holnap"},
        "en": {"ma": "today", "holnap": "tomorrow"},
        "ru": {"ma": "сегодня", "holnap": "завтра"},
    }
    wl = when_label_map.get(lang, when_label_map["hu"])
    when_label = wl.get(when_token, when_token)

    place_parts = [p for p in [row.get("county"), row.get("country")] if p]
    place = ", ".join(place_parts) if place_parts else row.get("country") or ""
    loc = f"{row['city']} ({place})" if place else row["city"]

    if lang == "hu":
        header = f"{fc['emoji']} {loc} – {when_label} ({dow}, {dt.isoformat()})"
        lines = [
            f"• Csúcs: {deg(fc['tmax'])} | Min: {deg(fc['tmin'])}",
            f"• Csapadék (max): {mm(fc['pr'])}",
            "Forrás: Open-Meteo + OpenWeather (konszenzus)",
        ]
    elif lang == "en":
        header = f"{fc['emoji']} {loc} – {when_label} ({dow}, {dt.isoformat()})"
        lines = [
            f"• High: {deg(fc['tmax'])} | Low: {deg(fc['tmin'])}",
            f"• Precipitation (max): {mm(fc['pr'])}",
            "Source: Open-Meteo + OpenWeather (consensus)",
        ]
    else:  # ru
        header = f"{fc['emoji']} {loc} – {when_label} ({dow}, {dt.isoformat()})"
        lines = [
            f"• Максимум: {deg(fc['tmax'])} | Минимум: {deg(fc['tmin'])}",
            f"• Осадки (макс.): {mm(fc['pr'])}",
            "Источник: Open-Meteо + OpenWeather (консенсус)",
        ]

    return header + "\n" + "\n".join(lines)

# ==== BOT KEZELŐK ====


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Első indításkor bekérjük a nevet"""
    try:
        ensure_users_table()
        tg_user = update.effective_user
        tg_chat = update.effective_chat
        user_row = get_user(tg_user.id)
        if user_row and user_row.get("name"):
            lang = decide_lang(user_row, None)
            await update.message.reply_text(
                f"Üdv újra, {user_row['name']}! 🌤️\n" +
                msg(lang, "usage")
            )
            return MAIN
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


# Unicode-képes: bármilyen betű (latin, cirill, stb.) + szóköz, kötőjel, pont, aposztróf
CITY_RE = re.compile(
    r"^\s*([\p{L}\s\-\.'’]+?)(?:\s+(ma|holnap|today|tomorrow|сегодня|завтра))?\s*$",
    re.IGNORECASE
)


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
            lang = decide_lang(row_before, None)
            await update.message.reply_text(msg(lang, "usage"))
            return MAIN

        city_query = m.group(1).strip()
        when_raw = (m.group(2) or "holnap").lower()

        # belső token: csak "ma" / "holnap"
        if when_raw in ("ma", "today", "сегодня"):
            when = "ma"
        else:
            when = "holnap"

        row = find_city_any(city_query)
        if not row:
            lang_nf = decide_lang(row_before, None)
            await update.message.reply_text(msg(lang_nf, "not_found"))
            return MAIN

        # nyelv döntés (user + ország)
        lang = decide_lang(row_before, row.get("iso2"))

        fc = forecast_city(row, when, lang)

        # AI-szöveg (blokkoló hívás külön szálon)
        ai_text = await asyncio.to_thread(
            generate_ai_forecast_text, lang, row, fc, when
        )
        if ai_text:
            msg_txt = ai_text
        else:
            msg_txt = format_fallback_message(lang, row, fc, when)

        # ha felfüggesztés alatt van, jelezzük
        row_after = get_user(tg_user.id)
        if row_after and is_paused(row_after):
            until = row_after["paused_until"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            note = {
                "hu": f"\n\n⏸️ Megjegyzés: a push értesítéseid {until}-ig fel vannak függesztve. (/resume)",
                "en": f"\n\n⏸️ Note: your push notifications are paused until {until}. (/resume)",
                "ru": f"\n\n⏸️ Замечание: push-уведомления приостановлены до {until}. (/resume)",
            }.get(lang, "")
            msg_txt += note

        await update.message.reply_text(msg_txt)
    except Exception as e:
        logger.exception("text_handler hiba")
        await notify_error(context, "text_handler", e)
        await update.message.reply_text(msg("hu", "error_generic"))
    return MAIN

# ---- /lang – alap nyelv beállítása ----


async def lang_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    urow = get_user(tg_user.id)
    base_lang = decide_lang(urow, None)

    args = context.args or []
    if not args:
        current = urow.get("preferred_lang") if urow else None
        txt = {
            "hu": f"Jelenlegi alapnyelv: {LANG_NAMES.get(normalize_lang(current), 'magyar')}.\n"
                  f"Átváltás: /lang hu | /lang en | /lang ru",
            "en": f"Current default language: {LANG_NAMES.get(normalize_lang(current), 'Hungarian')}.\n"
                  f"Change with: /lang hu | /lang en | /lang ru",
            "ru": f"Текущий язык по умолчанию: {LANG_NAMES.get(normalize_lang(current), 'венгерский')}.\n"
                  f"Сменить: /lang hu | /lang en | /lang ru",
        }.get(base_lang, "")
        await update.message.reply_text(txt)
        return

    new_lang = normalize_lang(args[0])
    if new_lang not in ("hu", "en", "ru"):
        await update.message.reply_text(msg(base_lang, "lang_invalid"))
        return

    set_preferred_lang(tg_user.id, new_lang)
    await update.message.reply_text(
        msg(new_lang, "lang_set", lang_name=LANG_NAMES.get(new_lang, new_lang))
    )

# ---- STOP (adatok törlése, dupla megerősítés) ----


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        urow = get_user(update.effective_user.id)
        lang = decide_lang(urow, None)
        if not context.user_data.get("stop_confirm"):
            context.user_data["stop_confirm"] = True
            await update.message.reply_text(msg(lang, "stop_confirm"))
            return
        tg_user = update.effective_user
        delete_user(tg_user.id)
        context.user_data.clear()
        await update.message.reply_text(msg(lang, "stop_done"))
    except Exception as e:
        await notify_error(context, "stop_cmd", e)
        await update.message.reply_text(msg("hu", "error_generic"))

# ---- PAUSE / RESUME ----


async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        urow = get_user(update.effective_user.id)
        lang = decide_lang(urow, None)

        args = context.args or []
        hours = 48
        if args and args[0].isdigit():
            hours = int(args[0])
            if hours not in (24, 48, 72, 96):
                hours = 48
        tg_user = update.effective_user
        set_pause(tg_user.id, hours)
        row = db_exec(
            "SELECT paused_until FROM public.telegram_users WHERE user_id=%(id)s;",
            {"id": tg_user.id},
            fetchone=True
        )
        until = row["paused_until"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        await update.message.reply_text(
            msg(lang, "pause_set", hours=hours, until=until)
        )
    except Exception as e:
        await notify_error(context, "pause_cmd", e)
        await update.message.reply_text(msg("hu", "pause_fail"))


async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        urow = get_user(update.effective_user.id)
        lang = decide_lang(urow, None)
        tg_user = update.effective_user
        clear_pause(tg_user.id)
        await update.message.reply_text(msg(lang, "resume_ok"))
    except Exception as e:
        await notify_error(context, "resume_cmd", e)
        await update.message.reply_text(msg("hu", "resume_fail"))

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

    if not OPENAI_API_KEY:
        logger.warning("⚠️ Nincs OPENAI_API_KEY beállítva, AI szöveg helyett sablonos üzenet lesz.")

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
    app.add_handler(CommandHandler("lang", lang_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("pause", pause_cmd))
    app.add_handler(CommandHandler("resume", resume_cmd))

    app.add_error_handler(on_error)
    logger.info("🤖 Bot indul… (polling mód)")

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
