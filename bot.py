import asyncio
import json
import logging
import os
import sqlite3
import sys
import time

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
import httpx

# ==================== CONFIGURATION ==================== #
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")  # e.g., @YourChannel
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK")  # e.g., https://t.me/YourChannel

if not BOT_TOKEN or not CHANNEL_USERNAME or not CHANNEL_INVITE_LINK:
    sys.exit(
        "❌ Missing required environment variables! Ensure BOT_TOKEN, CHANNEL_USERNAME, and CHANNEL_INVITE_LINK are set."
    )

if not CHANNEL_USERNAME.startswith("@"):
    CHANNEL_USERNAME = f"@{CHANNEL_USERNAME}"

# Endpoints
SESSION_KEY_URL = "https://api.eaes.et/api/v1/session/key"
RESULTS_WEB_URL = "https://api.eaes.et/api/v1/results/web"
DB_FILE = os.getenv("DB_PATH", "eaes_cache.db")

# Conversation States
STATE_ADMISSION = 1
STATE_FIRST_NAME = 2

# Network & Concurrency
MAX_CONCURRENT_REQUESTS = 12
OUTBOUND_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
REQUEST_TIMEOUT = 12.0
MAX_RETRIES = 2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://eaes-mini-app.pages.dev/",
    "Origin": "https://eaes-mini-app.pages.dev",
    "Accept": "application/json",
}

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Shared HTTP Client with Keep-Alive Pool
HTTP_CLIENT = httpx.AsyncClient(
    headers=HEADERS,
    http2=True,
    timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=5.0),
    limits=httpx.Limits(max_keepalive_connections=50, max_connections=100),
)

# Global Session Key Cache
CACHED_SESSION_KEY = None
SESSION_KEY_EXPIRES = 0


# ==================== DATABASE (CACHE) ==================== #
def init_db():
    """Initializes local SQLite database with Write-Ahead Logging."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS result_cache (
                admission_no TEXT PRIMARY KEY,
                first_name TEXT,
                data_json TEXT,
                created_at INTEGER
            )
            """
        )
        conn.commit()


def get_cached_result(admission_no: str) -> dict | None:
    """Retrieves cached result in ~1ms to reduce server load."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT data_json FROM result_cache WHERE admission_no = ?",
                (admission_no.strip(),),
            )
            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
    except Exception as e:
        logging.error(f"Cache read error: {e}")
    return None


def save_cached_result(admission_no: str, first_name: str, data: dict):
    """Saves valid result payload to local database."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO result_cache VALUES (?, ?, ?, ?)",
                (
                    admission_no.strip(),
                    first_name.strip().lower(),
                    json.dumps(data),
                    int(time.time()),
                ),
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Cache write error: {e}")


# ==================== DYNAMIC SESSION HANDSHAKE ==================== #
async def get_valid_session_key(client: httpx.AsyncClient) -> str | None:
    """Fetches and caches an active session token from EAES."""
    global CACHED_SESSION_KEY, SESSION_KEY_EXPIRES

    if CACHED_SESSION_KEY and time.time() < SESSION_KEY_EXPIRES:
        return CACHED_SESSION_KEY

    try:
        res = await client.get(SESSION_KEY_URL)
        if res.status_code == 200:
            data = res.json()
            key = None

            if isinstance(data, dict):
                key = (
                    data.get("key")
                    or data.get("apiKey")
                    or data.get("token")
                    or data.get("session_key")
                    or data.get("data")
                )
            elif isinstance(data, str):
                key = data

            if key:
                CACHED_SESSION_KEY = str(key).strip()
                SESSION_KEY_EXPIRES = time.time() + 300  # Cache for 5 minutes
                return CACHED_SESSION_KEY
    except Exception as e:
        logging.error(f"Failed to fetch session key: {e}")

    return None


# ==================== SUBSCRIPTION GATEWAY ==================== #
async def is_user_subscribed(bot, user_id: int) -> bool:
    """Checks whether the user has joined the required channel."""
    try:
        member = await bot.get_chat_member(
            chat_id=CHANNEL_USERNAME, user_id=user_id
        )
        return member.status in [
            "creator",
            "administrator",
            "member",
            "restricted",
        ]
    except Exception as e:
        logging.warning(
            f"Subscription check error ({e}). Ensure bot is an admin in the channel."
        )
        return False


def get_join_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📢 Join Channel First", url=CHANNEL_INVITE_LINK
                )
            ],
            [
                InlineKeyboardButton(
                    "🔄 I Have Joined (Verify)",
                    callback_data="verify_subscription",
                )
            ],
        ]
    )


# ==================== API FETCHER ==================== #
async def fetch_eaes_result(
    admission_no: str, first_name: str
) -> tuple[int, dict]:
    """Queries the EAES backend API with automatic session management and retries."""
    adm = admission_no.strip()
    name = first_name.strip()

    # 1. Local Cache Lookup
    cached = await asyncio.to_thread(get_cached_result, adm)
    if cached:
        return 200, cached

    # 2. Query with Concurrency Limit
    async with OUTBOUND_SEMAPHORE:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                session_key = await get_valid_session_key(HTTP_CLIENT)

                request_headers = {
                    **HEADERS,
                    "x-api-key": str(session_key) if session_key else "",
                    "authorization": f"Bearer {session_key}"
                    if session_key
                    else "",
                }

                params = {
                    "admission_no": adm,
                    "first_name": name,
                    "key": session_key or "",
                }

                response = await HTTP_CLIENT.get(
                    RESULTS_WEB_URL, params=params, headers=request_headers
                )
                status = response.status_code

                try:
                    payload = response.json()
                except Exception:
                    payload = {"detail": response.text}

                # Success
                if status == 200 and isinstance(payload, dict):
                    await asyncio.to_thread(
                        save_cached_result, adm, name, payload
                    )
                    return 200, payload

                # Expired or Invalid Key -> Invalidate cache and retry
                if status in [401, 403]:
                    global CACHED_SESSION_KEY
                    CACHED_SESSION_KEY = None
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(1.0)
                        continue

                # Temporary Server Lag -> Exponential Backoff
                if status in [500, 502, 503, 504, 429] and attempt < MAX_RETRIES:
                    await asyncio.sleep(1.5 * attempt)
                    continue

                return status, payload

            except (
                httpx.TimeoutException,
                httpx.ConnectError,
                httpx.ReadError,
            ):
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(1.5 * attempt)
                    continue
                return 504, {
                    "detail": "EAES server connection timed out under heavy load."
                }
            except Exception as e:
                logging.error(f"Fetch execution error: {e}")
                return 500, {"detail": "Internal server lookup error."}

    return 500, {"detail": "Failed to fetch result after retries."}


# ==================== DATA FORMATTER ==================== #
def format_result_card(
    data: dict, admission_no: str
) -> tuple[str, str | None, InlineKeyboardMarkup | None]:
    """Formats EAES student payload matching the official card design."""
    info = data.get("studentInfo", {})
    results_list = data.get("results", [])

    # Student Demographics
    name = str(
        info.get("FullName")
        or info.get("student_name")
        or info.get("name")
        or "STUDENT"
    ).strip().upper()
    school = str(
        info.get("School") or info.get("school_name") or "N/A"
    ).strip().upper()
    stream = str(info.get("Stream") or info.get("stream_name") or "N/A").strip()

    raw_sex = str(info.get("Sex") or info.get("gender") or "").strip().upper()
    sex = (
        "M"
        if raw_sex == "MALE"
        else ("F" if raw_sex == "FEMALE" else (raw_sex or "N/A"))
    )

    total_score = str(
        info.get("Total") or info.get("total_score") or "0.00"
    ).strip()
    photo_url = info.get("Photo") or info.get("photo_url")
    print_url = info.get("print") or info.get("certificate_url")

    # Subject Results Extraction
    score_lines = []
    if isinstance(results_list, list):
        for item in results_list:
            sub = str(
                item.get("Subject") or item.get("name") or "Subject"
            ).strip()
            score = str(
                item.get("Result") or item.get("score") or "0"
            ).strip()
            score_lines.append(f"• {sub}: **{score}**")
    elif isinstance(results_list, dict):
        for sub, score in results_list.items():
            if sub not in [
                "studentInfo",
                "FullName",
                "School",
                "Stream",
                "Sex",
                "Total",
                "Photo",
                "print",
            ]:
                score_lines.append(f"• {sub}: **{score}**")

    scores_text = (
        "\n".join(score_lines)
        if score_lines
        else "• Scores processing complete."
    )

    card_text = (
        f"**{name}**\n"
        f"─ ───────────────────────────\n\n"
        f"**Personal Information:**\n"
        f"🎟️ **Admission No:** `{admission_no}`\n"
        f"🏫 **School:** __{school}__\n"
        f"📚 **Stream:** {stream}\n"
        f"👤 **Sex:** {sex}\n\n"
        f"**Results:**\n"
        f"{scores_text}\n\n"
        f"🎉 **Total Score: {total_score}**\n\n"
        f"To check another result, please send /start."
    )

    markup = (
        InlineKeyboardMarkup(
            [[InlineKeyboardButton("🖨️ Print Certificate", url=print_url)]]
        )
        if print_url
        else None
    )

    return card_text, photo_url, markup


# ==================== CONVERSATION HANDLERS ==================== #
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    subscribed = await is_user_subscribed(context.bot, user_id)

    if not subscribed:
        await update.message.reply_text(
            "⚠️ **Subscription Required**\n\n"
            "To view your Grade 12 National Exam result, please join our channel first.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_join_keyboard(),
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "👋 **Grade 12 Result Checker**\n\n"
        "🎟️ **Step 1/2:** Please enter your **Admission / Registration Number**:",
        parse_mode=ParseMode.MARKDOWN,
    )
    return STATE_ADMISSION


async def handle_callback_verification(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    await query.answer()

    subscribed = await is_user_subscribed(context.bot, query.from_user.id)
    if subscribed:
        await query.edit_message_text(
            "✅ **Subscription Verified!**\n\n"
            "🎟️ **Step 1/2:** Please enter your **Admission / Registration Number**:",
            parse_mode=ParseMode.MARKDOWN,
        )
        return STATE_ADMISSION
    else:
        await query.answer(
            "❌ You haven't joined yet! Please join the channel first.",
            show_alert=True,
        )
        return ConversationHandler.END


async def handle_admission_step(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    admission_no = update.message.text.strip()

    if not admission_no.isalnum() or len(admission_no) < 4:
        await update.message.reply_text(
            "⚠️ Please enter a valid registration number (digits/letters only):"
        )
        return STATE_ADMISSION

    context.user_data["admission_no"] = admission_no

    await update.message.reply_text(
        f"✅ Admission No: `{admission_no}` saved.\n\n"
        "👤 **Step 2/2:** Now enter your **First Name** (as registered on your exam slip):",
        parse_mode=ParseMode.MARKDOWN,
    )
    return STATE_FIRST_NAME


async def handle_first_name_step(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    first_name = update.message.text.strip()
    admission_no = context.user_data.get("admission_no")

    if not admission_no:
        await update.message.reply_text(
            "⚠️ Session expired. Please type /start to try again."
        )
        return ConversationHandler.END

    status_msg = await update.message.reply_text(
        "⏳ Fetching your result from EAES database..."
    )

    status_code, data = await fetch_eaes_result(admission_no, first_name)

    # 423 Locked
    if status_code == 423:
        detail = data.get(
            "detail", "The results for this academic year are not released yet."
        )
        await status_msg.edit_text(
            f"🔒 **STATUS: NOT YET RELEASED**\n\n{detail}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    # 200 OK
    if status_code == 200:
        card_text, photo_url, markup = format_result_card(data, admission_no)
        await status_msg.delete()

        if photo_url:
            try:
                await update.message.reply_photo(
                    photo=photo_url,
                    caption=card_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=markup,
                )
                return ConversationHandler.END
            except Exception as e:
                logging.error(f"Failed photo delivery: {e}")

        await update.message.reply_text(
            card_text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup
        )
        return ConversationHandler.END

    # Server Overloaded (500/502/503/504)
    if status_code in [500, 502, 503, 504]:
        await status_msg.edit_text(
            "⚠️ **EAES Server Overloaded**\n\n"
            "Official examination servers are under heavy load. Please retry in 30 seconds with /start.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    # Mismatched / Not Found
    detail = data.get(
        "detail",
        "Result not found. Verify your admission number and name spelling.",
    )
    await status_msg.edit_text(
        f"❌ **Error ({status_code})**\n\n{detail}\n\nSend /start to try again.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Action cancelled. Send /start whenever you want to check a result."
    )
    return ConversationHandler.END


# ==================== MAIN RUNNER ==================== #
if __name__ == "__main__":
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CallbackQueryHandler(
                handle_callback_verification, pattern="^verify_subscription$"
            ),
        ],
        states={
            STATE_ADMISSION: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, handle_admission_step
                )
            ],
            STATE_FIRST_NAME: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, handle_first_name_step
                )
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("cancel", cancel),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)

    print("🛡️ EAES Production Bot with Auto-Handshake active and polling.")
    app.run_polling()
