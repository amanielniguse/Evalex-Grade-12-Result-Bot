
import asyncio
import json
import logging
import sqlite3
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
    MessageHandler,
    filters,
)
import httpx

# ==================== CONFIGURATION ==================== #
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK")

EAES_API_URL = "https://api.eaes.et/api/v1/results/bot"
DB_FILE = "eaes_cache.db"

# Concurrency & Rate Limiting Controls
MAX_CONCURRENT_REQUESTS = 12  # Prevents IP ban / Cloudflare 429
OUTBOUND_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
REQUEST_TIMEOUT = 10.0  # Seconds per attempt
MAX_RETRIES = 2  # Exponential backoff retries for 5xx/timeouts

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

# Reusable Global HTTP Client with Persistent Keep-Alive Pool
HTTP_CLIENT = httpx.AsyncClient(
    headers=HEADERS,
    http2=True,
    timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=5.0),
    limits=httpx.Limits(max_keepalive_connections=50, max_connections=100),
)


# ==================== PERSISTENT CACHE (SQLITE WAL) ==================== #
def init_db():
    """Initializes local cache database with Write-Ahead Logging for high concurrency."""
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
    """Reads result from local cache in 1ms."""
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
    """Saves verified 200 OK result locally forever."""
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


# ==================== FAIL-SAFE API FETCHER ==================== #
async def fetch_eaes_result_safely(
    admission_no: str, first_name: str
) -> tuple[int, dict]:
    """Fetches result with local cache check, concurrency throttling, and automatic retry."""
    adm = admission_no.strip()
    name = first_name.strip()

    # 1. Check local cache first (Instant zero-load hit)
    cached = await asyncio.to_thread(get_cached_result, adm)
    if cached:
        return 200, cached

    # 2. Limit concurrent outbound requests using Semaphore
    async with OUTBOUND_SEMAPHORE:
        params = {"admission_no": adm, "first_name": name}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await HTTP_CLIENT.get(EAES_API_URL, params=params)
                status = response.status_code

                try:
                    payload = response.json()
                except Exception:
                    payload = {"detail": response.text}

                # Cache immediately on success
                if status == 200 and isinstance(payload, dict):
                    await asyncio.to_thread(
                        save_cached_result, adm, name, payload
                    )
                    return 200, payload

                # If server returns transient gateway errors, retry
                if status in [500, 502, 503, 504, 429] and attempt < MAX_RETRIES:
                    await asyncio.sleep(1.5 * attempt)
                    continue

                return status, payload

            except (
                httpx.TimeoutException,
                httpx.ConnectError,
                httpx.ReadError,
            ) as err:
                logging.warning(
                    f"Attempt {attempt} failed for {adm}: {type(err).__name__}"
                )
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(1.5 * attempt)
                    continue
                return 504, {
                    "detail": "EAES server connection timed out under heavy load."
                }
            except Exception as ex:
                logging.error(f"Unexpected error during fetch: {ex}")
                return 500, {"detail": "Internal gateway lookup error."}

    return 500, {"detail": "Failed after multiple retries."}


# ==================== SUBSCRIPTION GATEWAY ==================== #
async def check_subscription(bot, user_id: int) -> bool:
    """Verifies membership; gracefully allows access if Telegram API glitches."""
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
            f"Telegram membership check failed ({e}). Bypassing gate gracefully."
        )
        return True


def join_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📢 Join Channel", url=CHANNEL_INVITE_LINK)],
            [
                InlineKeyboardButton(
                    "🔄 Verify & Continue",
                    callback_data="verify_subscription",
                )
            ],
        ]
    )


# ==================== DYNAMIC DATA PARSER ==================== #
def format_result_card(
    data: dict, admission_no: str
) -> tuple[str, str | None, InlineKeyboardMarkup | None]:
    name = (
        data.get("full_name")
        or data.get("student_name")
        or data.get("name")
        or "STUDENT"
    ).upper()
    school = (data.get("school") or data.get("school_name") or "N/A").upper()
    stream = data.get("stream") or data.get("stream_name") or "N/A"
    sex = data.get("sex") or data.get("gender") or "N/A"
    total_score = data.get("total") or data.get("total_score") or "0.00"
    photo_url = data.get("photo") or data.get("photo_url") or data.get("image")
    cert_url = data.get("certificate_url") or data.get("cert_url")

    # Dynamic Subject Extraction
    scores_raw = (
        data.get("results")
        or data.get("subjects")
        or data.get("scores")
        or {}
    )
    score_lines = []

    if isinstance(scores_raw, dict):
        for sub, score in scores_raw.items():
            if sub not in [
                "full_name",
                "school",
                "stream",
                "sex",
                "total",
                "total_score",
                "photo",
                "certificate_url",
            ]:
                score_lines.append(f"• {sub}: **{score}**")
    elif isinstance(scores_raw, list):
        for item in scores_raw:
            sub = item.get("name") or item.get("subject", "Subject")
            score = item.get("score") or item.get("result", "0")
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
            [[InlineKeyboardButton("🖨️ Print Certificate", url=cert_url)]]
        )
        if cert_url
        else None
    )
    return card_text, photo_url, markup


# ==================== HANDLERS ==================== #
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_subscription(context.bot, user_id):
        await update.message.reply_text(
            "⚠️ **Channel Subscription Required**\n\n"
            "Please join our channel below to get instant Grade 12 results.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=join_keyboard(),
        )
        return

    await update.message.reply_text(
        "👋 **Grade 12 Result Checker**\n\n"
        "Send your **Admission Number** and **First Name** separated by a space.\n\n"
        "**Example:**\n`2816147 Amaniel`",
        parse_mode=ParseMode.MARKDOWN,
    )


async def handle_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    query = update.callback_query
    await query.answer()

    if await check_subscription(context.bot, query.from_user.id):
        await query.edit_message_text(
            "✅ **Subscription Verified!**\n\n"
            "Send your **Admission Number** and **First Name** separated by a space.\n\n"
            "**Example:**\n`2816147 Amaniel`",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await query.answer(
            "❌ You haven't joined yet. Join the channel and press verify.",
            show_alert=True,
        )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_subscription(context.bot, user_id):
        await update.message.reply_text(
            "⚠️ Please join our channel to check results.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=join_keyboard(),
        )
        return

    tokens = update.message.text.strip().split()
    if len(tokens) < 2:
        await update.message.reply_text(
            "⚠️ **Invalid Format**\n\n"
            "Send both your **Admission Number** and **First Name**.\n"
            "**Example:** `2816147 Amaniel`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    admission_no, first_name = tokens[0], tokens[1]
    status_msg = await update.message.reply_text("⏳ Accessing database...")

    status_code, data = await fetch_eaes_result_safely(
        admission_no, first_name
    )

    if status_code == 423:
        detail = data.get("detail", "Results are not released yet.")
        await status_msg.edit_text(
            f"🔒 **LOCKED: NOT YET RELEASED**\n\n{detail}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

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
                return
            except Exception as e:
                logging.error(f"Failed photo delivery: {e}")

        await update.message.reply_text(
            card_text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup
        )
        return

    # Server Down or Heavy Load (500/502/504)
    if status_code in [500, 502, 503, 504]:
        await status_msg.edit_text(
            "⚠️ **EAES Server Overloaded**\n\n"
            "Official examination servers are currently experiencing extreme traffic. Please retry in 30 seconds.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # 404 / 400 Mismatched credentials
    detail = data.get(
        "detail",
        "Result not found. Verify your admission number and name spelling.",
    )
    await status_msg.edit_text(
        f"❌ **Error ({status_code})**\n\n{detail}",
        parse_mode=ParseMode.MARKDOWN,
    )


# ==================== MAIN APPLICATION ==================== #
if __name__ == "__main__":
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(
        CallbackQueryHandler(
            handle_callback, pattern="^verify_subscription$"
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    print("🛡️ EAES Fail-Safe Bot initialized and ready.")
    app.run_polling()
