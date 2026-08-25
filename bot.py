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
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")  # e.g., @YourChannelUsername
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK")  # e.g., https://t.me/YourChannelUsername

if not BOT_TOKEN or not CHANNEL_USERNAME or not CHANNEL_INVITE_LINK:
    sys.exit("❌ Error: Missing BOT_TOKEN, CHANNEL_USERNAME, or CHANNEL_INVITE_LINK in Railway variables.")

# Ensure username starts with @
if not CHANNEL_USERNAME.startswith("@"):
    CHANNEL_USERNAME = f"@{CHANNEL_USERNAME}"

EAES_API_URL = "https://api.eaes.et/api/v1/results/bot"
DB_FILE = "eaes_cache.db"

# Conversation States
STATE_ADMISSION = 1
STATE_FIRST_NAME = 2

# Concurrency & Network Settings
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

HTTP_CLIENT = httpx.AsyncClient(
    headers=HEADERS,
    http2=True,
    timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=5.0),
    limits=httpx.Limits(max_keepalive_connections=50, max_connections=100),
)


# ==================== DATABASE (CACHE) ==================== #
def init_db():
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
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO result_cache VALUES (?, ?, ?, ?)",
                (admission_no.strip(), first_name.strip().lower(), json.dumps(data), int(time.time())),
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Cache write error: {e}")


# ==================== MEMBERSHIP CHECK ==================== #
async def is_user_subscribed(bot, user_id: int) -> bool:
    """Strictly checks whether the user is joined to the required channel."""
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
        return member.status in ["creator", "administrator", "member", "restricted"]
    except Exception as e:
        logging.error(f"Subscription check failed for {user_id}: {e}")
        # Note: Make sure the bot is an Administrator in the channel to check user statuses
        return False


def get_join_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📢 Join Channel First", url=CHANNEL_INVITE_LINK)],
            [InlineKeyboardButton("🔄 I Have Joined (Verify)", callback_data="verify_subscription")],
        ]
    )


# ==================== API FETCHER ==================== #
async def fetch_eaes_result(admission_no: str, first_name: str) -> tuple[int, dict]:
    adm = admission_no.strip()
    name = first_name.strip()

    cached = await asyncio.to_thread(get_cached_result, adm)
    if cached:
        return 200, cached

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

                if status == 200 and isinstance(payload, dict):
                    await asyncio.to_thread(save_cached_result, adm, name, payload)
                    return 200, payload

                if status in [500, 502, 503, 504, 429] and attempt < MAX_RETRIES:
                    await asyncio.sleep(1.5 * attempt)
                    continue

                return status, payload
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError):
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(1.5 * attempt)
                    continue
                return 504, {"detail": "EAES server connection timed out."}
            except Exception as e:
                logging.error(f"Fetch exception: {e}")
                return 500, {"detail": "An internal error occurred."}

    return 500, {"detail": "Failed to retrieve results."}


# ==================== FORMATTER ==================== #
def format_result_card(data: dict, admission_no: str) -> tuple[str, str | None, InlineKeyboardMarkup | None]:
    name = (data.get("full_name") or data.get("student_name") or data.get("name") or "STUDENT").upper()
    school = (data.get("school") or data.get("school_name") or "N/A").upper()
    stream = data.get("stream") or data.get("stream_name") or "N/A"
    sex = data.get("sex") or data.get("gender") or "N/A"
    total_score = data.get("total") or data.get("total_score") or "0.00"
    photo_url = data.get("photo") or data.get("photo_url") or data.get("image")
    cert_url = data.get("certificate_url") or data.get("cert_url")

    raw_scores = data.get("results") or data.get("subjects") or data.get("scores") or {}
    score_lines = []

    if isinstance(raw_scores, dict):
        for sub, score in raw_scores.items():
            if sub not in ["full_name", "school", "stream", "sex", "total", "total_score", "photo", "certificate_url"]:
                score_lines.append(f"• {sub}: **{score}**")
    elif isinstance(raw_scores, list):
        for item in raw_scores:
            sub = item.get("name") or item.get("subject", "Subject")
            score = item.get("score") or item.get("result", "0")
            score_lines.append(f"• {sub}: **{score}**")

    scores_text = "\n".join(score_lines) if score_lines else "• Scores processing complete."

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
        InlineKeyboardMarkup([[InlineKeyboardButton("🖨️ Print Certificate", url=cert_url)]])
        if cert_url
        else None
    )
    return card_text, photo_url, markup


# ==================== CONVERSATION HANDLERS ==================== #
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Entry point: Enforces subscription before starting the step-by-step query."""
    user_id = update.effective_user.id
    subscribed = await is_user_subscribed(context.bot, user_id)

    if not subscribed:
        await update.message.reply_text(
            "⚠️ **Subscription Required**\n\n"
            "To view your Grade 12 National Exam result, please join our official update channel first.",
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


async def handle_callback_verification(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Verifies membership when user taps 'I Have Joined'."""
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
        await query.answer("❌ You haven't joined yet! Please join the channel first.", show_alert=True)
        return ConversationHandler.END


async def handle_admission_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Captures the admission number and prompts for first name."""
    admission_no = update.message.text.strip()

    if not admission_no.isalnum() or len(admission_no) < 4:
        await update.message.reply_text("⚠️ Please enter a valid registration number (digits/letters only):")
        return STATE_ADMISSION

    context.user_data["admission_no"] = admission_no

    await update.message.reply_text(
        f"✅ Admission No: `{admission_no}` saved.\n\n"
        "👤 **Step 2/2:** Now enter your **First Name** (as registered on your admission card):",
        parse_mode=ParseMode.MARKDOWN,
    )
    return STATE_FIRST_NAME


async def handle_first_name_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Captures first name, performs verification, and returns the result."""
    first_name = update.message.text.strip()
    admission_no = context.user_data.get("admission_no")

    if not admission_no:
        await update.message.reply_text("⚠️ Session expired. Please type /start to try again.")
        return ConversationHandler.END

    status_msg = await update.message.reply_text("⏳ Querying EAES Database...")

    status_code, data = await fetch_eaes_result(admission_no, first_name)

    if status_code == 423:
        detail = data.get("detail", "Results are not released yet.")
        await status_msg.edit_text(f"🔒 **STATUS: NOT YET RELEASED**\n\n{detail}", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

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

        await update.message.reply_text(card_text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
        return ConversationHandler.END

    if status_code in [500, 502, 503, 504]:
        await status_msg.edit_text(
            "⚠️ **EAES Server Overloaded**\n\n"
            "Official examination servers are currently experiencing extreme traffic. Please retry in a moment with /start.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    detail = data.get("detail", "Result not found. Verify your admission number and name spelling.")
    await status_msg.edit_text(f"❌ **Error ({status_code})**\n\n{detail}\n\nSend /start to try again.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Action cancelled. Send /start whenever you want to check again.")
    return ConversationHandler.END


# ==================== MAIN ==================== #
if __name__ == "__main__":
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CallbackQueryHandler(handle_callback_verification, pattern="^verify_subscription$"),
        ],
        states={
            STATE_ADMISSION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admission_step)],
            STATE_FIRST_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_first_name_step)],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("cancel", cancel),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)

    print("🚀 EAES Step-by-Step Result Bot is active.")
    app.run_polling()
