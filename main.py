"""
Proxy Checker Telegram Bot
===========================
- Multi-user concurrent support via asyncio + ThreadPoolExecutor
- /proxy command: reply to a .txt file containing proxy list
- Checks each proxy against ipify.org
- Returns live count + sends live proxies as a .txt file
- Threads: configurable via THREAD_COUNT

Requirements:
    pip install python-telegram-bot aiohttp requests

Usage:
    Set BOT_TOKEN below, then: python proxy_checker_bot.py
"""

import asyncio
import logging
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Optional

import requests
from telegram import Update, Document
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ─── CONFIG ──────────────────────────────────────────────────────────────────

BOT_TOKEN = os.getenv("BOT_TOKEN")       # ← paste your bot token here
IP_CHECK_ENDPOINTS = [
    "https://api.ipify.org?format=json",
    "https://api.ipify.org",
    "https://api64.ipify.org?format=json",
    "https://api64.ipify.org",
    "https://api6.ipify.org?format=json",
    "https://api6.ipify.org",
]
TIMEOUT      = 8                            # proxy check timeout (seconds)
THREAD_COUNT = 50                           # concurrent threads per check job
MAX_PROXIES  = 99999999                        # max proxies accepted per file

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# Runtime settings
RETRY_COUNT = 0
SINGLE_PROXY_TIMEOUT = 6

def mask_proxy(proxy: str) -> str:
    """Hide proxy credentials in logs/messages."""
    try:
        if "@" in proxy and "://" in proxy:
            scheme, rest = proxy.split("://", 1)
            auth, host = rest.rsplit("@", 1)
            if ":" in auth:
                user = auth.split(":", 1)[0]
                return f"{scheme}://{user}:***@{host}"
        return proxy
    except Exception:
        return proxy


def normalize_proxy_format(raw_proxy: str) -> Optional[str]:
    """
    Normalize supported proxy formats into a requests-compatible URL:
        IP:Port              -> http://IP:Port
        IP:Port:User:Pass    -> http://User:Pass@IP:Port
        User:Pass@IP:Port    -> http://User:Pass@IP:Port
        http://...           -> unchanged
    Returns None if the input is not a recognized proxy format.
    """
    if not raw_proxy:
        return None

    raw = raw_proxy.strip()
    if not raw:
        return None

    # Already has a scheme (http/https/socks4/socks5)
    if "://" in raw:
        return raw

    # user:pass@host:port
    if "@" in raw:
        return f"http://{raw}"

    parts = raw.split(":")

    # host:port
    if len(parts) == 2:
        host, port = parts
        if host and port.isdigit():
            return f"http://{host}:{port}"
        return None

    # host:port:user:pass
    if len(parts) == 4:
        host, port, user, pw = parts
        if host and port.isdigit():
            return f"http://{user}:{pw}@{host}:{port}"
        return None

    return None


def check_single_proxy(raw_proxy: str) -> tuple[bool, str, str]:
    """
    Check one proxy against multiple public IP endpoints.
    The first successful endpoint marks the proxy LIVE.
    Returns: (working, normalized_proxy, reason)
    """
    normalized = normalize_proxy_format(raw_proxy)
    if not normalized:
        return False, raw_proxy.strip(), "Invalid proxy format"

    last_error = "Unknown error"

    for endpoint in IP_CHECK_ENDPOINTS:
        try:
            response = requests.get(
                endpoint,
                proxies={"http": normalized, "https": normalized},
                timeout=SINGLE_PROXY_TIMEOUT,
            )

            if response.status_code == 200:
                return True, normalized, f"HTTP 200 via {endpoint}"

            last_error = f"HTTP {response.status_code} via {endpoint}"

        except requests.exceptions.ProxyError:
            last_error = f"Proxy error via {endpoint}"
        except requests.exceptions.ConnectTimeout:
            last_error = f"Connect timeout via {endpoint}"
        except requests.exceptions.ReadTimeout:
            last_error = f"Read timeout via {endpoint}"
        except requests.exceptions.SSLError:
            last_error = f"SSL error via {endpoint}"
        except requests.exceptions.ConnectionError:
            last_error = f"Connection error via {endpoint}"
        except requests.exceptions.RequestException as exc:
            last_error = f"Request error: {type(exc).__name__}"
        except Exception as exc:
            last_error = f"Error: {type(exc).__name__}"

    return False, normalized, last_error


def check_proxy(raw_proxy: str) -> Optional[str]:
    """Returns normalized proxy string if live, else None; never raises."""
    try:
        working, normalized, _reason = check_single_proxy(raw_proxy)
        return normalized if working else None
    except Exception:
        return None


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def parse_proxy_list(text: str) -> list[str]:
    """Extract one proxy per line, drop blanks and comments."""
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            lines.append(line)
    return lines[:MAX_PROXIES]


async def run_check_threaded(proxies: list[str]) -> list[str]:
    """
    Checks proxies concurrently using ThreadPoolExecutor.
    Returns list of live (normalized) proxies.
    """
    loop = asyncio.get_event_loop()
    live = []

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = {
            loop.run_in_executor(executor, check_proxy, p): p
            for p in proxies
        }
        results = await asyncio.gather(*futures.keys(), return_exceptions=True)

    for result in results:
        if isinstance(result, str) and result:
            live.append(result)

    return live


def build_progress_text(checked: int, total: int, live: int) -> str:
    pct     = int((checked / total) * 100) if total else 0
    bar_len = 20
    filled  = int(bar_len * pct / 100)
    bar     = "█" * filled + "░" * (bar_len - filled)
    return (
        f"🔍 *Proxy Check in Progress*\n"
        f"`[{bar}] {pct}%`\n\n"
        f"📦 Total   : `{total}`\n"
        f"✅ Live    : `{live}`\n"
        f"🔄 Checked : `{checked}`"
    )

# ─── COMMAND HANDLERS ────────────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Proxy Checker Bot\n\n"
        "How to use:\n"
        "1. Send a .txt file containing your proxy list\n"
        "2. Reply to that file with /proxy\n"
        "3. Or check one or multiple proxies directly with /check proxy1 proxy2 ...\n\n"
        "Multi-user supported | Fast threaded checking\n\n"
        "Supported proxy formats:\n"
        "IP:Port\n"
        "IP:Port:User:Pass\n"
        "User:Pass@IP:Port\n"
        "http://User:Pass@IP:Port"
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Commands:\n\n"
        "/start - Bot info and usage guide\n"
        "/help - This message\n"
        "/check <proxy1> [proxy2] [proxy3] ... - Check multiple proxies\n"
        "/proxy - Reply to a proxy .txt file to check multiple proxies\n\n"
        "Current Settings:\n"
        f"- Threads: {THREAD_COUNT}\n"
        f"- Timeout: {TIMEOUT}s per proxy\n"
        f"- Max file: {MAX_PROXIES} proxies"
        f"\n- Check endpoints: {len(IP_CHECK_ENDPOINTS)}"
    )


def parse_proxy_arguments(args: list[str]) -> list[str]:
    """Parse /check arguments into individual proxy strings."""
    if not args:
        return []
    values = []
    for arg in args:
        for item in re.split(r"[,;]+", arg.strip()):
            if item.strip():
                values.append(item.strip())
    seen=set(); result=[]
    for item in values:
        k=item.lower()
        if k not in seen:
            seen.add(k); result.append(item)
    return result[:MAX_PROXIES]


async def check_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /check proxy
    /check proxy1 proxy2 proxy3 ...

    Uses the same ipify + requests proxy-checking logic as the original bot.
    """
    msg = update.message
    raws = parse_proxy_arguments(context.args or [])

    if not raws:
        await msg.reply_text(
            "Usage:\n"
            "/check IP:Port\n"
            "/check IP:Port:USER:PASS\n"
            "/check USER:PASS@IP:PORT\n"
            "/check proxy1 proxy2 proxy3 ..."
        )
        return

    status = await msg.reply_text(f"🔍 Checking {len(raws)} proxy(s)...")

    async def check_one(raw: str):
        return await asyncio.to_thread(check_single_proxy, raw)

    try:
        results = await asyncio.gather(
            *(check_one(raw) for raw in raws),
            return_exceptions=True,
        )

        lines = []
        live_proxies = []

        for idx, (raw, result) in enumerate(zip(raws, results), 1):
            if isinstance(result, Exception):
                working = False
                normalized = normalize_proxy_format(raw) or raw
                reason = type(result).__name__
            else:
                working, normalized, reason = result

            shown = normalized or raw

            if working:
                live_proxies.append(normalized)
                lines.append(f"🟢 {idx}. {shown} — LIVE ({reason})")
            else:
                lines.append(f"🔴 {idx}. {shown} — DEAD ({reason})")

        chunks = []
        current = ""
        for line in lines:
            candidate = f"{current}\n{line}" if current else line
            if len(candidate) > 3800:
                if current:
                    chunks.append(current)
                current = line
            else:
                current = candidate
        if current:
            chunks.append(current)

        await status.edit_text(
            f"✅ Check complete\n\n"
            f"📦 Total: {len(raws)}\n"
            f"🟢 Live: {len(live_proxies)}\n"
            f"🔴 Dead: {len(raws) - len(live_proxies)}"
        )

        for chunk in chunks:
            await msg.reply_text(chunk)

        if live_proxies:
            live_file = BytesIO("\n".join(live_proxies).encode("utf-8"))
            live_file.name = f"live_proxies_{msg.from_user.id}_{int(time.time())}.txt"
            await msg.reply_document(
                document=live_file,
                caption=f"🟢 Live proxies: {len(live_proxies)}"
            )

    except Exception as e:
        logger.exception("Multi-proxy check failed")
        await status.edit_text(f"❌ Check failed safely: {type(e).__name__}")


async def proxy_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /proxy — must be sent as a reply to a message containing a .txt document.
    """
    msg = update.message

    # Must be a reply
    if not msg.reply_to_message:
        await msg.reply_text(
            "⚠️ Send a `.txt` proxy file first, then *reply to that file* with `/proxy`.",
            parse_mode="Markdown",
        )
        return

    replied = msg.reply_to_message
    doc: Optional[Document] = replied.document

    if not doc:
        await msg.reply_text(
            "⚠️ No file found in the replied message. Reply to a `.txt` proxy file."
        )
        return

    # Must be a text file
    if not (doc.file_name or "").lower().endswith(".txt") and \
       doc.mime_type not in ("text/plain", "application/octet-stream"):
        await msg.reply_text("⚠️ Only `.txt` files are supported.")
        return

    # File size guard (10 MB)
    if doc.file_size and doc.file_size > 10 * 1024 * 1024:
        await msg.reply_text("⚠️ File too large. Maximum size is 10 MB.")
        return

    user_id   = msg.from_user.id
    user_name = msg.from_user.first_name or str(user_id)

    # Acknowledge
    status_msg = await msg.reply_text(
        f"📥 File received, *{user_name}*!\nDownloading proxy list...",
        parse_mode="Markdown",
    )

    # Download file content
    try:
        tg_file    = await doc.get_file()
        file_bytes = await tg_file.download_as_bytearray()
        raw_text   = file_bytes.decode("utf-8", errors="ignore")
    except Exception as e:
        await status_msg.edit_text(f"❌ File download failed: {e}")
        return

    proxies = parse_proxy_list(raw_text)
    total   = len(proxies)

    if total == 0:
        await status_msg.edit_text("⚠️ No valid proxies found in this file.")
        return

    await status_msg.edit_text(
        f"⚡ Found *{total}* proxies!\n"
        f"🧵 Starting check with {THREAD_COUNT} threads...",
        parse_mode="Markdown",
    )

    start_time = time.time()

    # ── Chunked checking with live progress updates ──
    CHUNK        = 500
    live_proxies: list[str] = []
    checked      = 0

    for i in range(0, total, CHUNK):
        chunk   = proxies[i : i + CHUNK]
        results = await run_check_threaded(chunk)
        live_proxies.extend(results)
        checked += len(chunk)

        try:
            await status_msg.edit_text(
                build_progress_text(checked, total, len(live_proxies)),
                parse_mode="Markdown",
            )
        except Exception:
            pass   # edit rate-limit — skip silently

    elapsed  = time.time() - start_time
    live_cnt = len(live_proxies)
    dead_cnt = total - live_cnt

    # ── Final summary ──
    summary = (
        f"✅ *Check Complete!*\n\n"
        f"👤 User    : *{user_name}*\n"
        f"📦 Total   : `{total}`\n"
        f"🟢 Live    : `{live_cnt}`\n"
        f"🔴 Dead    : `{dead_cnt}`\n"
        f"⏱ Time    : `{elapsed:.1f}s`\n"
        f"⚡ Speed   : `{total / elapsed:.0f}` proxies/sec"
    )

    await status_msg.edit_text(summary, parse_mode="Markdown")

    # ── Send live proxies as .txt file ──
    if live_proxies:
        live_content = "\n".join(live_proxies).encode("utf-8")
        live_file    = BytesIO(live_content)
        live_file.name = f"live_proxies_{user_id}_{int(time.time())}.txt"

        await msg.reply_document(
            document=live_file,
            caption=(
                f"🟢 *Live Proxies* — `{live_cnt}` found\n"
                f"⏱ Checked `{total}` proxies in `{elapsed:.1f}s`"
            ),
            parse_mode="Markdown",
        )
    else:
        await msg.reply_text("😔 No live proxies found in this file.")


# ─── FILE HINT HANDLER ────────────────────────────────────────────────────────

async def document_hint_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """When user sends a .txt file without /proxy, nudge them."""
    doc = update.message.document
    if doc and (doc.file_name or "").lower().endswith(".txt"):
        await update.message.reply_text(
            "📎 Proxy file received! Now *reply to this file* with `/proxy` to start checking.",
            parse_mode="Markdown",
        )

# ─── MAIN ────────────────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error

    # Handle Telegram 409 Conflict — another instance is polling with same token.
    # Stop this instance cleanly to avoid spamming the log/retry loop.
    if isinstance(err, Conflict):
        logger.error(
            "Conflict: another bot instance is already polling with this token. "
            "Stopping this instance."
        )
        try:
            await context.application.stop_running()
        except Exception:
            pass
        return

    logger.error("Unhandled update error: %s", err, exc_info=err)


async def _post_init(application: Application):
    """Ensure no webhook is set before polling to avoid startup conflicts."""
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        # Give Telegram a moment to release any previous long-poll connection.
        await asyncio.sleep(2)
    except Exception as exc:
        logger.warning("delete_webhook failed: %s", exc)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is not set")

    try:
        app = (
            Application.builder()
            .token(BOT_TOKEN)
            .post_init(_post_init)
            .build()
        )

        app.add_handler(CommandHandler("start", start_handler))
        app.add_handler(CommandHandler("help", help_handler))
        app.add_handler(CommandHandler("check", check_command_handler))
        app.add_handler(CommandHandler("proxy", proxy_command_handler))
        app.add_handler(
            MessageHandler(
                filters.Document.ALL & ~filters.COMMAND,
                document_hint_handler,
            )
        )
        app.add_error_handler(error_handler)

        logger.info("Bot is running...")
        app.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )

    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception:
        logger.exception("Bot stopped due to an unrecoverable error.")


if __name__ == "__main__":
    main()
