"""
Proxy Checker Telegram Bot
===========================
Features:
- /check proxy1 proxy2 proxy3  -> check multiple proxies directly
- /proxy proxy1 proxy2 proxy3  -> check multiple proxies directly
- /proxy as a reply to a .txt file -> check all proxies in the file
- Concurrent checking with ThreadPoolExecutor
- Live proxies are returned as a .txt file
- Bot token is kept directly in this single file
- Supports:
    IP:PORT
    IP:PORT:USER:PASS
    USER:PASS@IP:PORT
    http://IP:PORT
    http://USER:PASS@IP:PORT

Requirements:
    pip install python-telegram-bot==20.7 requests
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Optional

import requests
from telegram import Update, Document
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)


# =============================================================================
# CONFIG
# =============================================================================

# Put your NEW Telegram bot token between the quotes.
# IMPORTANT: If the old token was shared publicly, revoke it in BotFather first.
BOT_TOKEN = "8531064839:AAFBrCMaGgJ559Eqs-NaGDMEbIbfg3oln9I"

TIMEOUT = 8
SINGLE_PROXY_TIMEOUT = 8
RETRY_COUNT = 1

THREAD_COUNT = 50
CHUNK = 200
MAX_PROXIES = 100000

# One lightweight endpoint is enough to determine whether the proxy can
# successfully make an outbound HTTPS request.
TEST_URL = "https://api.ipify.org"


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# =============================================================================
# PROXY FORMAT HELPERS
# =============================================================================

def mask_proxy(proxy: str) -> str:
    """Hide proxy passwords when displaying a proxy."""
    try:
        if "://" not in proxy:
            return proxy

        scheme, rest = proxy.split("://", 1)

        if "@" not in rest:
            return proxy

        auth, host = rest.rsplit("@", 1)

        if ":" not in auth:
            return proxy

        username = auth.split(":", 1)[0]
        return f"{scheme}://{username}:***@{host}"

    except Exception:
        return proxy


def auto_fix_proxy_format(raw_proxy: str) -> Optional[str]:
    """
    Normalize common proxy formats to:
        http://host:port
    or:
        http://user:pass@host:port
    """

    if not raw_proxy:
        return None

    proxy = raw_proxy.strip()

    if not proxy:
        return None

    # Common typo.
    if proxy.lower().startswith("hytp://"):
        proxy = "http://" + proxy[7:]

    # Detect scheme.
    protocol = "http"

    if "://" in proxy:
        protocol, core = proxy.split("://", 1)
        protocol = protocol.lower()

        if protocol not in ("http", "https"):
            return None
    else:
        core = proxy

    core = core.strip()

    if not core:
        return None

    # user:pass@host:port
    if "@" in core:
        auth, host_port = core.rsplit("@", 1)

        if ":" not in auth or ":" not in host_port:
            return None

        user, password = auth.split(":", 1)
        host, port = host_port.rsplit(":", 1)

        if not user or not password or not host or not port.isdigit():
            return None

        port_number = int(port)

        if not 1 <= port_number <= 65535:
            return None

        return f"{protocol}://{user}:{password}@{host}:{port}"

    # host:port:user:pass
    parts = core.split(":")

    if len(parts) >= 4 and parts[1].isdigit():
        host = parts[0]
        port = parts[1]
        user = parts[2]
        password = ":".join(parts[3:])

        if not host or not user or not password:
            return None

        port_number = int(port)

        if not 1 <= port_number <= 65535:
            return None

        return f"{protocol}://{user}:{password}@{host}:{port}"

    # host:port
    if len(parts) == 2 and parts[1].isdigit():
        host = parts[0]
        port = parts[1]

        if not host:
            return None

        port_number = int(port)

        if not 1 <= port_number <= 65535:
            return None

        return f"{protocol}://{host}:{port}"

    return None


def normalize_proxy_format(proxy: str) -> Optional[str]:
    return auto_fix_proxy_format(proxy)


def parse_proxy_list(text: str) -> list[str]:
    """Read proxies from text, one proxy per line."""
    proxies = []

    for line in text.splitlines():
        line = line.strip()

        if not line:
            continue

        if line.startswith("#"):
            continue

        proxies.append(line)

        if len(proxies) >= MAX_PROXIES:
            break

    return proxies


def parse_command_proxies(args: list[str]) -> list[str]:
    """
    Allows:
        /check 1.1.1.1:80 2.2.2.2:8080
        /proxy 1.1.1.1:80 2.2.2.2:8080

    Also accepts comma-separated input:
        /check 1.1.1.1:80,2.2.2.2:8080
    """

    proxies = []

    for arg in args:
        # Permit accidental commas between proxy values.
        for item in arg.split(","):
            item = item.strip()

            if item:
                proxies.append(item)

            if len(proxies) >= MAX_PROXIES:
                return proxies

    return proxies


# =============================================================================
# SINGLE PROXY CHECK
# =============================================================================

def check_single_proxy(raw_proxy: str) -> tuple[bool, str, str]:
    """
    Returns:
        (working, normalized_proxy, reason)
    """

    normalized = normalize_proxy_format(raw_proxy)

    if not normalized:
        return False, raw_proxy.strip(), "Invalid proxy format"

    last_error = "Unknown error"

    for attempt in range(RETRY_COUNT + 1):
        try:
            response = requests.get(
                TEST_URL,
                proxies={
                    "http": normalized,
                    "https": normalized,
                },
                timeout=SINGLE_PROXY_TIMEOUT,
            )

            if response.status_code == 200:
                return True, normalized, "HTTP 200"

            last_error = f"HTTP {response.status_code}"

        except requests.exceptions.ProxyError:
            last_error = "Proxy error"

        except requests.exceptions.ConnectTimeout:
            last_error = "Connect timeout"

        except requests.exceptions.ReadTimeout:
            last_error = "Read timeout"

        except requests.exceptions.SSLError:
            last_error = "SSL error"

        except requests.exceptions.ConnectionError:
            last_error = "Connection error"

        except requests.exceptions.RequestException as exc:
            last_error = f"Request error: {type(exc).__name__}"

        except Exception as exc:
            last_error = f"Error: {type(exc).__name__}"

        if attempt < RETRY_COUNT:
            time.sleep(0.20)

    return False, normalized, last_error


def check_proxy(raw_proxy: str) -> Optional[str]:
    """Compatibility helper: return normalized proxy only if live."""
    try:
        working, normalized, _reason = check_single_proxy(raw_proxy)

        if working:
            return normalized

    except Exception:
        pass

    return None


# =============================================================================
# CONCURRENT CHECKING
# =============================================================================

def check_proxy_batch(proxies: list[str]) -> list[tuple[str, bool, str, str]]:
    """
    Check a batch concurrently.

    Returns tuples:
        (original_proxy, working, normalized_proxy, reason)
    """

    if not proxies:
        return []

    results = [None] * len(proxies)

    workers = max(1, min(THREAD_COUNT, len(proxies)))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(check_single_proxy, proxy): index
            for index, proxy in enumerate(proxies)
        }

        for future in as_completed(future_map):
            index = future_map[future]
            original = proxies[index]

            try:
                working, normalized, reason = future.result()

            except Exception as exc:
                working = False
                normalized = original
                reason = f"Worker error: {type(exc).__name__}"

            results[index] = (
                original,
                working,
                normalized,
                reason,
            )

    return results


async def run_check_threaded(
    proxies: list[str],
) -> list[tuple[str, bool, str, str]]:
    """Run blocking proxy checks outside the asyncio event loop."""
    return await asyncio.to_thread(check_proxy_batch, proxies)


# =============================================================================
# TELEGRAM MESSAGE HELPERS
# =============================================================================

def build_progress_text(
    checked: int,
    total: int,
    live: int,
) -> str:

    pct = int((checked / total) * 100) if total else 0

    bar_len = 20
    filled = int(bar_len * pct / 100)

    bar = "█" * filled + "░" * (bar_len - filled)

    return (
        "🔍 *Proxy Check in Progress*\n"
        f"`[{bar}] {pct}%`\n\n"
        f"📦 Total   : `{total}`\n"
        f"✅ Live    : `{live}`\n"
        f"🔄 Checked : `{checked}`"
    )


def build_result_line(
    index: int,
    original: str,
    working: bool,
    normalized: str,
    reason: str,
) -> str:

    shown = mask_proxy(normalized or original)

    if working:
        return f"🟢 `{index}.` `{shown}` — *LIVE* ({reason})"

    return f"🔴 `{index}.` `{shown}` — *DEAD* ({reason})"


# =============================================================================
# COMMON CHECK JOB
# =============================================================================

async def process_proxy_list(
    update: Update,
    proxies: list[str],
    source_name: str = "proxy list",
):
    """
    Shared checker used by:
        /check proxy1 proxy2
        /proxy proxy1 proxy2
        /proxy as reply to a text file
    """

    msg = update.message

    if not proxies:
        await msg.reply_text("⚠️ No proxies were provided.")
        return

    proxies = proxies[:MAX_PROXIES]
    total = len(proxies)

    status = await msg.reply_text(
        f"📥 *{source_name}* received.\n"
        f"📦 Proxies: `{total}`\n"
        f"🧵 Threads: `{THREAD_COUNT}`\n"
        f"⏳ Starting check...",
        parse_mode="Markdown",
    )

    started = time.time()

    all_results = []
    checked = 0
    live_count = 0

    # Process in chunks so very large lists do not create an enormous
    # number of futures at once.
    for start in range(0, total, CHUNK):
        chunk = proxies[start:start + CHUNK]

        results = await run_check_threaded(chunk)

        all_results.extend(results)

        checked += len(chunk)
        live_count += sum(1 for result in results if result[1])

        try:
            await status.edit_text(
                build_progress_text(
                    checked=checked,
                    total=total,
                    live=live_count,
                ),
                parse_mode="Markdown",
            )
        except Exception:
            # Telegram edit limits/rate limits should not stop the checker.
            pass

    elapsed = max(time.time() - started, 0.001)

    live_results = [
        result for result in all_results if result[1]
    ]

    dead_count = total - len(live_results)

    speed = total / elapsed

    summary = (
        "✅ *Check Complete!*\n\n"
        f"📦 Total : `{total}`\n"
        f"🟢 Live  : `{len(live_results)}`\n"
        f"🔴 Dead  : `{dead_count}`\n"
        f"⏱ Time  : `{elapsed:.1f}s`\n"
        f"⚡ Speed : `{speed:.0f} proxies/sec`"
    )

    await status.edit_text(
        summary,
        parse_mode="Markdown",
    )

    # Send live proxies as a text file.
    if live_results:
        live_lines = []

        for _original, _working, normalized, _reason in live_results:
            live_lines.append(normalized)

        live_content = "\n".join(live_lines) + "\n"

        live_file = BytesIO(live_content.encode("utf-8"))
        live_file.name = f"live_proxies_{int(time.time())}.txt"

        await msg.reply_document(
            document=live_file,
            caption=(
                f"🟢 *Live Proxies:* `{len(live_results)}`\n"
                f"📦 *Checked:* `{total}`\n"
                f"⏱ *Time:* `{elapsed:.1f}s`"
            ),
            parse_mode="Markdown",
        )

    # For direct /check or /proxy arguments, also show a compact result list.
    # Keep it bounded so Telegram message limits are not exceeded.
    if total <= 50:
        lines = []

        for index, result in enumerate(all_results, start=1):
            lines.append(
                build_result_line(
                    index,
                    result[0],
                    result[1],
                    result[2],
                    result[3],
                )
            )

        result_text = "\n".join(lines)

        # Telegram messages have a length limit; split safely if necessary.
        max_len = 3800

        if len(result_text) <= max_len:
            await msg.reply_text(
                "📋 *Results*\n\n" + result_text,
                parse_mode="Markdown",
            )
        else:
            # Send only a safe-sized prefix.
            await msg.reply_text(
                "📋 *Results*\n\n" + result_text[:max_len],
                parse_mode="Markdown",
            )

    else:
        await msg.reply_text(
            f"📋 Detailed result list contains `{total}` entries.\n"
            "🟢 Live proxies have been sent in the `.txt` file above.",
            parse_mode="Markdown",
        )


# =============================================================================
# COMMAND HANDLERS
# =============================================================================

async def start_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await update.message.reply_text(
        "🤖 *Proxy Checker Bot*\n\n"
        "Commands:\n"
        "• `/check proxy1 proxy2 proxy3`\n"
        "• `/proxy proxy1 proxy2 proxy3`\n"
        "• Reply `/proxy` to a `.txt` proxy file\n\n"
        "*Supported formats:*\n"
        "• `IP:PORT`\n"
        "• `IP:PORT:USER:PASS`\n"
        "• `USER:PASS@IP:PORT`\n"
        "• `http://IP:PORT`\n"
        "• `http://USER:PASS@IP:PORT`\n\n"
        "*Example:*\n"
        "`/check 1.1.1.1:80 2.2.2.2:8080 3.3.3.3:3128`\n\n"
        "The bot checks proxies concurrently and sends live proxies as a `.txt` file.",
        parse_mode="Markdown",
    )


async def help_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await update.message.reply_text(
        "🛠 *Commands*\n\n"
        "`/start` — Bot information\n"
        "`/help` — Help\n\n"
        "`/check proxy1 proxy2 proxy3`\n"
        "Check multiple proxies directly.\n\n"
        "`/proxy proxy1 proxy2 proxy3`\n"
        "Check multiple proxies directly.\n\n"
        "Or reply `/proxy` to a `.txt` file to check its contents.\n\n"
        "*Settings*\n"
        f"Threads: `{THREAD_COUNT}`\n"
        f"Chunk: `{CHUNK}`\n"
        f"Timeout: `{SINGLE_PROXY_TIMEOUT}s`\n"
        f"Retries: `{RETRY_COUNT}`\n"
        f"Max proxies: `{MAX_PROXIES}`",
        parse_mode="Markdown",
    )


async def check_command_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    """
    Supports:
        /check proxy1
        /check proxy1 proxy2 proxy3
    """

    args = context.args or []

    proxies = parse_command_proxies(args)

    if not proxies:
        await update.message.reply_text(
            "⚠️ Usage:\n\n"
            "`/check IP:PORT`\n"
            "`/check proxy1 proxy2 proxy3`\n\n"
            "Example:\n"
            "`/check 1.1.1.1:80 2.2.2.2:8080`",
            parse_mode="Markdown",
        )
        return

    await process_proxy_list(
        update,
        proxies,
        source_name="Direct proxy list",
    )


async def proxy_command_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    """
    Two modes:

    1) Direct:
       /proxy proxy1 proxy2 proxy3

    2) File:
       Reply /proxy to a .txt document.
    """

    args = context.args or []

    # -------------------------------------------------------------------------
    # MODE 1: /proxy proxy1 proxy2 proxy3
    # -------------------------------------------------------------------------

    if args:
        proxies = parse_command_proxies(args)

        if not proxies:
            await update.message.reply_text(
                "⚠️ No valid proxy arguments found."
            )
            return

        await process_proxy_list(
            update,
            proxies,
            source_name="Direct proxy list",
        )
        return

    # -------------------------------------------------------------------------
    # MODE 2: reply /proxy to a .txt file
    # -------------------------------------------------------------------------

    msg = update.message

    if not msg.reply_to_message:
        await msg.reply_text(
            "⚠️ Use one of these formats:\n\n"
            "`/proxy proxy1 proxy2 proxy3`\n\n"
            "OR reply `/proxy` to a `.txt` proxy file.",
            parse_mode="Markdown",
        )
        return

    replied = msg.reply_to_message
    doc: Optional[Document] = replied.document

    if not doc:
        await msg.reply_text(
            "⚠️ The replied message does not contain a document."
        )
        return

    file_name = (doc.file_name or "").lower()

    if not file_name.endswith(".txt") and doc.mime_type not in (
        "text/plain",
        "application/octet-stream",
    ):
        await msg.reply_text(
            "⚠️ Only `.txt` proxy files are supported."
        )
        return

    # 10 MB file limit.
    if doc.file_size and doc.file_size > 10 * 1024 * 1024:
        await msg.reply_text(
            "⚠️ File too large. Maximum supported size is 10 MB."
        )
        return

    status = await msg.reply_text(
        "📥 Downloading proxy file..."
    )

    try:
        tg_file = await doc.get_file()

        file_bytes = await tg_file.download_as_bytearray()

        raw_text = file_bytes.decode(
            "utf-8",
            errors="ignore",
        )

    except Exception as exc:
        logger.exception("File download failed")

        await status.edit_text(
            f"❌ File download failed: `{type(exc).__name__}`",
            parse_mode="Markdown",
        )
        return

    proxies = parse_proxy_list(raw_text)

    if not proxies:
        await status.edit_text(
            "⚠️ No proxies found in the `.txt` file."
        )
        return

    # Remove the temporary download message.
    try:
        await status.delete()
    except Exception:
        pass

    await process_proxy_list(
        update,
        proxies,
        source_name="TXT proxy file",
    )


# =============================================================================
# DOCUMENT HINT
# =============================================================================

async def document_hint_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    """Tell users how to start checking a text file."""

    doc = update.message.document

    if not doc:
        return

    if (doc.file_name or "").lower().endswith(".txt"):
        await update.message.reply_text(
            "📎 Proxy `.txt` file received!\n\n"
            "Reply to this file with:\n"
            "`/proxy`\n\n"
            "Or use direct checking:\n"
            "`/proxy proxy1 proxy2 proxy3`",
            parse_mode="Markdown",
        )


# =============================================================================
# MAIN
# =============================================================================

def main():
    token = BOT_TOKEN.strip()

    if not token or token == "PASTE_YOUR_NEW_BOT_TOKEN_HERE":
        print(
            "\nERROR: BOT_TOKEN is not configured.\n"
            "Open this file and put your Telegram bot token in:\n\n"
            'BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"\n'
        )
        return

    while True:
        try:
            app = (
                Application.builder()
                .token(token)
                .build()
            )

            app.add_handler(
                CommandHandler(
                    "start",
                    start_handler,
                )
            )

            app.add_handler(
                CommandHandler(
                    "help",
                    help_handler,
                )
            )

            app.add_handler(
                CommandHandler(
                    "check",
                    check_command_handler,
                )
            )

            app.add_handler(
                CommandHandler(
                    "proxy",
                    proxy_command_handler,
                )
            )

            app.add_handler(
                MessageHandler(
                    filters.Document.ALL & ~filters.COMMAND,
                    document_hint_handler,
                )
            )

            logger.info(
                "Proxy Checker Bot is running..."
            )

            app.run_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES,
            )

            break

        except KeyboardInterrupt:
            logger.info("Bot stopped by user.")
            break

        except Exception:
            logger.exception(
                "Bot crashed; restarting in 5 seconds..."
            )
            time.sleep(5)


if __name__ == "__main__":
    main()
