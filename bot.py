"""
YouTube Shorts Scheduler Bot
-----------------------------
Telegram bot that lets you upload videos, add a caption and schedule date/time,
then automatically posts them to your YouTube channel as Shorts.

Also supports auto-reposting from a source YouTube channel daily.

FLOW (manual):
  1. Send a video OR a YouTube URL to the bot
  2. Bot asks for title/caption
  3. Bot asks for schedule date/time (or "now")
  4. At scheduled time, bot uploads to YouTube Shorts automatically

FLOW (auto-repost):
  1. /setsource <YouTube channel URL>
  2. /settime <HH:MM>  (daily repost time, e.g. 20:00)
  3. /autorepost on
  4. Bot downloads latest video from source channel daily and posts it

ENV VARS:
  BOT_TOKEN=              Telegram bot token
  ADMIN_ID=               Your Telegram user ID
  YOUTUBE_CLIENT_SECRET=  Path to client_secret JSON file (or inline JSON string)
  YOUTUBE_TOKEN_FILE=     Path to store OAuth token (default: token.json)
  YOUTUBE_TOKEN=          Full token JSON string (for Render, overrides file)
  DATABASE_URL=           PostgreSQL connection string (Neon)
  PORT=                   HTTP port for Render (default: 10000)
"""

import asyncio
import json
import os
import logging
import tempfile
import subprocess
import aiohttp
from aiohttp import web
from datetime import datetime, time as dtime
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    ContextTypes, filters
)

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import psycopg

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN             = os.environ["BOT_TOKEN"]
ADMIN_ID              = int(os.environ.get("ADMIN_ID", "0"))
DATABASE_URL          = os.environ["DATABASE_URL"]
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET", "./client_secret.json")
YOUTUBE_TOKEN_FILE    = os.environ.get("YOUTUBE_TOKEN_FILE", "./token.json")

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# Conversation states
WAIT_VIDEO, WAIT_TITLE, WAIT_SCHEDULE = range(3)


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg.connect(DATABASE_URL)


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_videos (
                id SERIAL PRIMARY KEY,
                file_id TEXT,
                source_url TEXT,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                scheduled_at TIMESTAMP NOT NULL,
                posted BOOLEAN DEFAULT FALSE,
                posted_at TIMESTAMP,
                youtube_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.commit()


def get_setting(key: str, default: str = "") -> str:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT value FROM bot_settings WHERE key=%s", (key,)
        ).fetchone()
    return row[0] if row else default


def set_setting(key: str, value: str):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO bot_settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        """, (key, value))
        conn.commit()


# ── YouTube auth ──────────────────────────────────────────────────────────────

def get_youtube_service():
    creds = None
    token_env = os.environ.get("YOUTUBE_TOKEN", "")
    if token_env.strip().startswith("{"):
        creds = Credentials.from_authorized_user_info(json.loads(token_env), SCOPES)
    elif os.path.exists(YOUTUBE_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(YOUTUBE_TOKEN_FILE, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        if token_env.strip().startswith("{"):
            os.environ["YOUTUBE_TOKEN"] = creds.to_json()
        else:
            with open(YOUTUBE_TOKEN_FILE, "w") as f:
                f.write(creds.to_json())

    if not creds or not creds.valid:
        raise RuntimeError("YouTube not authorized.")

    return build("youtube", "v3", credentials=creds)


# ── Download from YouTube URL ─────────────────────────────────────────────────

async def download_youtube_video(url: str, output_path: str) -> str:
    """Download a YouTube video using yt-dlp. Returns the actual file path."""
    loop = asyncio.get_event_loop()

    def _download():
        cmd = [
            "yt-dlp",
            "--format", "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "--merge-output-format", "mp4",
            "--output", output_path,
            "--no-playlist",
            url
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            raise RuntimeError(f"yt-dlp failed: {result.stderr}")
        return output_path

    return await loop.run_in_executor(None, _download)


async def get_channel_latest_video(channel_url: str) -> tuple[str, str]:
    """Get the latest video URL and title from a YouTube channel."""
    loop = asyncio.get_event_loop()

    def _fetch():
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--playlist-end", "5",
            "--print", "%(id)s\t%(title)s",
            channel_url
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(f"yt-dlp channel fetch failed: {result.stderr}")
        lines = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
        if not lines:
            raise RuntimeError("No videos found on channel")
        # Return first (latest) video
        parts = lines[0].split("\t", 1)
        video_id = parts[0]
        title = parts[1] if len(parts) > 1 else "Untitled"
        return f"https://www.youtube.com/watch?v={video_id}", title

    return await loop.run_in_executor(None, _fetch)


# ── Upload to YouTube ─────────────────────────────────────────────────────────

async def upload_to_youtube(video_path: str, title: str, description: str = "") -> str:
    loop = asyncio.get_event_loop()

    def _upload():
        youtube = get_youtube_service()
        body = {
            "snippet": {
                "title": title,
                "description": description + "\n\n#Shorts",
                "tags": ["Shorts"],
                "categoryId": "22",
            },
            "status": {
                "privacyStatus": "public",
                "selfDeclaredMadeForKids": False,
            },
        }
        media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        response = None
        while response is None:
            _, response = request.next_chunk()
        return response["id"]

    video_id = await loop.run_in_executor(None, _upload)
    return video_id


# ── Scheduler loop ────────────────────────────────────────────────────────────

async def scheduler_loop(bot):
    """Check every minute for videos due to be posted."""
    while True:
        try:
            with get_conn() as conn:
                rows = conn.execute("""
                    SELECT id, file_id, source_url, title, description
                    FROM scheduled_videos
                    WHERE posted = FALSE AND scheduled_at <= NOW()
                    ORDER BY scheduled_at ASC
                """).fetchall()

            for row_id, file_id, source_url, title, description in rows:
                logger.info(f"[scheduler] Processing video id={row_id} title={title}")
                tmp_path = None
                try:
                    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".mp4")
                    os.close(tmp_fd)

                    if source_url:
                        # Download from YouTube URL
                        await download_youtube_video(source_url, tmp_path)
                    elif file_id:
                        # Download from Telegram file
                        file = await bot.get_file(file_id)
                        await file.download_to_drive(tmp_path)
                    else:
                        raise RuntimeError("No file_id or source_url")

                    yt_id = await upload_to_youtube(tmp_path, title, description or "")

                    with get_conn() as conn:
                        conn.execute("""
                            UPDATE scheduled_videos
                            SET posted=TRUE, posted_at=NOW(), youtube_id=%s
                            WHERE id=%s
                        """, (yt_id, row_id))
                        conn.commit()

                    await bot.send_message(
                        ADMIN_ID,
                        f"✅ ቪዲዮ ተለቀቀ!\n\n📹 {title}\n🔗 https://youtube.com/shorts/{yt_id}"
                    )
                    logger.info(f"[scheduler] ✅ Posted: https://youtube.com/shorts/{yt_id}")

                except Exception as e:
                    logger.error(f"[scheduler] ❌ Failed id={row_id}: {e}")
                    with get_conn() as conn:
                        conn.execute(
                            "UPDATE scheduled_videos SET posted=TRUE, youtube_id=%s WHERE id=%s",
                            (f"ERROR: {e}", row_id)
                        )
                        conn.commit()
                    await bot.send_message(ADMIN_ID, f"❌ ቪዲዮ መለቀቅ አልተቻለም:\n{title}\n\nስህተት: {e}")
                finally:
                    if tmp_path and os.path.exists(tmp_path):
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass

        except Exception as e:
            logger.error(f"[scheduler] DB error: {e}")

        await asyncio.sleep(60)


# ── Auto-repost loop ──────────────────────────────────────────────────────────

async def auto_repost_loop(bot):
    """Daily auto-repost rotating through all source channels."""
    while True:
        try:
            enabled     = get_setting("autorepost", "off")
            repost_time = get_setting("repost_time", "20:00")
            channels    = get_source_channels()

            if enabled == "on" and channels:
                now = datetime.now()
                repost_times = get_setting("repost_times", "20:00").split(",")
                now_hm = (now.hour, now.minute)

                # Check if current time matches any scheduled time
                matched = any(
                    now_hm == tuple(map(int, t.strip().split(":")))
                    for t in repost_times if ":" in t
                )

                # Use time-slot key so each slot fires independently
                slot_key = f"last_repost_{now.hour:02d}{now.minute:02d}"
                today_str   = now.strftime("%Y-%m-%d")
                last_repost = get_setting(slot_key, "")

                if matched and last_repost != today_str:
                        logger.info(f"[autorepost] Time slot fired! {now.hour:02d}:{now.minute:02d}")

                        # Each time slot gets its own channel from the rotation
                        # slot index = which position this time is in the times list
                        repost_times_list = [t.strip() for t in repost_times if ":" in t]
                        now_str  = f"{now.hour:02d}:{now.minute:02d}"
                        try:
                            slot_idx = repost_times_list.index(now_str)
                        except ValueError:
                            slot_idx = 0

                        # Pick channel by slot index (wraps around if fewer channels than times)
                        ch_idx = slot_idx % len(channels)
                        source = channels[ch_idx]

                        try:
                            video_url, title = await get_channel_latest_video(source)

                            with get_conn() as conn:
                                existing = conn.execute(
                                    "SELECT id FROM scheduled_videos WHERE source_url=%s",
                                    (video_url,)
                                ).fetchone()

                            if existing:
                                logger.info(f"[autorepost] Already reposted {video_url}, skipping")
                                await bot.send_message(
                                    ADMIN_ID,
                                    f"ℹ️ Slot {slot_idx+1} ({now_str}): {source} — አዲስ ቪዲዮ የለም፣ ተዘሏል።"
                                )
                            else:
                                with get_conn() as conn:
                                    conn.execute(
                                        "INSERT INTO scheduled_videos (source_url, title, scheduled_at) VALUES (%s, %s, NOW())",
                                        (video_url, title)
                                    )
                                    conn.commit()

                                set_setting(slot_key, today_str)

                                await bot.send_message(
                                    ADMIN_ID,
                                    f"🔄 Auto-repost ተጀምሯል!\n\n"
                                    f"🕐 ሰዓት: {now_str} (slot {slot_idx+1})\n"
                                    f"📡 Channel {ch_idx+1}: {source}\n"
                                    f"📹 {title}\n\n"
                                    f"ቦቱ አሁን ያወርደዋል እና ይለቅቃል..."
                                )
                                logger.info(f"[autorepost] Slot {slot_idx+1} → Channel {ch_idx+1}: {title}")

                        except Exception as e:
                            logger.error(f"[autorepost] Error: {e}")
                            await bot.send_message(ADMIN_ID, f"❌ Auto-repost ስህተት (slot {slot_idx+1}):\n{e}")

        except Exception as e:
            logger.error(f"[autorepost] Loop error: {e}")

        await asyncio.sleep(60)


# ── Telegram handlers ─────────────────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ ይህ ቦት ለግል አጠቃቀም ብቻ ነው።")
        return
    await update.message.reply_text(
        "📹 YouTube Shorts Scheduler Bot\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📤 *ቪዲዮ ለማስቀጠር:*\n"
        "• ቪዲዮ ፋይል ይላኩ\n"
        "• ወይም YouTube URL ይላኩ\n\n"
        "🔄 *Auto-repost:*\n"
        "/setsource <channel URL>\n"
        "/settime <HH:MM> (ለምሳሌ: 20:00)\n"
        "/autorepost on\n\n"
        "📋 *ሌሎች ትዕዛዞች:*\n"
        "/list — ቀጠሮዎችን ይመልከቱ\n"
        "/status — የ auto-repost ሁኔታ\n"
        "/cancel — ይቅር\n"
        "━━━━━━━━━━━━━━━━━━",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="Markdown"
    )


async def receive_video(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END

    video = update.message.video or update.message.document
    if not video:
        await update.message.reply_text("ቪዲዮ ብቻ ይላኩ።")
        return WAIT_VIDEO

    ctx.user_data["file_id"] = video.file_id
    ctx.user_data["source_url"] = None
    await update.message.reply_text("✅ ቪዲዮ ተቀብሏል!\n\n📝 የቪዲዮውን ርዕስ ያስፍሩ:")
    return WAIT_TITLE


async def receive_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle YouTube URL sent as a message."""
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END

    url = update.message.text.strip()
    ctx.user_data["source_url"] = url
    ctx.user_data["file_id"] = None

    await update.message.reply_text(
        f"✅ YouTube URL ተቀብሏል!\n\n"
        f"🔗 {url}\n\n"
        f"📝 የቪዲዮውን ርዕስ ያስፍሩ (ወይም ✅ ለዋናው ርዕስ ይጠቀሙ):"
    )
    return WAIT_TITLE


async def receive_title(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["title"] = update.message.text.strip()

    keyboard = ReplyKeyboardMarkup(
        [["አሁን ይለቀቅ"]],
        resize_keyboard=True, one_time_keyboard=True
    )
    await update.message.reply_text(
        "🕐 መቼ ይለቀቅ?\n\n"
        "• **አሁን ይለቀቅ** ለወዲያውኑ\n"
        "• ወይም ቀን እና ሰዓት ያስፍሩ:\n"
        "  ቅርጸት: `2026-11-15 20:00`",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    return WAIT_SCHEDULE


async def receive_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "አሁን ይለቀቅ":
        scheduled_at = datetime.now()
    else:
        try:
            scheduled_at = datetime.strptime(text, "%Y-%m-%d %H:%M")
        except ValueError:
            await update.message.reply_text(
                "❌ ቅርጸት ትክክል አይደለም። እንደዚህ ያስፍሩ: `2026-11-15 20:00`\nወይም **አሁን ይለቀቅ** ይምረጡ።",
                parse_mode="Markdown"
            )
            return WAIT_SCHEDULE

    file_id    = ctx.user_data.get("file_id")
    source_url = ctx.user_data.get("source_url")
    title      = ctx.user_data["title"]

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO scheduled_videos (file_id, source_url, title, scheduled_at) VALUES (%s, %s, %s, %s)",
            (file_id, source_url, title, scheduled_at)
        )
        conn.commit()

    time_str = "አሁን ወዲያውኑ" if text == "አሁን ይለቀቅ" else scheduled_at.strftime("%Y-%m-%d %H:%M")

    await update.message.reply_text(
        f"✅ ቀጠሮ ተይዟል!\n\n📹 {title}\n🕐 {time_str}\n\n"
        f"ቦቱ በተቀናበረው ሰዓት ወደ YouTube Shorts ይለቅቃል።",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ ተሰርዟል።", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


async def list_scheduled(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT title, scheduled_at, posted, youtube_id, source_url
            FROM scheduled_videos
            ORDER BY scheduled_at DESC LIMIT 20
        """).fetchall()
    if not rows:
        await update.message.reply_text("ምንም ቀጠሮ የለም።")
        return
    lines = ["📋 የቀጠሮ ዝርዝር:\n"]
    for title, sched, posted, yt_id, source_url in rows:
        status = "✅" if posted else "⏳"
        time_str = sched.strftime("%Y-%m-%d %H:%M")
        src = " 🔄" if source_url else ""
        link = f"\n   🔗 youtu.be/shorts/{yt_id}" if posted and yt_id and not str(yt_id).startswith("ERROR") else ""
        lines.append(f"{status}{src} {title} — {time_str}{link}")
    await update.message.reply_text("\n".join(lines))


def get_source_channels() -> list:
    raw = get_setting("source_channels", "")
    return [c.strip() for c in raw.split("\n") if c.strip()]


def save_source_channels(channels: list):
    set_setting("source_channels", "\n".join(channels))


async def cmd_set_source(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args:
        channels = get_source_channels()
        if channels:
            lines = "\n".join(f"{i+1}. {c}" for i, c in enumerate(channels))
            await update.message.reply_text(
                f"📡 Source channels ({len(channels)}):\n\n{lines}\n\n"
                f"ለመጨመር: `/setsource https://youtube.com/@channel`\n"
                f"ለማስወገድ: `/removesource 1` (ቁጥር)",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                "❌ ምንም source channel የለም።\n`/setsource https://youtube.com/@channelname`",
                parse_mode="Markdown"
            )
        return
    url = ctx.args[0].strip()
    channels = get_source_channels()
    if url in channels:
        await update.message.reply_text("⚠️ ይህ channel ቀድሞ ተጨምሯል።")
        return
    channels.append(url)
    save_source_channels(channels)
    await update.message.reply_text(
        f"✅ Source channel ተጨምሯል! ({len(channels)} ጠቅላላ)\n\n🔗 {url}\n\n"
        f"አሁን `/autorepost on` ብለው auto-repost ያብሩ።",
        parse_mode="Markdown"
    )


async def cmd_remove_source(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    channels = get_source_channels()
    if not ctx.args:
        await update.message.reply_text("❌ ቁጥር ያስፍሩ: `/removesource 1`", parse_mode="Markdown")
        return
    try:
        idx = int(ctx.args[0]) - 1
        removed = channels.pop(idx)
        save_source_channels(channels)
        await update.message.reply_text(f"✅ ተወግዷል:\n{removed}")
    except (ValueError, IndexError):
        await update.message.reply_text("❌ ትክክለኛ ቁጥር ያስፍሩ።")


def parse_user_time(t: str) -> str:
    """
    Parse time in 12-hour (8:00PM, 8PM, 8:30am) or 24-hour (20:00) format.
    Returns UTC time string HH:MM (subtracts 3 hours for Addis Ababa UTC+3).
    Raises ValueError on bad input.
    """
    import re
    t = t.strip().upper().replace(" ", "")
    m = re.match(r'^(\d{1,2})(?::(\d{2}))?(AM|PM)?$', t)
    if not m:
        raise ValueError(f"Invalid time: {t}")
    hour   = int(m.group(1))
    minute = int(m.group(2)) if m.group(2) else 0
    ampm   = m.group(3)

    if ampm == "PM" and hour != 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0
    elif ampm is None:
        # 24-hour format — accept as-is
        pass

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Out of range: {t}")

    # Convert Addis (UTC+3) → UTC
    utc_hour = (hour - 3) % 24
    return f"{utc_hour:02d}:{minute:02d}"


def utc_to_addis(t: str) -> str:
    """Convert UTC HH:MM back to Addis AM/PM for display."""
    hour, minute = map(int, t.split(":"))
    addis_hour = (hour + 3) % 24
    ampm = "AM" if addis_hour < 12 else "PM"
    display_hour = addis_hour % 12 or 12
    return f"{display_hour}:{minute:02d} {ampm}"


async def cmd_set_time(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args:
        times = get_setting("repost_times", "17:00").split(",")
        times_str = "\n".join(f"  • {utc_to_addis(t)} (Addis)" for t in times)
        await update.message.reply_text(
            f"🕐 የቀጠሮ ሰዓቶች (Addis time):\n{times_str}\n\n"
            f"ለመቀየር: `/settime 9AM 2PM 8PM`",
            parse_mode="Markdown"
        )
        return

    valid_utc   = []
    valid_addis = []
    for t in ctx.args:
        try:
            utc = parse_user_time(t)
            valid_utc.append(utc)
            valid_addis.append(utc_to_addis(utc))
        except Exception:
            await update.message.reply_text(
                f"❌ `{t}` ትክክል አይደለም።\n"
                f"ምሳሌ: `9AM` `2:30PM` `8PM`",
                parse_mode="Markdown"
            )
            return

    set_setting("repost_times", ",".join(valid_utc))
    times_str = "\n".join(f"  • {a}" for a in valid_addis)
    await update.message.reply_text(
        f"✅ ሰዓቶች ተቀናብረዋል! ({len(valid_utc)} ጊዜ ዕለት ዕለት — Addis time)\n\n{times_str}",
        parse_mode="Markdown"
    )


async def cmd_autorepost(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args or ctx.args[0].lower() not in ("on", "off"):
        await update.message.reply_text("❌ `/autorepost on` ወይም `/autorepost off`", parse_mode="Markdown")
        return
    state = ctx.args[0].lower()
    set_setting("autorepost", state)

    if state == "on":
        channels    = get_source_channels()
        repost_time = get_setting("repost_time", "20:00")
        if not channels:
            await update.message.reply_text(
                "⚠️ ምንም source channel አልተጨመረም!\n`/setsource <URL>` ያስፍሩ።",
                parse_mode="Markdown"
            )
            return
        ch_lines = "\n".join(f"  {i+1}. {c}" for i, c in enumerate(channels))
        await update.message.reply_text(
            f"✅ Auto-repost ተበርቷል!\n\n"
            f"📡 Channels ({len(channels)}):\n{ch_lines}\n\n"
            f"🕐 ሰዓት: {repost_time} ዕለት ዕለት\n\n"
            f"ቦቱ ዕለት ዕለት channels ሲቀያይር ቪዲዮ ይለቅቃል።"
        )
    else:
        await update.message.reply_text("✅ Auto-repost ተዘግቷል።")


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    enabled      = get_setting("autorepost", "off")
    repost_times = get_setting("repost_times", "17:00").split(",")
    channels     = get_source_channels()

    status_icon = "🟢" if enabled == "on" else "🔴"
    ch_lines    = "\n".join(f"  {i+1}. {c}" for i, c in enumerate(channels)) if channels else "  ምንም የለም"
    time_lines  = "  " + ", ".join(utc_to_addis(t) for t in repost_times)
    await update.message.reply_text(
        f"📊 Bot Status\n\n"
        f"{status_icon} Auto-repost: {enabled.upper()}\n"
        f"🕐 ሰዓቶች: {time_lines}\n\n"
        f"📡 Source channels ({len(channels)}):\n{ch_lines}"
    )


# ── Web server ────────────────────────────────────────────────────────────────

async def health(request):
    return web.Response(text="OK - YouTube Shorts Scheduler Bot")


async def start_web_server():
    port = int(os.environ.get("PORT", 10000))
    web_app = web.Application()
    web_app.router.add_get("/", health)
    web_app.router.add_get("/ping", health)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"[web] Listening on port {port}")


async def self_ping_loop():
    port = int(os.environ.get("PORT", 10000))
    await asyncio.sleep(30)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://0.0.0.0:{port}/ping") as resp:
                    logger.info(f"[self-ping] {resp.status}")
        except Exception as e:
            logger.warning(f"[self-ping] failed: {e}")
        await asyncio.sleep(240)


# ── Main ──────────────────────────────────────────────────────────────────────

async def set_bot_commands(app):
    await app.bot.set_my_commands([
        ("start",         "📹 Bot ይጀምሩ / እርዳታ"),
        ("status",        "📊 የ auto-repost ሁኔታ"),
        ("list",          "📋 የቀጠሮ ዝርዝር"),
        ("setsource",     "📡 Source channel ጨምሩ"),
        ("removesource",  "🗑 Source channel አስወግዱ"),
        ("settime",       "🕐 የቀጠሮ ሰዓቶች ቀናብሩ"),
        ("autorepost",    "🔄 Auto-repost on/off"),
        ("cancel",        "❌ አሁን ያለውን ይቅር"),
    ])
    logger.info("[bot] Commands menu set ✅")


async def run():
    init_db()
    await start_web_server()

    tg_app = Application.builder().token(BOT_TOKEN).build()

    # URL pattern for YouTube links
    yt_url_filter = filters.Regex(r'https?://(www\.)?(youtube\.com|youtu\.be)/\S+')

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.VIDEO | filters.Document.VIDEO, receive_video),
            MessageHandler(yt_url_filter, receive_url),
        ],
        states={
            WAIT_VIDEO:    [MessageHandler(filters.VIDEO | filters.Document.VIDEO, receive_video)],
            WAIT_TITLE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_title)],
            WAIT_SCHEDULE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_schedule)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("list", list_scheduled))
    tg_app.add_handler(CommandHandler("status", cmd_status))
    tg_app.add_handler(CommandHandler("setsource", cmd_set_source))
    tg_app.add_handler(CommandHandler("removesource", cmd_remove_source))
    tg_app.add_handler(CommandHandler("settime", cmd_set_time))
    tg_app.add_handler(CommandHandler("autorepost", cmd_autorepost))
    tg_app.add_handler(conv)

    async with tg_app:
        await tg_app.start()
        await set_bot_commands(tg_app)
        await tg_app.updater.start_polling(allowed_updates=["message"])
        asyncio.create_task(scheduler_loop(tg_app.bot))
        asyncio.create_task(auto_repost_loop(tg_app.bot))
        asyncio.create_task(self_ping_loop())
        await asyncio.Event().wait()
        await tg_app.updater.stop()
        await tg_app.stop()


if __name__ == "__main__":
    asyncio.run(run())
