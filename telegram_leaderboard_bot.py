import asyncio
import csv
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import FSInputFile, Message, MessageReactionUpdated

# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DISCUSSION_CHAT_ID = int(os.getenv("DISCUSSION_CHAT_ID") or "0")
TIMEZONE = os.getenv("TIMEZONE") or "Europe/Istanbul"

ADMIN_USER_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_USER_IDS", "").split(",")
    if x.strip().isdigit()
}

DB_PATH = "leaderboard.db"

REACTION_POINTS = 1
COMMENT_POINTS = 3
MAX_COMMENTS_PER_THREAD = 3

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing.")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


# =========================================================
# HELPERS
# =========================================================

def now() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


def week_key() -> str:
    n = now()
    iso = n.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def is_admin_user(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def display_name(user_id: int, username: str | None, full_name: str | None) -> str:
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return str(user_id)


# =========================================================
# DATABASE
# =========================================================

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS scores (
    week TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT,
    full_name TEXT,
    points INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (week, user_id)
);

CREATE TABLE IF NOT EXISTS reactions (
    week TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (week, user_id, chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS comments (
    week TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    thread TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (week, message_id)
);

CREATE INDEX IF NOT EXISTS idx_comments_lookup
ON comments (week, user_id, chat_id, thread);
"""


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES_SQL)
        await db.commit()


async def add_or_update_score(
    db: aiosqlite.Connection,
    user_id: int,
    username: str | None,
    full_name: str | None,
    points_to_add: int,
) -> None:
    w = week_key()

    cur = await db.execute(
        "SELECT points FROM scores WHERE week=? AND user_id=?",
        (w, user_id)
    )
    row = await cur.fetchone()

    if row:
        await db.execute(
            "UPDATE scores SET points = points + ?, username = ?, full_name = ? WHERE week=? AND user_id=?",
            (points_to_add, username, full_name, w, user_id)
        )
    else:
        await db.execute(
            "INSERT INTO scores (week, user_id, username, full_name, points) VALUES (?, ?, ?, ?, ?)",
            (w, user_id, username, full_name, points_to_add)
        )


async def handle_reaction(
    user_id: int,
    username: str | None,
    full_name: str | None,
    chat_id: int,
    message_id: int,
) -> bool:
    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM reactions WHERE week=? AND user_id=? AND chat_id=? AND message_id=?",
            (w, user_id, chat_id, message_id)
        )

        if await cur.fetchone():
            return False

        await db.execute(
            "INSERT INTO reactions (week, user_id, chat_id, message_id) VALUES (?, ?, ?, ?)",
            (w, user_id, chat_id, message_id)
        )

        await add_or_update_score(
            db=db,
            user_id=user_id,
            username=username,
            full_name=full_name,
            points_to_add=REACTION_POINTS
        )

        await db.commit()
        return True


async def handle_comment(
    user_id: int,
    username: str | None,
    full_name: str | None,
    chat_id: int,
    thread: str,
    message_id: int,
) -> bool:
    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM comments WHERE week=? AND user_id=? AND chat_id=? AND thread=?",
            (w, user_id, chat_id, thread)
        )
        row = await cur.fetchone()
        count = row[0] if row else 0

        if count >= MAX_COMMENTS_PER_THREAD:
            return False

        await db.execute(
            "INSERT OR IGNORE INTO comments (week, user_id, chat_id, thread, message_id) VALUES (?, ?, ?, ?, ?)",
            (w, user_id, chat_id, thread, message_id)
        )

        cur = await db.execute("SELECT changes()")
        changed = await cur.fetchone()
        inserted = (changed[0] if changed else 0) > 0

        if not inserted:
            return False

        await add_or_update_score(
            db=db,
            user_id=user_id,
            username=username,
            full_name=full_name,
            points_to_add=COMMENT_POINTS
        )

        await db.commit()
        return True


async def get_leaderboard(limit: int = 10):
    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT user_id, username, full_name, points
            FROM scores
            WHERE week=?
            ORDER BY points DESC, user_id ASC
            LIMIT ?
            """,
            (w, limit)
        )
        return await cur.fetchall()


async def get_user_stats(user_id: int):
    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT user_id, username, full_name, points
            FROM scores
            WHERE week=? AND user_id=?
            """,
            (w, user_id)
        )
        return await cur.fetchone()


async def reset_current_week() -> None:
    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM scores WHERE week=?", (w,))
        await db.execute("DELETE FROM reactions WHERE week=?", (w,))
        await db.execute("DELETE FROM comments WHERE week=?", (w,))
        await db.commit()


async def export_current_week_csv(path: str) -> None:
    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT user_id, username, full_name, points
            FROM scores
            WHERE week=?
            ORDER BY points DESC, user_id ASC
            """,
            (w,)
        )
        rows = await cur.fetchall()

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["week", "rank", "user_id", "username", "full_name", "points"])

        for rank, row in enumerate(rows, start=1):
            writer.writerow([w, rank, row[0], row[1] or "", row[2] or "", row[3]])


# =========================================================
# COMMANDS
# =========================================================

@router.message(Command("chatid"))
async def chat_id_cmd(msg: Message):
    await msg.answer(f"Chat ID: {msg.chat.id}")


@router.message(Command("leaderboard"))
async def leaderboard(msg: Message):
    rows = await get_leaderboard(20)

    if not rows:
        await msg.answer("Henüz puan oluşmadı.")
        return

    text = "🏆 Weekly Leaderboard\n\n"

    for i, row in enumerate(rows, start=1):
        user_id, username, full_name, points = row
        name = display_name(user_id, username, full_name)
        text += f"{i}. {name} — {points} pts\n"

    await msg.answer(text)


@router.message(Command("top10"))
async def top10(msg: Message):
    rows = await get_leaderboard(10)

    if not rows:
        await msg.answer("Henüz puan oluşmadı.")
        return

    text = "🔥 Top 10\n\n"

    for i, row in enumerate(rows, start=1):
        user_id, username, full_name, points = row
        name = display_name(user_id, username, full_name)
        text += f"{i}. {name} — {points} pts\n"

    await msg.answer(text)


@router.message(Command("mystats"))
async def mystats(msg: Message):
    if not msg.from_user:
        return

    stats = await get_user_stats(msg.from_user.id)

    if not stats:
        await msg.answer("Bu hafta puanın yok.")
        return

    _, username, full_name, points = stats
    name = display_name(msg.from_user.id, username, full_name)

    await msg.answer(
        f"📊 Weekly Stats\n\n"
        f"User: {name}\n"
        f"Points: {points}"
    )


@router.message(Command("resetweek"))
async def resetweek(msg: Message):
    if not msg.from_user or not is_admin_user(msg.from_user.id):
        await msg.answer("Bu komut sadece adminler için açık.")
        return

    await reset_current_week()
    await msg.answer("Bu haftanın verileri sıfırlandı.")


@router.message(Command("exportweek"))
async def exportweek(msg: Message):
    if not msg.from_user or not is_admin_user(msg.from_user.id):
        await msg.answer("Bu komut sadece adminler için açık.")
        return

    path = f"weekly_export_{week_key()}.csv"
    await export_current_week_csv(path)
    await msg.answer_document(FSInputFile(path), caption=f"Weekly export: {week_key()}")


# =========================================================
# COMMENT TRACKING
# =========================================================

@router.message()
async def track_comments(msg: Message):
    if not msg.from_user:
        return

    if msg.from_user.is_bot:
        return

    if is_admin_user(msg.from_user.id):
        return

    if DISCUSSION_CHAT_ID and msg.chat.id != DISCUSSION_CHAT_ID:
        return

    if msg.text and msg.text.startswith("/"):
        return

    if msg.message_thread_id:
        thread = str(msg.message_thread_id)
    elif msg.reply_to_message:
        thread = str(msg.reply_to_message.message_id)
    else:
        return

    await handle_comment(
        user_id=msg.from_user.id,
        username=msg.from_user.username,
        full_name=msg.from_user.full_name,
        chat_id=msg.chat.id,
        thread=thread,
        message_id=msg.message_id
    )


# =========================================================
# REACTION TRACKING
# =========================================================

@router.message_reaction()
async def track_reaction(event: MessageReactionUpdated):
    user = event.user

    if not user:
        return

    if user.is_bot:
        return

    if is_admin_user(user.id):
        return

    if not event.new_reaction:
        return

    await handle_reaction(
        user_id=user.id,
        username=user.username,
        full_name=user.full_name,
        chat_id=event.chat.id,
        message_id=event.message_id
    )


# =========================================================
# START
# =========================================================

async def main():
    await init_db()

    await dp.start_polling(
        bot,
        allowed_updates=[
            "message",
            "message_reaction",
        ]
    )


if __name__ == "__main__":
    asyncio.run(main())
