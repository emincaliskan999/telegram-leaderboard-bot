import asyncio
import csv
import os
import aiosqlite
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, MessageReactionUpdated, FSInputFile
from aiogram.filters import Command

BOT_TOKEN = os.getenv("BOT_TOKEN")
DISCUSSION_CHAT_ID = int(os.getenv("DISCUSSION_CHAT_ID") or "0")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Istanbul")

ADMIN_USER_IDS = {
    int(x) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x
}

DB_PATH = "leaderboard.db"

REACTION_POINTS = 1
COMMENT_POINTS = 3
MAX_COMMENTS_PER_THREAD = 3

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


def now():
    return datetime.now(ZoneInfo(TIMEZONE))


def week_key():
    n = now()
    iso = n.isocalendar()
    return f"{iso.year}-W{iso.week}"


async def init_db():

    async with aiosqlite.connect(DB_PATH) as db:

        await db.executescript("""

        CREATE TABLE IF NOT EXISTS scores (
            week TEXT,
            user_id INTEGER,
            username TEXT,
            full_name TEXT,
            points INTEGER,
            PRIMARY KEY (week,user_id)
        );

        CREATE TABLE IF NOT EXISTS reactions (
            week TEXT,
            user_id INTEGER,
            chat_id INTEGER,
            message_id INTEGER,
            PRIMARY KEY (week,user_id,chat_id,message_id)
        );

        CREATE TABLE IF NOT EXISTS comments (
            week TEXT,
            user_id INTEGER,
            chat_id INTEGER,
            thread TEXT,
            message_id INTEGER
        );

        """)

        await db.commit()


async def add_points(user_id, username, full_name, pts):

    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(
            "SELECT points FROM scores WHERE week=? AND user_id=?",
            (w, user_id)
        )

        row = await cur.fetchone()

        if row:

            await db.execute(
                "UPDATE scores SET points = points + ? WHERE week=? AND user_id=?",
                (pts, w, user_id)
            )

        else:

            await db.execute(
                "INSERT INTO scores VALUES (?,?,?,?,?)",
                (w, user_id, username, full_name, pts)
            )

        await db.commit()


async def handle_reaction(user_id, username, full_name, chat_id, message_id):

    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(
            "SELECT 1 FROM reactions WHERE week=? AND user_id=? AND chat_id=? AND message_id=?",
            (w, user_id, chat_id, message_id)
        )

        if await cur.fetchone():
            return

        await db.execute(
            "INSERT INTO reactions VALUES (?,?,?,?)",
            (w, user_id, chat_id, message_id)
        )

        cur = await db.execute(
            "SELECT points FROM scores WHERE week=? AND user_id=?",
            (w, user_id)
        )

        row = await cur.fetchone()

        if row:
            await db.execute(
                "UPDATE scores SET points = points + ? WHERE week=? AND user_id=?",
                (REACTION_POINTS, w, user_id)
            )
        else:
            await db.execute(
                "INSERT INTO scores VALUES (?,?,?,?,?)",
                (w, user_id, username, full_name, REACTION_POINTS)
            )

        await db.commit()


async def handle_comment(user_id, username, full_name, chat_id, thread, message_id):

    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(
            "SELECT COUNT(*) FROM comments WHERE week=? AND user_id=? AND chat_id=? AND thread=?",
            (w, user_id, chat_id, thread)
        )

        count = (await cur.fetchone())[0]

        if count >= MAX_COMMENTS_PER_THREAD:
            return

        await db.execute(
            "INSERT INTO comments VALUES (?,?,?,?,?)",
            (w, user_id, chat_id, thread, message_id)
        )

        cur = await db.execute(
            "SELECT points FROM scores WHERE week=? AND user_id=?",
            (w, user_id)
        )

        row = await cur.fetchone()

        if row:
            await db.execute(
                "UPDATE scores SET points = points + ? WHERE week=? AND user_id=?",
                (COMMENT_POINTS, w, user_id)
            )
        else:
            await db.execute(
                "INSERT INTO scores VALUES (?,?,?,?,?)",
                (w, user_id, username, full_name, COMMENT_POINTS)
            )

        await db.commit()


async def get_leaderboard(limit=10):

    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(
            "SELECT user_id,username,full_name,points FROM scores WHERE week=? ORDER BY points DESC LIMIT ?",
            (w, limit)
        )

        rows = await cur.fetchall()

    return rows


async def get_user_stats(user_id):

    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(
            "SELECT username,full_name,points FROM scores WHERE week=? AND user_id=?",
            (w, user_id)
        )

        return await cur.fetchone()

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

    for i, r in enumerate(rows, 1):

        name = r[1] if r[1] else r[2] or r[0]

        if r[1]:
            name = "@" + name

        text += f"{i}. {name} — {r[3]} pts\n"

    await msg.answer(text)


@router.message(Command("top10"))
async def top10(msg: Message):

    rows = await get_leaderboard(10)

    text = "🔥 Top 10\n\n"

    for i, r in enumerate(rows, 1):

        name = r[1] if r[1] else r[2] or r[0]

        if r[1]:
            name = "@" + name

        text += f"{i}. {name} — {r[3]} pts\n"

    await msg.answer(text)


@router.message(Command("mystats"))
async def mystats(msg: Message):

    stats = await get_user_stats(msg.from_user.id)

    if not stats:

        await msg.answer("Bu hafta puanın yok.")
        return

    await msg.answer(f"📊 Puanın: {stats[2]}")


@router.message(Command("resetweek"))
async def resetweek(msg: Message):

    if msg.from_user.id not in ADMIN_USER_IDS:
        return

    w = week_key()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("DELETE FROM scores WHERE week=?", (w,))
        await db.execute("DELETE FROM reactions WHERE week=?", (w,))
        await db.execute("DELETE FROM comments WHERE week=?", (w,))

        await db.commit()

    await msg.answer("Week reset.")


@router.message(Command("exportweek"))
async def exportweek(msg: Message):

    if msg.from_user.id not in ADMIN_USER_IDS:
        return

    rows = await get_leaderboard(1000)

    path = "export.csv"

    with open(path, "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)

        writer.writerow(["rank", "username", "points"])

        for i, r in enumerate(rows, 1):

            name = r[1] if r[1] else r[2] or r[0]

            writer.writerow([i, name, r[3]])

    await msg.answer_document(FSInputFile(path))


@router.message()
async def track_comments(msg: Message):
    @router.message()
async def track_comments(msg: Message):

    # DEBUG
    if msg.text == "/debugcomment":
        await msg.answer(
            f"chat_id={msg.chat.id}\n"
            f"user_id={msg.from_user.id if msg.from_user else 'none'}\n"
            f"text={msg.text}\n"
            f"message_thread_id={msg.message_thread_id}\n"
            f"reply_to_message_id={msg.reply_to_message.message_id if msg.reply_to_message else 'none'}\n"
            f"discussion_env={DISCUSSION_CHAT_ID}"
        )
        return

    if msg.chat.id != DISCUSSION_CHAT_ID:
        return

    if msg.chat.id != DISCUSSION_CHAT_ID:
        return

    if msg.from_user.is_bot:
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
        msg.from_user.id,
        msg.from_user.username,
        msg.from_user.full_name,
        msg.chat.id,
        thread,
        msg.message_id
    )


@router.message_reaction()
async def track_reaction(event: MessageReactionUpdated):

    user = event.user

    if not user:
        return

    if user.is_bot:
        return

    if not event.new_reaction:
        return

    await handle_reaction(
        user.id,
        user.username,
        user.full_name,
        event.chat.id,
        event.message_id
    )


async def main():

    await init_db()

    await dp.start_polling(
        bot,
        allowed_updates=[
            "message",
            "message_reaction"
        ]
    )


if __name__ == "__main__":
    asyncio.run(main())
