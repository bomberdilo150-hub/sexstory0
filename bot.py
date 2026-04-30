from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.markdown import escape_md

import asyncio
import logging
from datetime import datetime
import sqlite3
import aiohttp
from bs4 import BeautifulSoup
import json

# ================= CONFIG =================
API_TOKEN = "PUT_YOUR_TOKEN_HERE"
WEBSITE_URL = "https://sexstory.lovable.app"
STORY_PAGE_URL = "https://sexstory.lovable.app/story/"
DATABASE_FILE = "bot_database.db"
ADMIN_IDS = [8459969831]

REFERRAL_BONUS = 10
MINIMUM_WITHDRAWAL = 100
STORY_READ_REWARD = 1
DAILY_STORY_LIMIT = 10

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= DATABASE =================
class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self.init_database()

    def get_conn(self):
        return sqlite3.connect(self.db_file, timeout=10)

    def init_database(self):
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER DEFAULT 0,
            stories_read_today INTEGER DEFAULT 0,
            last_story_date TEXT
        )
        """)

        conn.commit()
        conn.close()

    def add_user(self, user_id):
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "INSERT OR IGNORE INTO users (user_id) VALUES (?)",
            (user_id,),
        )

        conn.commit()
        conn.close()

    def get_balance(self, user_id):
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT balance FROM users WHERE user_id=?",
            (user_id,),
        )

        result = cursor.fetchone()
        conn.close()

        return result[0] if result else 0

    def reward_story(self, user_id):
        conn = self.get_conn()
        cursor = conn.cursor()

        today = datetime.now().strftime("%Y-%m-%d")

        cursor.execute(
            "SELECT stories_read_today,last_story_date FROM users WHERE user_id=?",
            (user_id,),
        )

        row = cursor.fetchone()

        if not row:
            conn.close()
            return False

        stories_today, last_date = row

        if last_date != today:
            stories_today = 0

        if stories_today >= DAILY_STORY_LIMIT:
            conn.close()
            return False

        cursor.execute(
            """
            UPDATE users
            SET balance = balance + ?,
            stories_read_today = ?,
            last_story_date = ?
            WHERE user_id=?
            """,
            (
                STORY_READ_REWARD,
                stories_today + 1,
                today,
                user_id,
            ),
        )

        conn.commit()
        conn.close()

        return True


# global db instance
DB = Database(DATABASE_FILE)


# ================= STORY FETCHER =================
class StoryFetcher:

    def __init__(self):
        self.cache = {}

    async def fetch(self):

        stories = []

        async with aiohttp.ClientSession() as session:

            async with session.get(STORY_PAGE_URL) as response:

                html = await response.text()

        soup = BeautifulSoup(html, "html.parser")

        script = soup.find("script", {"id": "__NEXT_DATA__"})

        if not script:
            logger.error("NEXT_DATA missing")
            return []

        data = json.loads(script.string)

        try:

            stories_data = (
                data["props"]["pageProps"]["stories"]
            )

        except:
            logger.error("stories json missing")
            return []

        for story in stories_data:

            slug = story.get("slug")

            if not slug:
                continue

            url = f"{WEBSITE_URL}/story/{slug}"

            stories.append(
                {
                    "id": abs(hash(url)) % 100000,
                    "title": story.get("title", "Story"),
                    "url": url,
                }
            )

        self.cache = {s["id"]: s for s in stories}

        logger.info(f"Loaded {len(stories)} stories")

        return stories

    async def get(self):

        if not self.cache:
            await self.fetch()

        return list(self.cache.values())


FETCHER = StoryFetcher()


# ================= STATES =================
class WithdrawState(StatesGroup):
    amount = State()


# ================= KEYBOARD =================
def main_keyboard():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Browse Stories",
                    callback_data="stories",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Balance",
                    callback_data="balance",
                )
            ],
        ]
    )


# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(message: types.Message):

    DB.add_user(message.from_user.id)

    await message.answer(
        "Welcome",
        reply_markup=main_keyboard(),
    )


@dp.callback_query(lambda c: c.data == "stories")
async def stories(callback: types.CallbackQuery):

    stories = await FETCHER.get()

    if not stories:

        await callback.message.edit_text(
            "No stories found"
        )

        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=escape_md(s["title"]),
                    url=s["url"],
                )
            ]
            for s in stories[:10]
        ]
    )

    await callback.message.edit_text(
        "Stories",
        reply_markup=keyboard,
    )


@dp.callback_query(lambda c: c.data == "balance")
async def balance(callback: types.CallbackQuery):

    balance = DB.get_balance(
        callback.from_user.id
    )

    await callback.message.edit_text(
        f"Balance: {balance}",
        reply_markup=main_keyboard(),
    )


# ================= MAIN =================
async def main():

    logger.info("bot started")

    await bot.delete_webhook(
        drop_pending_updates=True
    )

    await dp.start_polling(bot)


if __name__ == "__main__":

    asyncio.run(main())
