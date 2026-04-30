from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.markdown import escape_md

import asyncio
import logging
from datetime import datetime, date
import sqlite3
import aiohttp
from bs4 import BeautifulSoup
import json
from typing import List, Dict, Optional

# ================= CONFIG =================
API_TOKEN = "8777177819:AAHuJtPJR8VmoWSfqHtrHW7WeVNWJ6sbV7o"
WEBSITE_URL = "https://sexstory.lovable.app"
STORY_PAGE_URL = "https://sexstory.lovable.app/story"
DATABASE_FILE = "bot_database.db"
ADMIN_IDS = [8459969831]

REFERRAL_BONUS = 10
MINIMUM_WITHDRAWAL = 100
STORY_READ_REWARD = 1
DAILY_STORY_LIMIT = 10

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= DATABASE =================
class Database:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.init_database()

    def get_conn(self):
        return sqlite3.connect(self.db_file, timeout=10)

    def init_database(self):
        conn = self.get_conn()
        cursor = conn.cursor()

        # Users table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            balance INTEGER DEFAULT 0,
            stories_read_today INTEGER DEFAULT 0,
            last_story_date TEXT,
            total_stories_read INTEGER DEFAULT 0,
            referred_by INTEGER,
            registration_date TEXT,
            is_banned BOOLEAN DEFAULT 0
        )
        """)

        # Referrals table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER,
            date TEXT,
            reward_given BOOLEAN DEFAULT 0,
            FOREIGN KEY (referrer_id) REFERENCES users (user_id),
            FOREIGN KEY (referred_id) REFERENCES users (user_id)
        )
        """)

        # Withdrawals table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER,
            status TEXT DEFAULT 'pending',
            request_date TEXT,
            processed_date TEXT,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
        """)

        # Stories read tracking
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stories_read (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            story_url TEXT,
            read_date TEXT,
            reward_given BOOLEAN DEFAULT 0,
            UNIQUE(user_id, story_url)
        )
        """)

        conn.commit()
        conn.close()

    def add_user(self, user_id: int, username: str = None, first_name: str = None, referred_by: int = None):
        conn = self.get_conn()
        cursor = conn.cursor()

        # Check if user exists
        cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("""
                INSERT INTO users (user_id, username, first_name, registration_date, referred_by)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, username, first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), referred_by))

            # Process referral if exists
            if referred_by:
                self.process_referral(referred_by, user_id)

        conn.commit()
        conn.close()

    def process_referral(self, referrer_id: int, referred_id: int):
        conn = self.get_conn()
        cursor = conn.cursor()

        # Check if referral already processed
        cursor.execute("SELECT id FROM referrals WHERE referrer_id = ? AND referred_id = ?", 
                      (referrer_id, referred_id))
        if not cursor.fetchone():
            # Add referral record
            cursor.execute("""
                INSERT INTO referrals (referrer_id, referred_id, date, reward_given)
                VALUES (?, ?, ?, ?)
            """, (referrer_id, referred_id, datetime.now().strftime("%Y-%m-%d"), 0))

            # Give bonus to referrer
            cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", 
                          (REFERRAL_BONUS, referrer_id))

        conn.commit()
        conn.close()

    def get_balance(self, user_id: int) -> int:
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
        result = cursor.fetchone()
        conn.close()

        return result[0] if result else 0

    def get_user_stats(self, user_id: int) -> Dict:
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT balance, total_stories_read, stories_read_today 
            FROM users WHERE user_id=?
        """, (user_id,))
        user_data = cursor.fetchone()

        # Get referral count
        cursor.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,))
        referral_count = cursor.fetchone()[0]

        # Get pending withdrawal
        cursor.execute("SELECT COUNT(*) FROM withdrawals WHERE user_id = ? AND status = 'pending'", (user_id,))
        pending_withdrawals = cursor.fetchone()[0]

        conn.close()

        if user_data:
            return {
                'balance': user_data[0],
                'total_read': user_data[1] or 0,
                'today_read': user_data[2] or 0,
                'referrals': referral_count,
                'pending_withdrawals': pending_withdrawals
            }
        return None

    def reward_story(self, user_id: int, story_url: str) -> bool:
        conn = self.get_conn()
        cursor = conn.cursor()

        today = datetime.now().strftime("%Y-%m-%d")

        # Check if story already rewarded
        cursor.execute(
            "SELECT id FROM stories_read WHERE user_id = ? AND story_url = ? AND read_date = ?",
            (user_id, story_url, today)
        )
        if cursor.fetchone():
            conn.close()
            return False

        cursor.execute(
            "SELECT stories_read_today, last_story_date FROM users WHERE user_id=?",
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

        # Add reward
        cursor.execute("""
            UPDATE users
            SET balance = balance + ?,
                stories_read_today = ?,
                last_story_date = ?,
                total_stories_read = total_stories_read + 1
            WHERE user_id = ?
        """, (STORY_READ_REWARD, stories_today + 1, today, user_id))

        # Record story read
        cursor.execute("""
            INSERT INTO stories_read (user_id, story_url, read_date, reward_given)
            VALUES (?, ?, ?, ?)
        """, (user_id, story_url, today, 1))

        conn.commit()
        conn.close()
        return True

    def request_withdrawal(self, user_id: int, amount: int) -> bool:
        conn = self.get_conn()
        cursor = conn.cursor()

        balance = self.get_balance(user_id)

        if amount < MINIMUM_WITHDRAWAL:
            conn.close()
            return False

        if amount > balance:
            conn.close()
            return False

        # Check for pending withdrawals
        cursor.execute("SELECT id FROM withdrawals WHERE user_id = ? AND status = 'pending'", (user_id,))
        if cursor.fetchone():
            conn.close()
            return False

        # Deduct balance
        cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))

        # Create withdrawal request
        cursor.execute("""
            INSERT INTO withdrawals (user_id, amount, request_date, status)
            VALUES (?, ?, ?, 'pending')
        """, (user_id, amount, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        conn.commit()
        conn.close()
        return True

    def get_pending_withdrawals(self) -> List[Dict]:
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, user_id, amount, request_date 
            FROM withdrawals 
            WHERE status = 'pending'
            ORDER BY request_date ASC
        """)

        withdrawals = []
        for row in cursor.fetchall():
            withdrawals.append({
                'id': row[0],
                'user_id': row[1],
                'amount': row[2],
                'date': row[3]
            })

        conn.close()
        return withdrawals

    def approve_withdrawal(self, withdrawal_id: int) -> bool:
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE withdrawals 
            SET status = 'approved', processed_date = ?
            WHERE id = ? AND status = 'pending'
        """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), withdrawal_id))

        conn.commit()
        success = cursor.rowcount > 0
        conn.close()
        return success

    def reject_withdrawal(self, withdrawal_id: int) -> bool:
        conn = self.get_conn()
        cursor = conn.cursor()

        # Get user and amount to refund
        cursor.execute("SELECT user_id, amount FROM withdrawals WHERE id = ? AND status = 'pending'", 
                      (withdrawal_id,))
        result = cursor.fetchone()

        if result:
            user_id, amount = result
            # Refund user
            cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
            # Update withdrawal status
            cursor.execute("""
                UPDATE withdrawals 
                SET status = 'rejected', processed_date = ?
                WHERE id = ?
            """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), withdrawal_id))

            conn.commit()
            conn.close()
            return True

        conn.close()
        return False

    def get_leaderboard(self, limit: int = 10) -> List[Dict]:
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT user_id, username, first_name, total_stories_read, balance
            FROM users 
            WHERE total_stories_read > 0
            ORDER BY total_stories_read DESC 
            LIMIT ?
        """, (limit,))

        leaders = []
        for row in cursor.fetchall():
            leaders.append({
                'user_id': row[0],
                'username': row[1],
                'name': row[2] or f"User {row[0]}",
                'stories': row[3],
                'balance': row[4]
            })

        conn.close()
        return leaders

# global db instance
DB = Database(DATABASE_FILE)

# ================= STORY FETCHER =================
class StoryFetcher:
    def __init__(self):
        self.cache = {}
        self.last_update = None

    async def fetch(self) -> List[Dict]:
        stories = []

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(STORY_PAGE_URL, timeout=10) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch stories: HTTP {response.status}")
                        return []
                    html = await response.text()

            soup = BeautifulSoup(html, "html.parser")

            # Try multiple methods to find stories
            stories_data = []

            # Method 1: Next.js data
            script = soup.find("script", {"id": "__NEXT_DATA__"})
            if script:
                try:
                    data = json.loads(script.string)
                    # Try different paths to find stories
                    possible_paths = [
                        ["props", "pageProps", "stories"],
                        ["props", "pageProps", "initialStories"],
                        ["props", "pageProps", "data", "stories"],
                        ["pageProps", "stories"]
                    ]
                    
                    for path in possible_paths:
                        temp = data
                        for key in path:
                            temp = temp.get(key, {})
                            if not temp:
                                break
                        if temp:
                            stories_data = temp
                            break
                except json.JSONDecodeError:
                    logger.error("Failed to parse NEXT_DATA JSON")

            # Method 2: Find stories in regular HTML
            if not stories_data:
                story_elements = soup.find_all('a', href=lambda x: x and '/story/' in x)
                for elem in story_elements[:20]:
                    title_elem = elem.find(['h2', 'h3', 'div', 'span'])
                    title = title_elem.get_text(strip=True) if title_elem else "Story"
                    url = elem.get('href')
                    if url:
                        if not url.startswith('http'):
                            url = WEBSITE_URL + url
                        stories_data.append({
                            'title': title,
                            'slug': url.split('/')[-1]
                        })

            # Process stories
            for story in stories_data:
                if isinstance(story, dict):
                    slug = story.get("slug") or story.get("id")
                    title = story.get("title", "Untitled Story")
                else:
                    continue

                if not slug:
                    continue

                url = f"{WEBSITE_URL}/story/{slug}"
                story_id = abs(hash(url)) % 100000

                stories.append({
                    "id": story_id,
                    "title": title[:100],  # Limit title length
                    "url": url,
                })

            # Remove duplicates by URL
            seen_urls = set()
            unique_stories = []
            for story in stories:
                if story['url'] not in seen_urls:
                    seen_urls.add(story['url'])
                    unique_stories.append(story)

            self.cache = {s["id"]: s for s in unique_stories}
            self.last_update = datetime.now()

            logger.info(f"Loaded {len(unique_stories)} stories")
            return unique_stories

        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching stories: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching stories: {e}")
            return []

    async def get(self) -> List[Dict]:
        if not self.cache or (datetime.now() - self.last_update).seconds > 300:  # Cache for 5 minutes
            await self.fetch()
        return list(self.cache.values())

FETCHER = StoryFetcher()

# ================= STATES =================
class WithdrawState(StatesGroup):
    amount = State()

class AdminState(StatesGroup):
    broadcast = State()

# ================= KEYBOARDS =================
def main_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📚 Browse Stories", callback_data="stories"),
                InlineKeyboardButton(text="💰 Balance", callback_data="balance")
            ],
            [
                InlineKeyboardButton(text="👥 Referral", callback_data="referral"),
                InlineKeyboardButton(text="📊 Stats", callback_data="stats")
            ],
            [
                InlineKeyboardButton(text="💸 Withdraw", callback_data="withdraw"),
                InlineKeyboardButton(text="🏆 Leaderboard", callback_data="leaderboard")
            ]
        ]
    )

def story_keyboard(stories: List[Dict]):
    buttons = []
    for story in stories[:10]:
        buttons.append([InlineKeyboardButton(
            text=f"📖 {escape_md(story['title'])}",
            url=story['url']
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Back to Menu", callback_data="back_to_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="💵 Pending Withdrawals", callback_data="admin_withdrawals")],
            [InlineKeyboardButton(text="📊 User Stats", callback_data="admin_stats")],
            [InlineKeyboardButton(text="◀️ Back to Menu", callback_data="back_to_menu")]
        ]
    )

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(message: types.Message, command: Command):
    args = command.args
    referred_by = None
    
    if args and args.startswith("ref_"):
        try:
            referred_by = int(args.split("_")[1])
            if referred_by == message.from_user.id:
                referred_by = None
        except (IndexError, ValueError):
            pass
    
    DB.add_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name,
        referred_by
    )
    
    welcome_text = (
        f"Welcome {message.from_user.first_name}! 👋\n\n"
        f"🎯 Read exciting stories and earn rewards!\n"
        f"💰 {STORY_READ_REWARD} coin per story\n"
        f"📚 {DAILY_STORY_LIMIT} stories per day\n"
        f"👥 {REFERRAL_BONUS} coins per referral\n\n"
        f"Use the buttons below to get started!"
    )
    
    await message.answer(welcome_text, reply_markup=main_keyboard())

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Main Menu:",
        reply_markup=main_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "stories")
async def stories(callback: types.CallbackQuery):
    await callback.message.edit_text("Loading stories... 📖")
    
    stories_list = await FETCHER.get()

    if not stories_list:
        await callback.message.edit_text(
            "❌ No stories found at the moment. Please try again later.",
            reply_markup=main_keyboard()
        )
        return

    await callback.message.edit_text(
        f"📚 Available Stories ({len(stories_list[:10])} shown):\n\nTap any story to read it!",
        reply_markup=story_keyboard(stories_list)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "balance")
async def balance(callback: types.CallbackQuery):
    stats = DB.get_user_stats(callback.from_user.id)
    
    if stats:
        balance_text = (
            f"💰 Your Balance: {stats['balance']} coins\n\n"
            f"📊 Statistics:\n"
            f"• Total stories read: {stats['total_read']}\n"
            f"• Today's stories: {stats['today_read']}/{DAILY_STORY_LIMIT}\n"
            f"• Referrals: {stats['referrals']}\n"
            f"• Pending withdrawals: {stats['pending_withdrawals']}\n\n"
            f"💸 Minimum withdrawal: {MINIMUM_WITHDRAWAL} coins"
        )
    else:
        balance_text = "💰 Balance: 0 coins"
    
    await callback.message.edit_text(balance_text, reply_markup=main_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "stats")
async def show_stats(callback: types.CallbackQuery):
    stats = DB.get_user_stats(callback.from_user.id)
    
    if stats:
        stats_text = (
            f"📊 Your Reading Statistics:\n\n"
            f"📚 Total stories read: {stats['total_read']}\n"
            f"📖 Today's stories: {stats['today_read']}/{DAILY_STORY_LIMIT}\n"
            f"💰 Total earnings: {stats['balance']} coins\n"
            f"👥 Referrals: {stats['referrals']}\n"
            f"🎯 Referral bonus: {REFERRAL_BONUS} coins each\n\n"
            f"Keep reading to earn more! 🚀"
        )
    else:
        stats_text = "No statistics available yet. Start reading stories!"
    
    await callback.message.edit_text(stats_text, reply_markup=main_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "referral")
async def referral(callback: types.CallbackQuery):
    stats = DB.get_user_stats(callback.from_user.id)
    bot_username = (await bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start=ref_{callback.from_user.id}"
    
    referral_text = (
        f"👥 Referral Program\n\n"
        f"Earn {REFERRAL_BONUS} coins for each friend who joins!\n\n"
        f"Your referral link:\n"
        f"`{referral_link}`\n\n"
        f"Referrals: {stats['referrals'] if stats else 0}\n\n"
        f"Share your link and start earning! 🎉"
    )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Share Link", switch_inline_query=f"Join me on this bot! {referral_link}")],
            [InlineKeyboardButton(text="◀️ Back to Menu", callback_data="back_to_menu")]
        ]
    )
    
    await callback.message.edit_text(referral_text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "leaderboard")
async def leaderboard(callback: types.CallbackQuery):
    leaders = DB.get_leaderboard(10)
    
    if not leaders:
        leaderboard_text = "🏆 Leaderboard\n\nNo readers yet! Be the first! 🚀"
    else:
        leaderboard_text = "🏆 Top Readers Leaderboard 🏆\n\n"
        for i, leader in enumerate(leaders, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "📖"
            leaderboard_text += f"{medal} {i}. {leader['name']}: {leader['stories']} stories ({leader['balance']} coins)\n"
    
    await callback.message.edit_text(leaderboard_text, reply_markup=main_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "withdraw")
async def withdraw_request(callback: types.CallbackQuery, state: FSMContext):
    balance = DB.get_balance(callback.from_user.id)
    
    if balance < MINIMUM_WITHDRAWAL:
        await callback.message.edit_text(
            f"❌ Insufficient balance!\n\n"
            f"Your balance: {balance} coins\n"
            f"Minimum withdrawal: {MINIMUM_WITHDRAWAL} coins\n\n"
            f"Keep reading to earn more! 📚",
            reply_markup=main_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"💸 Withdrawal Request\n\n"
        f"Your balance: {balance} coins\n"
        f"Minimum: {MINIMUM_WITHDRAWAL} coins\n\n"
        f"Enter the amount you want to withdraw (in coins):",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Cancel", callback_data="back_to_menu")]
            ]
        )
    )
    await state.set_state(WithdrawState.amount)
    await callback.answer()

@dp.message(WithdrawState.amount)
async def process_withdrawal(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text)
        
        if amount < MINIMUM_WITHDRAWAL:
            await message.answer(
                f"❌ Amount must be at least {MINIMUM_WITHDRAWAL} coins!\n"
                f"Please try again or type /cancel",
                reply_markup=main_keyboard()
            )
            return
        
        success = DB.request_withdrawal(message.from_user.id, amount)
        
        if success:
            await message.answer(
                f"✅ Withdrawal request of {amount} coins has been submitted!\n\n"
                f"Your request will be processed by an admin within 24-48 hours.\n"
                f"Thank you for your patience! 🙏",
                reply_markup=main_keyboard()
            )
            
            # Notify admins
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"💰 New withdrawal request!\n"
                        f"User: {message.from_user.id}\n"
                        f"Amount: {amount} coins"
                    )
                except:
                    pass
        else:
            await message.answer(
                f"❌ Withdrawal failed!\n\n"
                f"Possible reasons:\n"
                f"• Insufficient balance\n"
                f"• Pending withdrawal already exists\n"
                f"• Invalid amount\n\n"
                f"Please try again later.",
                reply_markup=main_keyboard()
            )
    except ValueError:
        await message.answer(
            "❌ Please enter a valid number!\n"
            "Type /cancel to go back to menu",
            reply_markup=main_keyboard()
        )
    
    await state.clear()

@dp.message(Command("cancel"))
async def cancel(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Operation cancelled!", reply_markup=main_keyboard())

# Admin handlers
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ You are not authorized to use this command!")
        return
    
    await message.answer(
        "🔧 Admin Panel\n\nSelect an action:",
        reply_markup=admin_keyboard()
    )

@dp.callback_query(lambda c: c.data == "admin_withdrawals")
async def admin_withdrawals(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    withdrawals = DB.get_pending_withdrawals()
    
    if not withdrawals:
        await callback.message.edit_text(
            "No pending withdrawals! ✅",
            reply_markup=admin_keyboard()
        )
        return
    
    text = "💵 Pending Withdrawals:\n\n"
    keyboard_buttons = []
    
    for w in withdrawals:
        text += f"ID: {w['id']} | User: {w['user_id']} | Amount: {w['amount']} | Date: {w['date']}\n"
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"✅ Approve {w['id']}", 
                callback_data=f"approve_{w['id']}"
            ),
            InlineKeyboardButton(
                text=f"❌ Reject {w['id']}", 
                callback_data=f"reject_{w['id']}"
            )
        ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_back")])
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("approve_"))
async def approve_withdrawal(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    withdrawal_id = int(callback.data.split("_")[1])
    success = DB.approve_withdrawal(withdrawal_id)
    
    if success:
        await callback.answer("Withdrawal approved! ✅", show_alert=True)
    else:
        await callback.answer("Failed to approve! ❌", show_alert=True)
    
    # Refresh admin panel
    await admin_withdrawals(callback)

@dp.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_withdrawal(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    withdrawal_id = int(callback.data.split("_")[1])
    success = DB.reject_withdrawal(withdrawal_id)
    
    if success:
        await callback.answer("Withdrawal rejected! ✅", show_alert=True)
    else:
        await callback.answer("Failed to reject! ❌", show_alert=True)
    
    # Refresh admin panel
    await admin_withdrawals(callback)

@dp.callback_query(lambda c: c.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔧 Admin Panel\n\nSelect an action:",
        reply_markup=admin_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📢 Send a message to broadcast to all users.\n\n"
        "Send your broadcast message (supports text, images, etc.):\n"
        "Type /cancel to cancel."
    )
    await state.set_state(AdminState.broadcast)
    await callback.answer()

@dp.message(AdminState.broadcast)
async def admin_broadcast_send(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Unauthorized!")
        await state.clear()
        return
    
    # Get all users
    conn = DB.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()
    conn.close()
    
    success_count = 0
    fail_count = 0
    
    status_msg = await message.answer("Sending broadcast... 📡")
    
    for user in users:
        try:
            await message.copy_to(user[0])
            success_count += 1
        except:
            fail_count += 1
        await asyncio.sleep(0.05)  # Avoid flooding
    
    await status_msg.edit_text(
        f"✅ Broadcast completed!\n\n"
        f"Sent: {success_count}\n"
        f"Failed: {fail_count}"
    )
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    conn = DB.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(total_stories_read) FROM users")
    total_stories = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT SUM(balance) FROM users")
    total_balance = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM withdrawals WHERE status = 'pending'")
    pending_withdrawals = cursor.fetchone()[0]
    
    conn.close()
    
    stats_text = (
        f"📊 Bot Statistics:\n\n"
        f"👥 Total users: {total_users}\n"
        f"📚 Total stories read: {total_stories}\n"
        f"💰 Total balance: {total_balance} coins\n"
        f"💸 Pending withdrawals: {pending_withdrawals}\n"
        f"🎯 Daily limit: {DAILY_STORY_LIMIT} stories/user\n"
        f"✨ Reward per story: {STORY_READ_REWARD} coin"
    )
    
    await callback.message.edit_text(stats_text, reply_markup=admin_keyboard())
    await callback.answer()

# Webhook for story read rewards
@dp.message(lambda message: message.text and WEBSITE_URL in message.text)
async def track_story_read(message: types.Message):
    # Extract story URL from message
    story_url = None
    for word in message.text.split():
        if WEBSITE_URL in word:
            story_url = word
            break
    
    if story_url:
        rewarded = DB.reward_story(message.from_user.id, story_url)
        
        if rewarded:
            await message.reply(
                f"✅ +{STORY_READ_REWARD} coins earned!\n"
                f"Check your balance with /balance",
                disable_notification=True
            )
        else:
            # Check if daily limit reached
            stats = DB.get_user_stats(message.from_user.id)
            if stats and stats['today_read'] >= DAILY_STORY_LIMIT:
                await message.reply(
                    f"⚠️ You've reached your daily limit of {DAILY_STORY_LIMIT} stories!\n"
                    f"Come back tomorrow for more rewards!",
                    disable_notification=True
                )

# ================= MAIN =================
async def main():
    logger.info("Bot is starting...")
    
    # Fetch stories on startup
    await FETCHER.fetch()
    
    # Set commands
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Start the bot"),
        types.BotCommand(command="cancel", description="Cancel current operation"),
        types.BotCommand(command="admin", description="Admin panel (admins only)")
    ])
    
    await bot.delete_webhook(drop_pending_updates=True)
    
    logger.info("Bot is running!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
