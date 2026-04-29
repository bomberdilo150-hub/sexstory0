from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
import asyncio
import logging
from datetime import datetime
import sqlite3
import os
import re
import aiohttp
from bs4 import BeautifulSoup

# ================= CONFIG =================
API_TOKEN = "8777177819:AAHuJtPJR8VmoWSfqHtrHW7WeVNWJ6sbV7o"
WEBSITE_URL = "https://sexstory.lovable.app"
DATABASE_FILE = "bot_database.db"
ADMIN_IDS = [8459969831]

REFERRAL_BONUS = 10  # 10 Rupees per referral
MINIMUM_WITHDRAWAL = 100  # Minimum 100 Rupees

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= STORY FETCHER =================
class StoryFetcher:
    def __init__(self):
        self.stories_cache = []
        self.last_fetch = None
    
    async def fetch_stories(self):
        """Fetch stories from website"""
        stories = []
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                async with session.get(WEBSITE_URL, headers=headers, timeout=15) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Find all links that might be stories
                        all_links = soup.find_all('a', href=True)
                        for link in all_links:
                            href = link.get('href', '')
                            title = link.get_text(strip=True)
                            
                            # Check if it's a story link
                            if ('/story/' in href or '/post/' in href or '/read/' in href) and title and len(title) > 5:
                                full_url = href if href.startswith('http') else WEBSITE_URL.rstrip('/') + href
                                stories.append({
                                    'title': title[:60],
                                    'url': full_url
                                })
                        
                        # Remove duplicates
                        seen = set()
                        unique_stories = []
                        for story in stories:
                            if story['url'] not in seen:
                                seen.add(story['url'])
                                unique_stories.append(story)
                        
                        if unique_stories:
                            self.stories_cache = unique_stories[:20]
                            self.last_fetch = datetime.now()
                            logger.info(f"Fetched {len(unique_stories)} stories")
                            return self.stories_cache
            
            # If no stories found, return dummy stories
            if not stories:
                stories = [
                    {"title": "The Midnight Story", "url": f"{WEBSITE_URL}/story/1"},
                    {"title": "Secret Desires", "url": f"{WEBSITE_URL}/story/2"},
                    {"title": "Forbidden Love", "url": f"{WEBSITE_URL}/story/3"},
                    {"title": "Mystery of the Night", "url": f"{WEBSITE_URL}/story/4"},
                    {"title": "The Lost Treasure", "url": f"{WEBSITE_URL}/story/5"},
                ]
                self.stories_cache = stories
                return stories
                
        except Exception as e:
            logger.error(f"Fetch error: {e}")
            # Return cached stories if available
            if self.stories_cache:
                return self.stories_cache
        
        return stories[:15]
    
    async def get_stories(self, force_refresh=False):
        """Get stories (from cache or fresh)"""
        if force_refresh or not self.stories_cache or not self.last_fetch:
            return await self.fetch_stories()
        return self.stories_cache

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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                joined_date TIMESTAMP,
                balance INTEGER DEFAULT 0,
                total_earned INTEGER DEFAULT 0,
                total_withdrawn INTEGER DEFAULT 0,
                referral_count INTEGER DEFAULT 0,
                is_admin BOOLEAN DEFAULT 0,
                is_banned BOOLEAN DEFAULT 0
            )
        ''')
        
        # Withdrawal requests
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                upi_id TEXT,
                status TEXT DEFAULT 'pending',
                request_date TIMESTAMP
            )
        ''')
        
        # Add admin
        for admin_id in ADMIN_IDS:
            cursor.execute('''
                INSERT OR IGNORE INTO users (user_id, is_admin, joined_date, balance)
                VALUES (?, 1, ?, 0)
            ''', (admin_id, datetime.now()))
        
        conn.commit()
        conn.close()
        logger.info("Database initialized")
    
    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = "", referred_by: int = None):
        conn = self.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO users (user_id, username, first_name, last_name, joined_date, balance)
                VALUES (?, ?, ?, ?, ?, 0)
            ''', (user_id, username, first_name, last_name, datetime.now()))
            
            # Add referral bonus
            if referred_by and referred_by != user_id:
                self.add_balance(referred_by, REFERRAL_BONUS)
                cursor.execute("UPDATE users SET referral_count = referral_count + 1 WHERE user_id = ?", (referred_by,))
        
        conn.commit()
        conn.close()
        return True
    
    def get_balance(self, user_id: int) -> int:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0
    
    def add_balance(self, user_id: int, amount: int):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET balance = balance + ?, total_earned = total_earned + ? WHERE user_id = ?", 
                      (amount, amount, user_id))
        conn.commit()
        conn.close()
    
    def deduct_balance(self, user_id: int, amount: int) -> bool:
        conn = self.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        
        if result and result[0] >= amount:
            cursor.execute("UPDATE users SET balance = balance - ?, total_withdrawn = total_withdrawn + ? WHERE user_id = ?", 
                          (amount, amount, user_id))
            conn.commit()
            conn.close()
            return True
        
        conn.close()
        return False
    
    def create_withdrawal(self, user_id: int, amount: int, upi_id: str) -> int:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO withdrawals (user_id, amount, upi_id, request_date)
            VALUES (?, ?, ?, ?)
        ''', (user_id, amount, upi_id, datetime.now()))
        conn.commit()
        withdraw_id = cursor.lastrowid
        conn.close()
        return withdraw_id
    
    def get_pending_withdrawals(self):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT w.*, u.first_name, u.username 
            FROM withdrawals w
            JOIN users u ON w.user_id = u.user_id
            WHERE w.status = 'pending'
            ORDER BY w.request_date ASC
        ''')
        results = cursor.fetchall()
        conn.close()
        return results
    
    def approve_withdrawal(self, withdraw_id: int):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("UPDATE withdrawals SET status = 'approved' WHERE id = ?", (withdraw_id,))
        conn.commit()
        conn.close()
    
    def reject_withdrawal(self, withdraw_id: int, user_id: int, amount: int):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("UPDATE withdrawals SET status = 'rejected' WHERE id = ?", (withdraw_id,))
        self.add_balance(user_id, amount)
        conn.commit()
        conn.close()
    
    def get_user_stats(self, user_id: int):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT balance, total_earned, total_withdrawn, referral_count, joined_date
            FROM users WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'balance': result[0],
                'total_earned': result[1],
                'total_withdrawn': result[2],
                'referral_count': result[3],
                'joined_date': result[4]
            }
        return {'balance': 0, 'total_earned': 0, 'total_withdrawn': 0, 'referral_count': 0, 'joined_date': datetime.now()}
    
    def is_admin(self, user_id: int) -> bool:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] == 1 if result else False
    
    def is_banned(self, user_id: int) -> bool:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] == 1 if result else False
    
    def get_all_users(self):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE is_banned = 0")
        results = cursor.fetchall()
        conn.close()
        return results

# ================= STATES =================
class WithdrawState(StatesGroup):
    amount = State()
    upi = State()

class AdminState(StatesGroup):
    broadcast = State()

# ================= KEYBOARDS =================
def get_main_keyboard(is_admin: bool = False):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📱 Open Stories App",
                web_app=WebAppInfo(url=WEBSITE_URL)
            )
        ],
        [
            InlineKeyboardButton(text="📚 Stories", callback_data="stories"),
            InlineKeyboardButton(text="💰 Refer", callback_data="referral")
        ],
        [
            InlineKeyboardButton(text="💳 Balance", callback_data="balance"),
            InlineKeyboardButton(text="📊 Stats", callback_data="stats")
        ],
        [
            InlineKeyboardButton(text="🏧 Withdraw", callback_data="withdraw"),
            InlineKeyboardButton(text="❓ Help", callback_data="help")
        ]
    ])
    
    if is_admin:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text="👑 Admin", callback_data="admin_panel")
        ])
    
    return keyboard

# ================= STORIES KEYBOARD =================
def get_stories_keyboard(stories, page=0, items_per_page=5):
    """Create paginated stories keyboard"""
    start = page * items_per_page
    end = start + items_per_page
    page_stories = stories[start:end]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    for story in page_stories:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"📖 {story['title'][:35]}", url=story['url'])
        ])
    
    # Pagination buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"stories_page_{page-1}"))
    if end < len(stories):
        nav_buttons.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"stories_page_{page+1}"))
    
    if nav_buttons:
        keyboard.inline_keyboard.append(nav_buttons)
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="stories_refresh"),
        InlineKeyboardButton(text="◀️ Back", callback_data="back")
    ])
    
    return keyboard

# ================= COMMANDS =================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user = message.from_user
    db = Database(DATABASE_FILE)
    
    if db.is_banned(user.id):
        await message.answer("❌ You are banned!")
        return
    
    # Check for referral
    args = message.text.split()
    referred_by = None
    if len(args) > 1 and args[1].isdigit():
        referred_by = int(args[1])
    
    db.add_user(user.id, user.username, user.first_name, user.last_name or "", referred_by)
    balance = db.get_balance(user.id)
    
    text = f"""🌟 Welcome {user.first_name}! 🌟

💰 Your Balance: ₹{balance}

📱 Click 'Open Stories App' to read stories!
💰 Invite friends - Earn ₹{REFERRAL_BONUS} each!
💳 Withdraw at ₹{MINIMUM_WITHDRAWAL}

1 Diamond = ₹1

Choose an option:"""
    
    await message.answer(text, reply_markup=get_main_keyboard(db.is_admin(user.id)), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "stories")
async def stories_cmd(callback: types.CallbackQuery):
    await callback.answer("📚 Fetching stories...")
    
    fetcher = StoryFetcher()
    stories = await fetcher.get_stories()
    
    if not stories:
        await callback.message.answer("❌ No stories found! Please try again later.")
        return
    
    # Store stories in state or cache
    await callback.message.answer(
        f"📚 **Latest Stories**\n\nFound {len(stories)} stories:",
        reply_markup=get_stories_keyboard(stories, 0),
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data.startswith("stories_page_"))
async def stories_page_cmd(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    
    fetcher = StoryFetcher()
    stories = await fetcher.get_stories()
    
    if stories:
        await callback.message.edit_reply_markup(reply_markup=get_stories_keyboard(stories, page))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "stories_refresh")
async def stories_refresh_cmd(callback: types.CallbackQuery):
    await callback.answer("🔄 Refreshing stories...")
    
    fetcher = StoryFetcher()
    stories = await fetcher.get_stories(force_refresh=True)
    
    if stories:
        await callback.message.edit_reply_markup(reply_markup=get_stories_keyboard(stories, 0))
        await callback.answer("✅ Stories refreshed!")
    else:
        await callback.answer("❌ Failed to refresh!", show_alert=True)

@dp.callback_query(lambda c: c.data == "balance")
async def balance_cmd(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""💳 Your Balance

💰 Current: ₹{balance}
📈 Total Earned: ₹{stats['total_earned']}
💸 Total Withdrawn: ₹{stats['total_withdrawn']}
👥 Referrals: {stats['referral_count']}

💰 Earn ₹{REFERRAL_BONUS} per referral!
💳 Min withdrawal: ₹{MINIMUM_WITHDRAWAL}"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "stats")
async def stats_cmd(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""📊 Your Statistics

💰 Balance: ₹{stats['balance']}
📈 Total Earned: ₹{stats['total_earned']}
👥 Referrals: {stats['referral_count']}
📅 Joined: {str(stats['joined_date'])[:10]}

Keep referring to earn more!"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "referral")
async def referral_cmd(callback: types.CallbackQuery):
    bot_user = await bot.get_me()
    link = f"https://t.me/{bot_user.username}?start={callback.from_user.id}"
    db = Database(DATABASE_FILE)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""💰 Refer & Earn

🔗 Your Link:
`{link}`

📊 Your Stats:
• Referrals: {stats['referral_count']}
• Earned: ₹{stats['total_earned']}

🎁 Per Referral: ₹{REFERRAL_BONUS}
💳 1 Diamond = ₹1

Share link and start earning!"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Share", switch_inline_query=f"Join and earn: {link}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "withdraw")
async def withdraw_cmd(callback: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    
    if balance < MINIMUM_WITHDRAWAL:
        await callback.answer(f"Minimum withdrawal is ₹{MINIMUM_WITHDRAWAL}! You have ₹{balance}", show_alert=True)
        return
    
    await callback.message.answer(f"💰 Your balance: ₹{balance}\n\nEnter amount to withdraw (min ₹{MINIMUM_WITHDRAWAL}):\nType /cancel")
    await state.set_state(WithdrawState.amount)
    await callback.answer()

@dp.message(WithdrawState.amount)
async def withdraw_amount(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await start_cmd(message)
        return
    
    try:
        amount = int(message.text)
        db = Database(DATABASE_FILE)
        balance = db.get_balance(message.from_user.id)
        
        if amount < MINIMUM_WITHDRAWAL:
            await message.answer(f"❌ Minimum withdrawal is ₹{MINIMUM_WITHDRAWAL}!")
            return
        
        if amount > balance:
            await message.answer(f"❌ Insufficient balance! You have ₹{balance}")
            return
        
        await state.update_data(amount=amount)
        await message.answer("📱 Enter your UPI ID:\nExample: example@okhdfcbank\nType /cancel")
        await state.set_state(WithdrawState.upi)
        
    except ValueError:
        await message.answer("❌ Please enter a valid number!")

@dp.message(WithdrawState.upi)
async def withdraw_upi(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await start_cmd(message)
        return
    
    upi_id = message.text.strip()
    
    if '@' not in upi_id or len(upi_id) < 5:
        await message.answer("❌ Invalid UPI ID! Please enter valid UPI ID like: example@okhdfcbank")
        return
    
    data = await state.get_data()
    amount = data['amount']
    
    db = Database(DATABASE_FILE)
    
    if db.deduct_balance(message.from_user.id, amount):
        withdraw_id = db.create_withdrawal(message.from_user.id, amount, upi_id)
        
        await message.answer(
            f"✅ Withdrawal Request Submitted!\n\n"
            f"💰 Amount: ₹{amount}\n"
            f"📱 UPI: `{upi_id}`\n"
            f"🆔 ID: #{withdraw_id}\n\n"
            f"Admin will process within 24-48 hours.",
            parse_mode="Markdown"
        )
        
        # Notify admins
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"💰 NEW WITHDRAWAL\n"
                    f"User: {message.from_user.first_name}\n"
                    f"Amount: ₹{amount}\n"
                    f"UPI: {upi_id}\n"
                    f"ID: #{withdraw_id}",
                    parse_mode="Markdown"
                )
            except:
                pass
    else:
        await message.answer("❌ Failed to process withdrawal!")
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "help")
async def help_cmd(callback: types.CallbackQuery):
    text = f"""📖 Help Guide

💰 Earn: ₹{REFERRAL_BONUS} per referral
💳 Withdraw: Minimum ₹{MINIMUM_WITHDRAWAL}
📱 Payment: UPI transfer
📚 Stories: Updated daily

1 Diamond = ₹1

Commands:
/start - Restart bot
/help - This help

Contact @admin for support"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back")
async def back_cmd(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    
    text = f"""🌟 Main Menu

💰 Balance: ₹{balance}

Choose an option:"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(db.is_admin(callback.from_user.id)), parse_mode="Markdown")
    await callback.answer()

# ================= ADMIN PANEL =================
@dp.callback_query(lambda c: c.data == "admin_panel")
async def admin_panel_cmd(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    pending = db.get_pending_withdrawals()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💰 Withdrawals ({len(pending)})", callback_data="admin_withdrawals")],
        [InlineKeyboardButton(text="🔄 Refresh Stories", callback_data="admin_refresh_stories")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back")]
    ])
    
    await callback.message.edit_text("👑 Admin Panel", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_refresh_stories")
async def admin_refresh_stories(callback: types.CallbackQuery):
    await callback.answer("🔄 Refreshing stories from website...")
    
    fetcher = StoryFetcher()
    stories = await fetcher.get_stories(force_refresh=True)
    
    if stories:
        await callback.message.answer(f"✅ Refreshed {len(stories)} stories from website!")
    else:
        await callback.message.answer("❌ Failed to fetch stories from website!")
    
    await admin_panel_cmd(callback)

@dp.callback_query(lambda c: c.data == "admin_withdrawals")
async def admin_withdrawals_cmd(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    withdrawals = db.get_pending_withdrawals()
    
    if not withdrawals:
        await callback.message.answer("No pending withdrawals!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for w in withdrawals:
        name = w[11] if w[11] else f"User_{w[1]}"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"💰 {name[:15]} - ₹{w[2]}",
                callback_data=f"process_{w[0]}"
            )
        ])
    
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")])
    
    await callback.message.edit_text(f"💰 Pending Withdrawals ({len(withdrawals)})", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("process_"))
async def process_withdrawal(callback: types.CallbackQuery):
    withdraw_id = int(callback.data.split("_")[1])
    db = Database(DATABASE_FILE)
    
    conn = db.get_conn()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT w.*, u.first_name, u.username 
        FROM withdrawals w
        JOIN users u ON w.user_id = u.user_id
        WHERE w.id = ?
    ''', (withdraw_id,))
    w = cursor.fetchone()
    conn.close()
    
    if not w:
        await callback.message.answer("Withdrawal not found!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{withdraw_id}"),
         InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{withdraw_id}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_withdrawals")]
    ])
    
    text = f"""💰 Withdrawal Request #{w[0]}

👤 User: {w[11]}
📝 Username: @{w[12] or 'N/A'}
💰 Amount: ₹{w[2]}
📱 UPI: {w[3]}
📅 Date: {w[5]}

Approve or Reject?"""
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("approve_"))
async def approve_withdrawal(callback: types.CallbackQuery):
    withdraw_id = int(callback.data.split("_")[1])
    db = Database(DATABASE_FILE)
    
    db.approve_withdrawal(withdraw_id)
    
    await callback.answer("✅ Approved!", show_alert=True)
    await callback.message.edit_text("✅ Withdrawal approved!")
    await asyncio.sleep(2)
    await admin_withdrawals_cmd(callback)

@dp.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_withdrawal(callback: types.CallbackQuery):
    withdraw_id = int(callback.data.split("_")[1])
    db = Database(DATABASE_FILE)
    
    conn = db.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, amount FROM withdrawals WHERE id = ?", (withdraw_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        user_id, amount = result
        db.reject_withdrawal(withdraw_id, user_id, amount)
        
        try:
            await bot.send_message(user_id, f"❌ Your withdrawal of ₹{amount} was rejected. Amount refunded.")
        except:
            pass
    
    await callback.answer("❌ Rejected!", show_alert=True)
    await callback.message.edit_text("❌ Withdrawal rejected!")
    await asyncio.sleep(2)
    await admin_withdrawals_cmd(callback)

@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def admin_broadcast_cmd(callback: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    await callback.message.answer("📢 Send message to broadcast:\nType /cancel")
    await state.set_state(AdminState.broadcast)
    await callback.answer()

@dp.message(AdminState.broadcast)
async def process_broadcast(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await start_cmd(message)
        return
    
    db = Database(DATABASE_FILE)
    users = db.get_all_users()
    
    sent = 0
    status_msg = await message.answer("⏳ Broadcasting...")
    
    for user in users:
        try:
            await bot.send_message(user[0], message.text)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            pass
    
    await status_msg.edit_text(f"✅ Broadcast sent to {sent} users!")
    await state.clear()

# ================= ERROR HANDLER =================
@dp.errors()
async def error_handler(update: types.Update, exception: Exception):
    logger.error(f"Error: {exception}")
    return True

# ================= MAIN =================
async def main():
    logger.info("🚀 Starting bot with Story Fetcher...")
    
    if os.path.exists(DATABASE_FILE):
        try:
            os.remove(DATABASE_FILE)
            logger.info("Old database removed")
        except:
            pass
    
    db = Database(DATABASE_FILE)
    
    # Pre-fetch stories on startup
    fetcher = StoryFetcher()
    stories = await fetcher.get_stories()
    logger.info(f"Loaded {len(stories)} stories")
    
    logger.info("Bot ready!")
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
