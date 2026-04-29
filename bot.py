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
from typing import List, Dict, Optional
import os
import re
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ================= CONFIG =================
API_TOKEN = os.getenv("BOT_TOKEN", "8777177819:AAHuJtPJR8VmoWSfqHtrHW7WeVNWJ6sbV7o")
WEBSITE_URL = "https://sexstory.lovable.app"
DATABASE_FILE = "bot_database.db"
ADMIN_IDS = [8459969831]  # Apni ID dalo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= DATABASE =================
class Database:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.init_database()
    
    def get_connection(self):
        return sqlite3.connect(self.db_file)
    
    def init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    joined_date TIMESTAMP,
                    referred_by INTEGER,
                    referral_count INTEGER DEFAULT 0,
                    total_interactions INTEGER DEFAULT 0,
                    is_admin BOOLEAN DEFAULT 0,
                    is_banned BOOLEAN DEFAULT 0
                )
            ''')
            
            # Stories table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    url TEXT UNIQUE,
                    fetched_at TIMESTAMP,
                    views INTEGER DEFAULT 0
                )
            ''')
            
            # Add admins
            for admin_id in ADMIN_IDS:
                cursor.execute('''
                    INSERT OR REPLACE INTO users (user_id, is_admin, joined_date)
                    VALUES (?, 1, ?)
                ''', (admin_id, datetime.now()))
            
            conn.commit()
            logger.info("Database initialized")
    
    def is_admin(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] == 1 if result else False
    
    def is_banned(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] == 1 if result else False
    
    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = ""):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            if cursor.fetchone():
                return True
            
            cursor.execute('''
                INSERT INTO users (user_id, username, first_name, last_name, joined_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name, datetime.now()))
            conn.commit()
            return True
    
    def save_stories(self, stories: List[Dict]):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM stories")
            for story in stories:
                cursor.execute('''
                    INSERT INTO stories (title, url, fetched_at)
                    VALUES (?, ?, ?)
                ''', (story['title'], story['url'], datetime.now()))
            conn.commit()
            logger.info(f"Saved {len(stories)} stories")
    
    def get_stories(self) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT title, url, views FROM stories ORDER BY fetched_at DESC LIMIT 20')
            results = cursor.fetchall()
            return [{'title': row[0], 'url': row[1], 'views': row[2]} for row in results]
    
    def get_all_users(self) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT user_id, username, first_name, last_name, joined_date, 
                       referral_count, total_interactions, is_admin, is_banned 
                FROM users ORDER BY joined_date DESC
            ''')
            results = cursor.fetchall()
            
            users = []
            for row in results:
                users.append({
                    'user_id': row[0],
                    'username': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'joined_date': row[4],
                    'referral_count': row[5],
                    'interactions': row[6],
                    'is_admin': row[7],
                    'is_banned': row[8]
                })
            return users
    
    def get_user_count(self) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]
    
    def get_admin_count(self) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
            return cursor.fetchone()[0]
    
    def get_banned_count(self) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
            return cursor.fetchone()[0]
    
    def ban_user(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def unban_user(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def make_admin(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def remove_admin(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_admin = 0 WHERE user_id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def search_users(self, query: str) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT user_id, username, first_name, last_name, is_admin, is_banned 
                FROM users 
                WHERE username LIKE ? OR first_name LIKE ? OR CAST(user_id AS TEXT) LIKE ?
                LIMIT 20
            ''', (f'%{query}%', f'%{query}%', f'%{query}%'))
            
            results = cursor.fetchall()
            users = []
            for row in results:
                users.append({
                    'user_id': row[0],
                    'username': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'is_admin': row[4],
                    'is_banned': row[5]
                })
            return users
    
    def update_interaction(self, user_id: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET total_interactions = total_interactions + 1 WHERE user_id = ?', (user_id,))
            conn.commit()
    
    def get_user_stats(self, user_id: int) -> Dict:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT referral_count, total_interactions, joined_date FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            if result:
                return {'referral_count': result[0], 'interactions': result[1], 'joined_date': result[2]}
            return {}

# ================= STORY FETCHER =================
class StoryFetcher:
    def __init__(self):
        self.db = Database(DATABASE_FILE)
    
    async def fetch_stories(self) -> List[Dict]:
        cached_stories = self.db.get_stories()
        if cached_stories:
            logger.info(f"Returning {len(cached_stories)} cached stories")
            return cached_stories
        
        stories = []
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                
                async with session.get(WEBSITE_URL, headers=headers, timeout=15) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        all_links = soup.find_all('a', href=True)
                        for link in all_links:
                            href = link.get('href', '')
                            title = link.get_text(strip=True)
                            
                            if '/story/' in href and title and len(title) > 3:
                                full_url = href if href.startswith('http') else WEBSITE_URL.rstrip('/') + href
                                stories.append({'title': title[:50], 'url': full_url})
                        
                        unique_stories = []
                        seen_urls = set()
                        for story in stories:
                            if story['url'] not in seen_urls:
                                seen_urls.add(story['url'])
                                unique_stories.append(story)
                        
                        if unique_stories:
                            self.db.save_stories(unique_stories)
                            logger.info(f"Fetched {len(unique_stories)} real stories")
                            return unique_stories[:15]
        
        except Exception as e:
            logger.error(f"Fetch error: {e}")
        
        if not stories:
            stories = [
                {"title": "The Midnight Story", "url": f"{WEBSITE_URL}/story/1"},
                {"title": "Secret Desires", "url": f"{WEBSITE_URL}/story/2"},
                {"title": "Forbidden Love", "url": f"{WEBSITE_URL}/story/3"},
            ]
            self.db.save_stories(stories)
        
        return stories[:15]

# ================= STATES =================
class AdminStates(StatesGroup):
    broadcasting = State()
    searching_user = State()
    ban_user = State()
    make_admin_state = State()

# ================= MAIN MENU WITH MINI APP BUTTON =================
@dp.message(Command("start"))
async def start_command(message: types.Message, state: FSMContext):
    user = message.from_user
    db = Database(DATABASE_FILE)
    
    if db.is_banned(user.id):
        await message.answer("❌ You have been banned from this bot.")
        return
    
    db.add_user(user.id, user.username, user.first_name, user.last_name or "")
    
    # ========== YAHAN MINI APP BUTTON ADD KIYA HAI ==========
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📱 Open Stories App",
                web_app=WebAppInfo(url=WEBSITE_URL)  # Mini App button
            )
        ],
        [
            InlineKeyboardButton(text="📚 Latest Stories", callback_data="stories"),
            InlineKeyboardButton(text="💰 Referral", callback_data="referral")
        ],
        [
            InlineKeyboardButton(text="📊 My Stats", callback_data="stats")
        ]
    ])
    
    # Admin button for admins
    if db.is_admin(user.id):
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text="👑 ADMIN PANEL", callback_data="admin_panel")
        ])
    
    await message.answer(
        f"🌟 **Welcome {user.first_name}!** 🌟\n\n"
        f"📱 **Click 'Open Stories App'** to read stories inside Telegram!\n"
        f"💰 Invite friends and earn rewards!\n\n"
        f"Choose an option below:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await state.clear()

# ================= STORIES HANDLER =================
@dp.callback_query(lambda c: c.data == "stories")
async def stories_handler(callback_query: types.CallbackQuery):
    await callback_query.answer("📚 Fetching stories...")
    
    db = Database(DATABASE_FILE)
    db.update_interaction(callback_query.from_user.id)
    
    loading_msg = await callback_query.message.answer("⏳ Loading stories from website...")
    
    fetcher = StoryFetcher()
    stories = await fetcher.fetch_stories()
    
    if not stories:
        await loading_msg.edit_text("❌ No stories found. Please try again later.")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for idx, story in enumerate(stories[:10], 1):
        button_text = f"📖 {story['title'][:35]}"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=button_text, url=story['url'])
        ])
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="refresh_stories"),
        InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")
    ])
    
    await loading_msg.delete()
    await callback_query.message.answer(
        f"📚 **Latest Stories** ({len(stories[:10])} found)\n\nTap any story to read:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data == "refresh_stories")
async def refresh_stories(callback_query: types.CallbackQuery):
    await callback_query.answer("🔄 Refreshing...")
    
    loading_msg = await callback_query.message.answer("⏳ Fetching fresh stories...")
    
    fetcher = StoryFetcher()
    stories = await fetcher.fetch_stories()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for idx, story in enumerate(stories[:10], 1):
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"📖 {story['title'][:35]}", url=story['url'])
        ])
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="refresh_stories"),
        InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")
    ])
    
    await loading_msg.delete()
    await callback_query.message.edit_text(
        f"✅ **Refreshed!** ({len(stories[:10])} stories)",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

# ================= REFERRAL HANDLER =================
@dp.callback_query(lambda c: c.data == "referral")
async def referral_handler(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    bot_username = (await bot.get_me()).username
    link = f"https://t.me/{bot_username}?start={user_id}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Share Link", switch_inline_query=f"Join: {link}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.answer(
        f"💰 **Your Referral Link**\n\n`{link}`\n\nShare with friends!\n\n"
        f"💡 **Tip:** When friends join using your link, you get rewards!",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

# ================= STATS HANDLER =================
@dp.callback_query(lambda c: c.data == "stats")
async def stats_handler(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    stats = db.get_user_stats(callback_query.from_user.id)
    
    text = f"📊 **Your Stats**\n\n"
    text += f"👥 Referrals: {stats.get('referral_count', 0)}\n"
    text += f"🔄 Interactions: {stats.get('interactions', 0)}\n"
    text += f"📅 Joined: {stats.get('joined_date', 'Unknown')[:10] if stats.get('joined_date') else 'Unknown'}\n\n"
    text += f"💡 Invite more friends to increase referrals!"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.answer(text, reply_markup=keyboard, parse_mode="Markdown")

# ================= BACK TO MENU =================
@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await start_command(callback_query.message, state)

# ================= ADMIN PANEL =================
@dp.callback_query(lambda c: c.data == "admin_panel")
async def admin_panel(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin access only!", show_alert=True)
        return
    
    total_users = db.get_user_count()
    admin_count = db.get_admin_count()
    banned_count = db.get_banned_count()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Dashboard", callback_data="admin_dashboard")],
        [InlineKeyboardButton(text="👥 All Users", callback_data="admin_users_list")],
        [InlineKeyboardButton(text="🔍 Search Users", callback_data="admin_search")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="➕ Make Admin", callback_data="admin_make")],
        [InlineKeyboardButton(text="🚫 Ban/Unban", callback_data="admin_ban")],
        [InlineKeyboardButton(text="🔄 Refresh Stories", callback_data="admin_refresh_stories")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    text = f"👑 **Admin Panel**\n\n📊 Users: {total_users}\n👑 Admins: {admin_count}\n🚫 Banned: {banned_count}"
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "admin_refresh_stories")
async def admin_refresh_stories(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    await callback_query.answer("🔄 Refreshing stories...")
    
    fetcher = StoryFetcher()
    stories = await fetcher.fetch_stories()
    
    await callback_query.message.answer(f"✅ Refreshed {len(stories)} stories from website!")

# ================= ADMIN USER MANAGEMENT =================
@dp.callback_query(lambda c: c.data == "admin_users_list")
async def admin_users_list(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    users = db.get_all_users()
    
    if not users:
        await callback_query.message.answer("No users found.")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for idx, user in enumerate(users[:10], 1):
        status = "👑" if user['is_admin'] else "👤"
        if user['is_banned']:
            status = "🚫"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{idx}. {status} {user['first_name'] or user['username'] or user['user_id']}",
                callback_data=f"user_detail_{user['user_id']}"
            )
        ])
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_users_list"),
        InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")
    ])
    
    await callback_query.message.edit_text(f"👥 **Users** ({len(users)} total)", reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data.startswith("user_detail_"))
async def user_detail(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split("_")[2])
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, referral_count, total_interactions, is_admin, is_banned FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
    
    if not user:
        await callback_query.message.answer("User not found.")
        return
    
    status = "👑 Admin" if user[7] else "👤 User"
    if user[8]:
        status = "🚫 Banned"
    
    text = f"**User Details**\n\n🆔 ID: `{user[0]}`\n👤 Name: {user[2]}\n📝 Username: @{user[1] or 'N/A'}\n📅 Joined: {user[4][:10] if user[4] else 'Unknown'}\n👥 Referrals: {user[5]}\n🔄 Interactions: {user[6]}\n⭐ Status: {status}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Ban" if not user[8] else "✅ Unban", callback_data=f"toggle_ban_{user[0]}"),
         InlineKeyboardButton(text="👑 Make Admin" if not user[7] else "❌ Remove Admin", callback_data=f"toggle_admin_{user[0]}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_users_list")]
    ])
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data.startswith("toggle_ban_"))
async def toggle_ban(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split("_")[2])
    db = Database(DATABASE_FILE)
    
    if db.is_banned(user_id):
        db.unban_user(user_id)
        action = "unbanned"
    else:
        db.ban_user(user_id)
        action = "banned"
    
    await callback_query.answer(f"User {action}!")
    await user_detail(callback_query)

@dp.callback_query(lambda c: c.data.startswith("toggle_admin_"))
async def toggle_admin(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split("_")[2])
    db = Database(DATABASE_FILE)
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        
        if result and result[0] == 1:
            db.remove_admin(user_id)
            action = "removed from admin"
        else:
            db.make_admin(user_id)
            action = "made admin"
    
    await callback_query.answer(f"User {action}!")
    await user_detail(callback_query)

@dp.callback_query(lambda c: c.data == "admin_search")
async def admin_search(callback_query: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    await callback_query.message.edit_text("🔍 Send username, name, or ID to search:\nType /cancel")
    await state.set_state(AdminStates.searching_user)
    await callback_query.answer()

@dp.message(AdminStates.searching_user)
async def process_search(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await admin_panel(message)
        return
    
    db = Database(DATABASE_FILE)
    users = db.search_users(message.text)
    
    if not users:
        await message.answer("❌ No users found.")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for user in users[:10]:
        status = "👑" if user['is_admin'] else "👤"
        if user['is_banned']:
            status = "🚫"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"{status} {user['first_name'] or user['username'] or user['user_id']}", callback_data=f"user_detail_{user['user_id']}")
        ])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")])
    
    await message.answer(f"🔍 Found {len(users)} users:", reply_markup=keyboard)
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def admin_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    await callback_query.message.edit_text("📢 Send broadcast message:\nType /cancel")
    await state.set_state(AdminStates.broadcasting)
    await callback_query.answer()

@dp.message(AdminStates.broadcasting)
async def process_broadcast(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await admin_panel(message)
        return
    
    db = Database(DATABASE_FILE)
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE is_banned = 0")
        users = cursor.fetchall()
    
    sent, failed = 0, 0
    status_msg = await message.answer("⏳ Sending...")
    
    for user in users:
        try:
            await bot.send_message(user[0], message.text)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    
    await status_msg.edit_text(f"✅ Sent: {sent}\n❌ Failed: {failed}")
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_dashboard")
async def admin_dashboard(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    text = f"📊 **Dashboard**\n\n👥 Users: {db.get_user_count()}\n👑 Admins: {db.get_admin_count()}\n🚫 Banned: {db.get_banned_count()}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")]])
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "admin_make")
async def admin_make_handler(callback_query: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    await callback_query.message.edit_text("➕ Send user ID to make admin:\nType /cancel")
    await state.set_state(AdminStates.make_admin_state)
    await callback_query.answer()

@dp.message(AdminStates.make_admin_state)
async def process_make_admin(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await admin_panel(message)
        return
    
    try:
        user_id = int(message.text)
        db = Database(DATABASE_FILE)
        db.make_admin(user_id)
        await message.answer(f"✅ User {user_id} is now admin!")
    except:
        await message.answer("❌ Invalid ID!")
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_ban")
async def admin_ban_handler(callback_query: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    await callback_query.message.edit_text("🚫 Send user ID to ban/unban:\nType /cancel")
    await state.set_state(AdminStates.ban_user)
    await callback_query.answer()

@dp.message(AdminStates.ban_user)
async def process_ban(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await admin_panel(message)
        return
    
    try:
        user_id = int(message.text)
        db = Database(DATABASE_FILE)
        
        if db.is_banned(user_id):
            db.unban_user(user_id)
            await message.answer(f"✅ User {user_id} unbanned!")
        else:
            db.ban_user(user_id)
            await message.answer(f"✅ User {user_id} banned!")
    except:
        await message.answer("❌ Invalid ID!")
    await state.clear()

# ================= ERROR HANDLER =================
@dp.errors()
async def error_handler(update: types.Update, exception: Exception):
    logger.error(f"Error: {exception}")
    return True

# ================= MAIN =================
async def main():
    logger.info("🚀 Starting bot with Mini App feature...")
    db = Database(DATABASE_FILE)
    
    fetcher = StoryFetcher()
    stories = await fetcher.fetch_stories()
    logger.info(f"Loaded {len(stories)} stories")
    
    await dp.start_polling(bot)
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Bot stopped")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())