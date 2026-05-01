from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

import asyncio
import logging
from datetime import datetime
import sqlite3
import aiohttp
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional
import time
import json

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# ================= CONFIG =================
API_TOKEN = "8777177819:AAH5v7Dbckc-iNByI6U9AT479l1E9zmgSzY"
WEBSITE_URL = "https://sexstory.lovable.app"
DATABASE_FILE = "bot_database.db"
ADMIN_IDS = [8459969831]  # Your admin ID

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

        # Create users table
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
            is_banned BOOLEAN DEFAULT 0,
            upi_id TEXT
        )
        """)

        # Create referrals table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER,
            date TEXT,
            reward_given BOOLEAN DEFAULT 0
        )
        """)

        # Create withdrawals table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER,
            upi_id TEXT,
            status TEXT DEFAULT 'pending',
            request_date TEXT,
            processed_date TEXT
        )
        """)

        # Create stories_read table
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
        logger.info("✅ Database initialized successfully")

    def add_user(self, user_id: int, username: str = None, first_name: str = None, referred_by: int = None):
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        exists = cursor.fetchone()

        if not exists:
            logger.info(f"📝 New user registering: {user_id} | Referred by: {referred_by}")
            
            cursor.execute("""
                INSERT INTO users (user_id, username, first_name, registration_date, referred_by)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, username, first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), referred_by))
            
            conn.commit()
            logger.info(f"✅ User {user_id} inserted successfully")

            # Process referral AFTER user is inserted
            if referred_by and referred_by != user_id:
                # Check if referrer exists
                cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (referred_by,))
                referrer = cursor.fetchone()
                
                if referrer:
                    logger.info(f"🎯 Processing referral: {referred_by} referred {user_id}")
                    self.process_referral(referred_by, user_id)
                else:
                    logger.warning(f"⚠️ Referrer {referred_by} not found in database")
        else:
            logger.info(f"ℹ️ User {user_id} already exists")

        conn.commit()
        conn.close()

    def process_referral(self, referrer_id: int, referred_id: int):
        conn = self.get_conn()
        cursor = conn.cursor()

        # Check if this referral already exists
        cursor.execute("SELECT id FROM referrals WHERE referrer_id = ? AND referred_id = ?", 
                      (referrer_id, referred_id))
        existing = cursor.fetchone()
        
        if not existing:
            logger.info(f"💰 Adding referral bonus: {REFERRAL_BONUS} coins to referrer {referrer_id}")
            
            # Insert referral record with reward_given = 1 (true)
            cursor.execute("""
                INSERT INTO referrals (referrer_id, referred_id, date, reward_given)
                VALUES (?, ?, ?, ?)
            """, (referrer_id, referred_id, datetime.now().strftime("%Y-%m-%d"), 1))

            # Add bonus to referrer's balance
            cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", 
                          (REFERRAL_BONUS, referrer_id))
            
            conn.commit()
            logger.info(f"✅ Referral completed: User {referrer_id} got {REFERRAL_BONUS} coins for referring {referred_id}")
            
            # Verify the update
            cursor.execute("SELECT balance FROM users WHERE user_id = ?", (referrer_id,))
            new_balance = cursor.fetchone()
            if new_balance:
                logger.info(f"💰 Referrer {referrer_id} new balance: {new_balance[0]}")
        else:
            logger.info(f"⚠️ Duplicate referral prevented: {referrer_id} already referred {referred_id}")

        conn.close()

    def update_upi_id(self, user_id: int, upi_id: str):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET upi_id = ? WHERE user_id = ?", (upi_id, user_id))
        conn.commit()
        conn.close()

    def get_upi_id(self, user_id: int) -> str:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT upi_id FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

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

        # Count referrals where this user is the referrer AND reward was given
        cursor.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ? AND reward_given = 1", (user_id,))
        referral_count = cursor.fetchone()[0]
        
        logger.info(f"📊 Stats for user {user_id}: referrals count = {referral_count}")

        cursor.execute("SELECT COUNT(*) FROM withdrawals WHERE user_id = ? AND status = 'pending'", (user_id,))
        pending_withdrawals = cursor.fetchone()[0]

        conn.close()

        if user_data:
            return {
                'balance': user_data[0] or 0,
                'total_read': user_data[1] or 0,
                'today_read': user_data[2] or 0,
                'referrals': referral_count,
                'pending_withdrawals': pending_withdrawals
            }
        return None

    def reward_story(self, user_id: int, story_url: str) -> tuple:
        conn = self.get_conn()
        cursor = conn.cursor()

        today = datetime.now().strftime("%Y-%m-%d")

        cursor.execute(
            "SELECT id FROM stories_read WHERE user_id = ? AND story_url = ? AND read_date = ?",
            (user_id, story_url, today)
        )
        if cursor.fetchone():
            conn.close()
            return False, "already_read"

        cursor.execute(
            "SELECT stories_read_today, last_story_date FROM users WHERE user_id=?",
            (user_id,),
        )

        row = cursor.fetchone()

        if not row:
            conn.close()
            return False, "no_user"

        stories_today, last_date = row
        
        if last_date is None or last_date != today:
            stories_today = 0

        if stories_today >= DAILY_STORY_LIMIT:
            conn.close()
            return False, "daily_limit"

        cursor.execute("""
            UPDATE users
            SET balance = balance + ?,
                stories_read_today = ?,
                last_story_date = ?,
                total_stories_read = total_stories_read + 1
            WHERE user_id = ?
        """, (STORY_READ_REWARD, stories_today + 1, today, user_id))

        cursor.execute("""
            INSERT INTO stories_read (user_id, story_url, read_date, reward_given)
            VALUES (?, ?, ?, ?)
        """, (user_id, story_url, today, 1))

        conn.commit()
        conn.close()
        return True, "success"

    def request_withdrawal(self, user_id: int, amount: int, upi_id: str) -> bool:
        conn = self.get_conn()
        cursor = conn.cursor()

        balance = self.get_balance(user_id)

        if amount < MINIMUM_WITHDRAWAL:
            conn.close()
            return False

        if amount > balance:
            conn.close()
            return False

        cursor.execute("SELECT id FROM withdrawals WHERE user_id = ? AND status = 'pending'", (user_id,))
        if cursor.fetchone():
            conn.close()
            return False

        cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
        cursor.execute("""
            INSERT INTO withdrawals (user_id, amount, upi_id, request_date, status)
            VALUES (?, ?, ?, ?, 'pending')
        """, (user_id, amount, upi_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        conn.commit()
        conn.close()
        return True

    def get_pending_withdrawals(self) -> List[Dict]:
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, user_id, amount, upi_id, request_date 
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
                'upi_id': row[3],
                'date': row[4]
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

        cursor.execute("SELECT user_id, amount FROM withdrawals WHERE id = ? AND status = 'pending'", 
                      (withdrawal_id,))
        result = cursor.fetchone()

        if result:
            user_id, amount = result
            cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
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
                'name': row[2] or f"User {row[0]}",
                'stories': row[3] or 0,
                'balance': row[4] or 0
            })

        conn.close()
        return leaders

    def get_all_users(self) -> List[int]:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE is_banned = 0")
        users = [row[0] for row in cursor.fetchall()]
        conn.close()
        return users

    def get_total_users(self) -> int:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def get_total_stories_read(self) -> int:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(total_stories_read) FROM users")
        total = cursor.fetchone()[0] or 0
        conn.close()
        return total

    def get_total_balance(self) -> int:
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(balance) FROM users")
        total = cursor.fetchone()[0] or 0
        conn.close()
        return total

    def get_all_users_list(self, limit=20):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, username, first_name, balance, total_stories_read FROM users ORDER BY user_id DESC LIMIT ?", (limit,))
        users = cursor.fetchall()
        conn.close()
        return users

    def get_referral_details(self, user_id: int) -> List[Dict]:
        """Get detailed referral information for a user"""
        conn = self.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT r.referred_id, u.first_name, u.username, r.date, r.reward_given
            FROM referrals r
            LEFT JOIN users u ON r.referred_id = u.user_id
            WHERE r.referrer_id = ?
            ORDER BY r.date DESC
        """, (user_id,))
        
        referrals = []
        for row in cursor.fetchall():
            referrals.append({
                'referred_id': row[0],
                'name': row[1] or f"User{row[0]}",
                'username': row[2],
                'date': row[3],
                'rewarded': row[4]
            })
        
        conn.close()
        return referrals

    def get_all_referrals_for_admin(self) -> List:
        """Get all referrals for admin panel"""
        conn = self.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT r.referrer_id, r.referred_id, r.date, r.reward_given,
                   u1.first_name as referrer_name, u2.first_name as referred_name
            FROM referrals r
            LEFT JOIN users u1 ON r.referrer_id = u1.user_id
            LEFT JOIN users u2 ON r.referred_id = u2.user_id
            ORDER BY r.date DESC
            LIMIT 30
        """)
        
        referrals = cursor.fetchall()
        conn.close()
        return referrals

# Global DB instance
DB = Database(DATABASE_FILE)

# ================= STORY FETCHER =================
class StoryFetcher:
    def __init__(self):
        self.stories = []
        self.last_update = None

    def get_driver(self):
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )
            return driver
        except Exception as e:
            logger.error(f"Driver error: {e}")
            return None

    def get_stories_selenium(self) -> List[Dict]:
        driver = None
        try:
            driver = self.get_driver()
            if not driver:
                return []

            driver.get(WEBSITE_URL)
            time.sleep(5)
            
            # Scroll multiple times
            for _ in range(3):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
            
            # Find all story links
            links = driver.find_elements(By.TAG_NAME, "a")
            stories = []
            seen_urls = set()
            
            for link in links:
                try:
                    href = link.get_attribute("href")
                    title = link.text.strip()
                    
                    if href and "/story/" in href:
                        if href not in seen_urls:
                            seen_urls.add(href)
                            if not title or len(title) < 3:
                                # Try to get title from parent
                                parent = link.find_element(By.XPATH, "..")
                                title = parent.text.strip()[:100] if parent else "Story"
                            stories.append({
                                "title": title[:100] if title else "Story",
                                "url": href
                            })
                except:
                    continue
            
            driver.quit()
            logger.info(f"Selenium se {len(stories)} stories mili")
            return stories[:20]
            
        except Exception as e:
            logger.error(f"Selenium error: {e}")
            if driver:
                driver.quit()
            return []

    async def get_stories_fallback(self) -> List[Dict]:
        stories = []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(WEBSITE_URL, timeout=15, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }) as response:
                    if response.status != 200:
                        return []
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find all links
                    all_links = soup.find_all('a', href=True)
                    seen_urls = set()
                    
                    for link in all_links:
                        href = link.get('href', '')
                        if '/story/' in href:
                            full_url = WEBSITE_URL + href if href.startswith('/') else href
                            title = link.get_text(strip=True)
                            
                            if not title or len(title) < 3:
                                parent = link.find_parent(['div', 'article', 'section'])
                                if parent:
                                    title_elem = parent.find(['h1', 'h2', 'h3', 'p'])
                                    if title_elem:
                                        title = title_elem.get_text(strip=True)
                            
                            if not title:
                                title = "Story"
                            
                            if full_url and full_url not in seen_urls:
                                seen_urls.add(full_url)
                                stories.append({
                                    "title": title[:100],
                                    "url": full_url
                                })
                    
                    logger.info(f"Fallback se {len(stories)} stories mili")
                    return stories[:20]
                    
        except Exception as e:
            logger.error(f"Fallback error: {e}")
            return []

    async def get_stories(self) -> List[Dict]:
        # Return cached stories if fresh
        if self.stories and self.last_update:
            age = (datetime.now() - self.last_update).seconds
            if age < 300:
                return self.stories
        
        # Try Selenium first
        stories = self.get_stories_selenium()
        
        # If Selenium fails, try fallback
        if not stories:
            stories = await self.get_stories_fallback()
        
        # Add IDs
        for i, story in enumerate(stories, 1):
            story['id'] = i
        
        self.stories = stories
        self.last_update = datetime.now()
        
        if not stories:
            logger.warning("❌ No stories found!")
        else:
            logger.info(f"✅ Total {len(stories)} stories ready")
        
        return stories

FETCHER = StoryFetcher()

# ================= STATES =================
class WithdrawState(StatesGroup):
    upi_id = State()
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
            ],
            [
                InlineKeyboardButton(text="🌐 Open Website", web_app=WebAppInfo(url=WEBSITE_URL))
            ]
        ]
    )

def story_keyboard(stories: List[Dict]):
    buttons = []
    for story in stories[:10]:
        buttons.append([InlineKeyboardButton(
            text=f"📖 {story['title'][:50]}",
            web_app=WebAppInfo(url=story['url'])
        )])
    
    buttons.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="refresh_stories")])
    buttons.append([InlineKeyboardButton(text="◀️ Back to Menu", callback_data="back_to_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="💵 Pending Withdrawals", callback_data="admin_withdrawals")],
            [InlineKeyboardButton(text="📊 Bot Statistics", callback_data="admin_stats")],
            [InlineKeyboardButton(text="🔄 Refresh Stories", callback_data="admin_refresh")],
            [InlineKeyboardButton(text="🔍 Debug Stories", callback_data="admin_debug")],
            [InlineKeyboardButton(text="📋 User List", callback_data="admin_users")],
            [InlineKeyboardButton(text="🧪 Test Referrals", callback_data="admin_test_referral")],
            [InlineKeyboardButton(text="👥 All Referrals", callback_data="admin_all_referrals")],
            [InlineKeyboardButton(text="◀️ Back to Menu", callback_data="back_to_menu")]
        ]
    )

# ================= USER HANDLERS =================
@dp.message(Command("start"))
async def start(message: types.Message):
    args = message.text.split()
    referred_by = None
    
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referred_by = int(args[1].split("_")[1])
            if referred_by == message.from_user.id:
                referred_by = None
            logger.info(f"🔗 Referral link detected: referrer={referred_by}, new_user={message.from_user.id}")
        except Exception as e:
            logger.error(f"Error parsing referral: {e}")
            pass
    
    DB.add_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name,
        referred_by
    )
    
    welcome_text = (
        f"🎉 **Welcome {message.from_user.first_name}!**\n\n"
        f"📚 Read stories and earn rewards!\n"
        f"💰 `{STORY_READ_REWARD}` coin per story\n"
        f"📖 `{DAILY_STORY_LIMIT}` stories per day\n"
        f"👥 `{REFERRAL_BONUS}` coins per referral\n\n"
        f"✨ Stories open in Mini App!\n"
        f"👇 Tap below to start!"
    )
    
    # Send notification to referrer if applicable
    if referred_by and referred_by != message.from_user.id:
        try:
            referrer_stats = DB.get_user_stats(referred_by)
            if referrer_stats:
                await bot.send_message(
                    referred_by,
                    f"🎉 **New Referral!**\n\n"
                    f"👤 {message.from_user.first_name} joined using your link!\n"
                    f"💰 You earned `{REFERRAL_BONUS}` coins!\n"
                    f"👥 Total referrals: `{referrer_stats['referrals']}`\n"
                    f"💰 New balance: `{referrer_stats['balance']}` coins",
                    parse_mode="Markdown"
                )
        except Exception as e:
            logger.error(f"Failed to notify referrer {referred_by}: {e}")
    
    await message.answer(welcome_text, reply_markup=main_keyboard(), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🏠 **Main Menu**\n\nChoose an option:",
        reply_markup=main_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "refresh_stories")
async def refresh_stories(callback: types.CallbackQuery):
    await callback.message.edit_text("🔄 Refreshing stories from website...\n\n⏳ Please wait...")
    
    FETCHER.last_update = None
    stories = await FETCHER.get_stories()
    
    if stories:
        await callback.message.edit_text(
            f"✅ **{len(stories)} stories found!**\n\n✨ Tap any story to read:",
            reply_markup=story_keyboard(stories),
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            "❌ **No stories found on website!**\n\nCheck website: sexstory.lovable.app",
            reply_markup=main_keyboard(),
            parse_mode="Markdown"
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "stories")
async def stories(callback: types.CallbackQuery):
    await callback.message.edit_text("📖 Loading stories from website...\n\n⏳ Please wait...")
    
    stories_list = await FETCHER.get_stories()

    if not stories_list:
        await callback.message.edit_text(
            "❌ **No stories available on website!**\n\n🔗 Check website: sexstory.lovable.app",
            reply_markup=main_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    await callback.message.edit_text(
        f"📚 **Stories from Website** ({len(stories_list)} found)\n\n"
        f"✨ Tap any story to read & earn `{STORY_READ_REWARD}` coin!\n"
        f"📖 Max `{DAILY_STORY_LIMIT}` stories/day\n\n"
        f"👇 Choose a story:",
        reply_markup=story_keyboard(stories_list),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "balance")
async def balance(callback: types.CallbackQuery):
    stats = DB.get_user_stats(callback.from_user.id)
    upi = DB.get_upi_id(callback.from_user.id)
    
    if stats:
        upi_text = f"\n💳 UPI: `{upi if upi else 'Not set'}`" if upi else "\n💳 UPI: `Not set`"
        text = (
            f"💰 **Your Balance:** `{stats['balance']}` coins\n\n"
            f"📊 **Statistics:**\n"
            f"• Total read: `{stats['total_read']}`\n"
            f"• Today: `{stats['today_read']}/{DAILY_STORY_LIMIT}`\n"
            f"• Referrals: `{stats['referrals']}`\n"
            f"• Pending withdrawals: `{stats['pending_withdrawals']}`{upi_text}\n\n"
            f"💸 Min withdrawal: `{MINIMUM_WITHDRAWAL}` coins"
        )
    else:
        text = "💰 Balance: `0` coins"
    
    await callback.message.edit_text(text, reply_markup=main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "stats")
async def show_stats(callback: types.CallbackQuery):
    stats = DB.get_user_stats(callback.from_user.id)
    
    if stats:
        text = (
            f"📊 **Your Reading Stats**\n\n"
            f"📚 Total: `{stats['total_read']}`\n"
            f"📖 Today: `{stats['today_read']}/{DAILY_STORY_LIMIT}`\n"
            f"💰 Earnings: `{stats['balance']}` coins\n"
            f"👥 Referrals: `{stats['referrals']}`\n"
            f"🎯 Bonus each: `{REFERRAL_BONUS}` coins"
        )
    else:
        text = "📊 No stats yet. Start reading!"
    
    await callback.message.edit_text(text, reply_markup=main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "referral")
async def referral(callback: types.CallbackQuery):
    stats = DB.get_user_stats(callback.from_user.id)
    bot_username = (await bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start=ref_{callback.from_user.id}"
    
    # Get detailed referral list
    referral_details = DB.get_referral_details(callback.from_user.id)
    
    text = (
        f"👥 **Referral Program**\n\n"
        f"🎁 `{REFERRAL_BONUS}` coins per referral!\n\n"
        f"🔗 **Your link:**\n"
        f"`{referral_link}`\n\n"
        f"👥 Total Referrals: `{stats['referrals'] if stats else 0}`\n\n"
    )
    
    if referral_details:
        text += "📋 **Your Referrals:**\n"
        for ref in referral_details[:10]:  # Show last 10
            status = "✅" if ref['rewarded'] else "⏳"
            text += f"{status} {ref['name']} - {ref['date']}\n"
    
    text += "\nShare and earn!"
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📤 Share", switch_inline_query=f"Join me! {referral_link}")],
            [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
        ]
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "leaderboard")
async def leaderboard(callback: types.CallbackQuery):
    leaders = DB.get_leaderboard(10)
    
    if not leaders:
        text = "🏆 **Leaderboard**\n\nNo readers yet! Be first! 🚀"
    else:
        text = "🏆 **Top Readers** 🏆\n\n"
        for i, leader in enumerate(leaders, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "📖"
            text += f"{medal} {i}. {leader['name']}: `{leader['stories']}` stories\n"
    
    await callback.message.edit_text(text, reply_markup=main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "withdraw")
async def withdraw_request(callback: types.CallbackQuery, state: FSMContext):
    balance = DB.get_balance(callback.from_user.id)
    
    if balance < MINIMUM_WITHDRAWAL:
        await callback.message.edit_text(
            f"❌ **Insufficient balance!**\n\n"
            f"💰 Your balance: `{balance}`\n"
            f"💸 Minimum: `{MINIMUM_WITHDRAWAL}` coins\n\n"
            f"📚 Read more stories to earn!",
            reply_markup=main_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return
    
    upi_id = DB.get_upi_id(callback.from_user.id)
    
    if upi_id:
        await callback.message.edit_text(
            f"💸 **Withdrawal Request**\n\n"
            f"💰 Balance: `{balance}` coins\n"
            f"💳 UPI: `{upi_id}`\n"
            f"💸 Minimum: `{MINIMUM_WITHDRAWAL}` coins\n\n"
            f"✏️ Enter amount to withdraw:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="back_to_menu")]]
            ),
            parse_mode="Markdown"
        )
        await state.update_data(upi_id=upi_id)
        await state.set_state(WithdrawState.amount)
    else:
        await callback.message.edit_text(
            f"💸 **Withdrawal Request**\n\n"
            f"💰 Balance: `{balance}` coins\n"
            f"💸 Minimum: `{MINIMUM_WITHDRAWAL}` coins\n\n"
            f"📱 **Please enter your UPI ID:**\n"
            f"(e.g., name@okhdfcbank or 9876543210@paytm)\n\n"
            f"Type /cancel to cancel",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="back_to_menu")]]
            ),
            parse_mode="Markdown"
        )
        await state.set_state(WithdrawState.upi_id)
    
    await callback.answer()

@dp.message(WithdrawState.upi_id)
async def process_upi_id(message: types.Message, state: FSMContext):
    upi_id = message.text.strip()
    
    # Basic UPI ID validation
    if '@' not in upi_id or len(upi_id) < 5:
        await message.answer(
            "❌ **Invalid UPI ID!**\n\n"
            "Valid examples:\n"
            "• `username@okhdfcbank`\n"
            "• `9876543210@paytm`\n"
            "• `name@ybl`\n\n"
            "Try again or /cancel",
            parse_mode="Markdown"
        )
        return
    
    DB.update_upi_id(message.from_user.id, upi_id)
    balance = DB.get_balance(message.from_user.id)
    
    await message.answer(
        f"✅ UPI ID saved: `{upi_id}`\n\n"
        f"💰 Balance: `{balance}` coins\n"
        f"💸 Minimum: `{MINIMUM_WITHDRAWAL}` coins\n\n"
        f"✏️ **Enter amount to withdraw:**",
        reply_markup=main_keyboard(),
        parse_mode="Markdown"
    )
    
    await state.update_data(upi_id=upi_id)
    await state.set_state(WithdrawState.amount)

@dp.message(WithdrawState.amount)
async def process_withdrawal(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        
        if amount < MINIMUM_WITHDRAWAL:
            await message.answer(
                f"❌ Minimum `{MINIMUM_WITHDRAWAL}` coins required!\n\nTry again or /cancel",
                parse_mode="Markdown"
            )
            return
        
        data = await state.get_data()
        upi_id = data.get('upi_id') or DB.get_upi_id(message.from_user.id)
        
        if not upi_id:
            await message.answer("❌ UPI ID missing! Please start withdrawal again.", reply_markup=main_keyboard())
            await state.clear()
            return
        
        balance = DB.get_balance(message.from_user.id)
        if amount > balance:
            await message.answer(
                f"❌ Insufficient balance!\n\n💰 Your balance: `{balance}` coins\n💰 Requested: `{amount}` coins",
                parse_mode="Markdown"
            )
            return
        
        success = DB.request_withdrawal(message.from_user.id, amount, upi_id)
        
        if success:
            await message.answer(
                f"✅ **Withdrawal request submitted!**\n\n"
                f"💰 Amount: `{amount}` coins\n"
                f"💳 UPI ID: `{upi_id}`\n\n"
                f"⏳ Admin will process within 24-48 hours.\n"
                f"Thank you! 🙏",
                reply_markup=main_keyboard(),
                parse_mode="Markdown"
            )
            
            # Notify admins
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"💰 **New Withdrawal Request!**\n\n"
                        f"👤 User: `{message.from_user.id}`\n"
                        f"👤 Username: @{message.from_user.username or 'N/A'}\n"
                        f"💰 Amount: `{amount}` coins\n"
                        f"💳 UPI ID: `{upi_id}`\n"
                        f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        parse_mode="Markdown"
                    )
                except:
                    pass
        else:
            await message.answer(
                "❌ **Withdrawal failed!**\n\n"
                "Possible reasons:\n"
                "• Insufficient balance\n"
                "• Pending withdrawal already exists\n"
                "• Invalid amount\n\n"
                "Please try again later.",
                reply_markup=main_keyboard(),
                parse_mode="Markdown"
            )
    except ValueError:
        await message.answer(
            "❌ Please enter a valid number!\n"
            "Type /cancel to cancel",
            reply_markup=main_keyboard()
        )
    
    await state.clear()

@dp.message(Command("cancel"))
async def cancel(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Operation cancelled!", reply_markup=main_keyboard())

# ================= ADMIN HANDLERS =================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ You are not authorized to use this command!")
        return
    
    await message.answer(
        "🔧 **Admin Panel**\n\nSelect an option:",
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📢 **Broadcast Message**\n\n"
        "Send the message you want to broadcast to all users.\n\n"
        "Supported: Text, Photos, Videos, Documents\n"
        "Type /cancel to cancel.",
        parse_mode="Markdown"
    )
    await state.set_state(AdminState.broadcast)
    await callback.answer()

@dp.message(AdminState.broadcast)
async def admin_broadcast_send(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Unauthorized!")
        await state.clear()
        return
    
    users = DB.get_all_users()
    
    if not users:
        await message.answer("❌ No users found in database!")
        await state.clear()
        return
    
    success_count = 0
    fail_count = 0
    status_msg = await message.answer(f"📡 Sending broadcast to {len(users)} users...")
    
    for user_id in users:
        try:
            await message.copy_to(user_id)
            success_count += 1
        except Exception as e:
            fail_count += 1
            logger.error(f"Failed to send to {user_id}: {e}")
        await asyncio.sleep(0.05)
    
    await status_msg.edit_text(
        f"✅ **Broadcast Completed!**\n\n"
        f"📨 Sent: `{success_count}`\n"
        f"❌ Failed: `{fail_count}`",
        parse_mode="Markdown"
    )
    
    await state.clear()
    
    await message.answer(
        "🔧 **Admin Panel**",
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data == "admin_withdrawals")
async def admin_withdrawals(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    withdrawals = DB.get_pending_withdrawals()
    
    if not withdrawals:
        await callback.message.edit_text(
            "✅ **No pending withdrawals!**",
            reply_markup=admin_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return
    
    text = "💵 **Pending Withdrawals:**\n\n"
    buttons = []
    
    for w in withdrawals:
        text += f"🆔 ID: `{w['id']}`\n"
        text += f"👤 User: `{w['user_id']}`\n"
        text += f"💰 Amount: `{w['amount']}` coins\n"
        text += f"💳 UPI: `{w['upi_id']}`\n"
        text += f"📅 Date: {w['date']}\n\n"
        
        buttons.append([
            InlineKeyboardButton(f"✅ Approve #{w['id']}", callback_data=f"approve_{w['id']}"),
            InlineKeyboardButton(f"❌ Reject #{w['id']}", callback_data=f"reject_{w['id']}")
        ])
    
    buttons.append([InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back")])
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="Markdown"
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
        await callback.answer("✅ Withdrawal approved!", show_alert=True)
    else:
        await callback.answer("❌ Failed to approve!", show_alert=True)
    
    # Refresh withdrawals list
    await admin_withdrawals(callback)

@dp.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_withdrawal(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    withdrawal_id = int(callback.data.split("_")[1])
    success = DB.reject_withdrawal(withdrawal_id)
    
    if success:
        await callback.answer("✅ Withdrawal rejected & refunded!", show_alert=True)
    else:
        await callback.answer("❌ Failed to reject!", show_alert=True)
    
    # Refresh withdrawals list
    await admin_withdrawals(callback)

@dp.callback_query(lambda c: c.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    total_users = DB.get_total_users()
    total_stories = DB.get_total_stories_read()
    total_balance = DB.get_total_balance()
    pending_withdrawals = len(DB.get_pending_withdrawals())
    
    # Get total referrals
    conn = DB.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM referrals")
    total_referrals = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM referrals WHERE reward_given = 1")
    rewarded_referrals = cursor.fetchone()[0]
    conn.close()
    
    text = (
        f"📊 **Bot Statistics**\n\n"
        f"👥 Total users: `{total_users}`\n"
        f"📚 Total stories read: `{total_stories}`\n"
        f"💰 Total balance: `{total_balance}` coins\n"
        f"💸 Pending withdrawals: `{pending_withdrawals}`\n"
        f"👥 Total referrals: `{total_referrals}`\n"
        f"✅ Rewarded referrals: `{rewarded_referrals}`\n\n"
        f"⚙️ **Settings:**\n"
        f"• Daily limit: `{DAILY_STORY_LIMIT}` stories\n"
        f"• Reward per story: `{STORY_READ_REWARD}` coin\n"
        f"• Referral bonus: `{REFERRAL_BONUS}` coins\n"
        f"• Minimum withdrawal: `{MINIMUM_WITHDRAWAL}` coins"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_refresh")
async def admin_refresh(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    await callback.message.edit_text("🔄 Refreshing stories from website...\n⏳ Please wait...")
    
    FETCHER.last_update = None
    stories = await FETCHER.get_stories()
    
    if stories:
        await callback.message.edit_text(
            f"✅ **{len(stories)} stories found!**\n\n"
            f"Stories are now available for users.",
            reply_markup=admin_keyboard(),
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            f"❌ **No stories found on website!**\n\n"
            f"Please check if website has stories:\n"
            f"🔗 {WEBSITE_URL}",
            reply_markup=admin_keyboard(),
            parse_mode="Markdown"
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_debug")
async def admin_debug(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):  # FIXED: from_user (underscore)
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    await callback.message.edit_text("🔍 Debugging website...\n⏳ Please wait...")
    
    try:
        driver = FETCHER.get_driver()
        if driver:
            driver.get(WEBSITE_URL)
            time.sleep(5)
            
            links = driver.find_elements(By.TAG_NAME, "a")
            story_links = []
            
            for link in links:
                try:
                    href = link.get_attribute("href")
                    title = link.text.strip()
                    if href and "/story/" in href:
                        story_links.append(f"• {href[:80]}\n  Title: '{title[:50]}'")
                except:
                    continue
            
            driver.quit()
            
            debug_text = f"🔍 **Selenium Debug Info**\n\n"
            debug_text += f"📄 Website: {WEBSITE_URL}\n"
            debug_text += f"🔗 Story links found: {len(story_links)}\n\n"
            
            if story_links:
                debug_text += "**Story URLs:**\n" + "\n".join(story_links[:10])
            else:
                debug_text += "❌ No story links found on homepage!\n\n"
                debug_text += "Check if website has '/story/' links."
            
            await callback.message.edit_text(
                debug_text[:4000],
                reply_markup=admin_keyboard(),
                parse_mode="Markdown"
            )
        else:
            await callback.message.edit_text(
                "❌ Failed to initialize Selenium!\n\nPlease check Chrome installation.",
                reply_markup=admin_keyboard()
            )
            
    except Exception as e:
        await callback.message.edit_text(
            f"❌ Debug error: {e}",
            reply_markup=admin_keyboard()
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_users")
async def admin_users(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    users = DB.get_all_users_list(20)
    
    if not users:
        await callback.message.edit_text(
            "📋 **No users found!**",
            reply_markup=admin_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return
    
    text = "📋 **Recent Users:**\n\n"
    for user in users:
        text += f"🆔 `{user[0]}` | 👤 {user[2] or 'N/A'}\n"
        text += f"💰 Balance: `{user[3]}` | 📚 Read: `{user[4]}` stories\n"
        if user[1]:
            text += f"📱 @{user[1]}\n"
        text += "\n"
    
    await callback.message.edit_text(
        text[:3000],
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_test_referral")
async def admin_test_referral(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    conn = DB.get_conn()
    cursor = conn.cursor()
    
    # Total referrals
    cursor.execute("SELECT COUNT(*) FROM referrals")
    total_referrals = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM referrals WHERE reward_given = 1")
    rewarded = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM referrals WHERE reward_given = 0")
    unrewarded = cursor.fetchone()[0]
    
    # Recent 10 referrals
    cursor.execute("""
        SELECT r.id, r.referrer_id, r.referred_id, r.date, r.reward_given,
               u1.first_name as ref_name, u2.first_name as refd_name,
               u1.balance as ref_balance
        FROM referrals r
        LEFT JOIN users u1 ON r.referrer_id = u1.user_id
        LEFT JOIN users u2 ON r.referred_id = u2.user_id
        ORDER BY r.id DESC
        LIMIT 10
    """)
    
    recent = cursor.fetchall()
    
    text = "📊 Referral Statistics\n\n"
    text += f"📝 Total referrals: {total_referrals}\n"
    text += f"✅ Rewarded: {rewarded}\n"
    text += f"⚠️ Unrewarded: {unrewarded}\n\n"
    
    if recent:
        text += "📋 Recent 10 Referrals:\n\n"
        for r in recent:
            status = "✅" if r[4] else "❌"
            ref_name = r[5] if r[5] else f"User{r[1]}"
            refd_name = r[6] if r[6] else f"User{r[2]}"
            text += f"{status} ID:{r[0]} | {ref_name} -> {refd_name}\n"
            text += f"   Date: {r[3]} | Ref Balance: {r[7] if r[7] else 0}\n\n"
    else:
        text += "❌ No referrals found in database!\n\n"
        text += "⚠️ Referral system not working!\n"
        text += "Check /start command with ref_ parameter"
    
    conn.close()
    
    await callback.message.edit_text(
        text[:4000],
        reply_markup=admin_keyboard()
    )
    await callback.answer()
@dp.callback_query(lambda c: c.data == "admin_all_referrals")
async def admin_all_referrals(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    referrals = DB.get_all_referrals_for_admin()
    
    if not referrals:
        await callback.message.edit_text(
            "📋 **No referrals found!**",
            reply_markup=admin_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return
    
    text = "👥 **All Referrals (Last 30):**\n\n"
    for ref in referrals:
        status = "✅" if ref[3] else "❌"
        text += f"{status} {ref[4] or 'User'+str(ref[0])} referred {ref[5] or 'User'+str(ref[1])}\n"
        text += f"   Date: {ref[2]}\n\n"
    
    await callback.message.edit_text(
        text[:4000],
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔧 **Admin Panel**\n\nSelect an option:",
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()

# Track story reads from WebApp
@dp.message(lambda message: message.web_app_data)
async def handle_web_app_data(message: types.Message):
    try:
        data = json.loads(message.web_app_data.data)
        story_url = data.get('url', '')
        
        if story_url:
            rewarded, reason = DB.reward_story(message.from_user.id, story_url)
            
            if rewarded:
                await message.answer(
                    f"✅ **+{STORY_READ_REWARD} coin earned!**\n"
                    f"💰 New balance: `{DB.get_balance(message.from_user.id)}` coins",
                    parse_mode="Markdown",
                    disable_notification=True
                )
            elif reason == "daily_limit":
                await message.answer(
                    f"⚠️ **Daily limit reached!**\n"
                    f"You've read `{DAILY_STORY_LIMIT}` stories today.\n"
                    f"Come back tomorrow! 🌙",
                    parse_mode="Markdown",
                    disable_notification=True
                )
    except json.JSONDecodeError:
        pass
    except Exception as e:
        logger.error(f"WebApp data error: {e}")

# Track story reads from shared links
@dp.message(lambda message: message.text and WEBSITE_URL in message.text)
async def track_story_read(message: types.Message):
    story_url = None
    for word in message.text.split():
        if WEBSITE_URL in word and '/story/' in word:
            story_url = word
            break
    
    if story_url:
        rewarded, reason = DB.reward_story(message.from_user.id, story_url)
        
        if rewarded:
            await message.reply(
                f"✅ **+{STORY_READ_REWARD} coin earned!**\n"
                f"💰 New balance: `{DB.get_balance(message.from_user.id)}` coins",
                parse_mode="Markdown",
                disable_notification=True
            )
        elif reason == "daily_limit":
            await message.reply(
                f"⚠️ **Daily limit reached!**\n"
                f"You've read `{DAILY_STORY_LIMIT}` stories today.\n"
                f"Come back tomorrow! 🌙",
                parse_mode="Markdown",
                disable_notification=True
            )

# ================= MAIN =================
async def main():
    logger.info("🚀 Bot starting...")
    
    # Pre-fetch stories
    logger.info("📚 Fetching stories from website...")
    stories = await FETCHER.get_stories()
    logger.info(f"✅ Found {len(stories)} stories")
    
    # Test database connection and referrals
    conn = DB.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM referrals")
    ref_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]
    conn.close()
    
    logger.info(f"📊 Database Status: {user_count} users, {ref_count} referrals")
    
    # Set bot commands
    await bot.set_my_commands([
        types.BotCommand(command="start", description="🚀 Start the bot"),
        types.BotCommand(command="cancel", description="❌ Cancel current operation"),
        types.BotCommand(command="admin", description="🔧 Admin panel (admins only)")
    ])
    
    # Remove webhook and start polling
    await bot.delete_webhook(drop_pending_updates=True)
    
    logger.info("✅ Bot is running with all features! Referral system is active.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
