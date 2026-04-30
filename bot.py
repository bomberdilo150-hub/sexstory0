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
STORY_READ_REWARD = 1  # 1 Rupee per story read
DAILY_STORY_LIMIT = 10  # Max stories per day per user

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= STORY FETCHER =================
class StoryFetcher:
    def __init__(self):
        self.stories_cache = {}
        self.last_fetch = None
    
    async def fetch_stories_from_website(self):
        """Fetch stories directly from website"""
        stories = []
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                }
                
                # Try multiple endpoints for stories
                endpoints = [
                    WEBSITE_URL,
                    f"{WEBSITE_URL}/stories",
                    f"{WEBSITE_URL}/all-stories",
                    f"{WEBSITE_URL}/category/all",
                    f"{WEBSITE_URL}/posts",
                    f"{WEBSITE_URL}/blog"
                ]
                
                for endpoint in endpoints:
                    try:
                        logger.info(f"Fetching from: {endpoint}")
                        async with session.get(endpoint, headers=headers, timeout=15) as response:
                            if response.status == 200:
                                html = await response.text()
                                soup = BeautifulSoup(html, 'html.parser')
                                
                                # Strategy 1: Look for article/story containers
                                story_containers = soup.find_all(['article', 'div', 'section'], 
                                    class_=re.compile(r'story|post|article|card|content-item|entry', re.I))
                                
                                for container in story_containers:
                                    # Find links within container
                                    links = container.find_all('a', href=True)
                                    for link in links:
                                        href = link.get('href', '')
                                        title = link.get_text(strip=True)
                                        
                                        # Also check for title elements
                                        if not title:
                                            title_elem = link.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p'])
                                            if title_elem:
                                                title = title_elem.get_text(strip=True)
                                        
                                        if title and len(title) > 5 and len(title) < 200:
                                            full_url = href if href.startswith('http') else WEBSITE_URL.rstrip('/') + href
                                            story_id = abs(hash(full_url)) % 100000
                                            
                                            # Try to find description
                                            desc_elem = container.find(['p', 'span', 'div'], 
                                                class_=re.compile(r'desc|excerpt|summary|content', re.I))
                                            snippet = desc_elem.get_text(strip=True)[:150] if desc_elem else 'Click to read full story...'
                                            
                                            stories.append({
                                                'id': story_id,
                                                'title': title.strip()[:80],
                                                'url': full_url,
                                                'snippet': snippet
                                            })
                                
                                # Strategy 2: Find all story links directly
                                all_links = soup.find_all('a', href=True)
                                for link in all_links:
                                    href = link.get('href', '')
                                    text = link.get_text(strip=True)
                                    
                                    # Look for story-like URLs
                                    if re.search(r'/(story|post|read|article|blog)/', href, re.I):
                                        if text and len(text) > 5 and len(text) < 200:
                                            full_url = href if href.startswith('http') else WEBSITE_URL.rstrip('/') + href
                                            story_id = abs(hash(full_url)) % 100000
                                            
                                            if not any(s['url'] == full_url for s in stories):
                                                stories.append({
                                                    'id': story_id,
                                                    'title': text.strip()[:80],
                                                    'url': full_url,
                                                    'snippet': 'Click to read full story...'
                                                })
                                
                                # Strategy 3: Find heading elements with links
                                headings = soup.find_all(['h1', 'h2', 'h3', 'h4'])
                                for heading in headings:
                                    link = heading.find('a', href=True)
                                    if link:
                                        href = link.get('href', '')
                                        text = heading.get_text(strip=True)
                                        if text and len(text) > 10 and len(text) < 200:
                                            full_url = href if href.startswith('http') else WEBSITE_URL.rstrip('/') + href
                                            story_id = abs(hash(full_url)) % 100000
                                            
                                            if not any(s['url'] == full_url for s in stories):
                                                stories.append({
                                                    'id': story_id,
                                                    'title': text.strip()[:80],
                                                    'url': full_url,
                                                    'snippet': 'Click to read full story...'
                                                })
                                
                                if len(stories) > 5:
                                    logger.info(f"Found {len(stories)} stories from {endpoint}")
                                    break
                                    
                    except Exception as e:
                        logger.warning(f"Failed to fetch from {endpoint}: {e}")
                        continue
                
                # If stories found, cache them
                if stories:
                    # Remove duplicates
                    seen_urls = set()
                    unique_stories = []
                    for story in stories:
                        if story['url'] not in seen_urls:
                            seen_urls.add(story['url'])
                            unique_stories.append(story)
                    
                    # Cache stories by ID
                    self.stories_cache = {}
                    for story in unique_stories[:50]:
                        self.stories_cache[story['id']] = story
                    
                    self.last_fetch = datetime.now()
                    logger.info(f"Successfully fetched and cached {len(unique_stories)} stories")
                    return list(self.stories_cache.values())
                
        except Exception as e:
            logger.error(f"Story fetch error: {e}")
        
        # If no stories found and no cache, return empty
        if not self.stories_cache:
            logger.warning("No stories found and no cache available")
            return []
        
        # Return cached stories if available
        logger.info("Returning cached stories")
        return list(self.stories_cache.values())
    
    async def get_stories(self, force_refresh=False, page=1, per_page=5):
        """Get stories with pagination"""
        try:
            if force_refresh or not self.stories_cache or not self.last_fetch:
                stories_list = await self.fetch_stories_from_website()
            else:
                stories_list = list(self.stories_cache.values())
            
            if not stories_list:
                return {
                    'stories': [],
                    'total': 0,
                    'page': page,
                    'total_pages': 0
                }
            
            total = len(stories_list)
            total_pages = max(1, (total + per_page - 1) // per_page)
            
            # Adjust page if out of bounds
            if page < 1:
                page = 1
            elif page > total_pages:
                page = total_pages
            
            start = (page - 1) * per_page
            end = start + per_page
            
            return {
                'stories': stories_list[start:end],
                'total': total,
                'page': page,
                'total_pages': total_pages
            }
            
        except Exception as e:
            logger.error(f"Error in get_stories: {e}")
            return {
                'stories': [],
                'total': 0,
                'page': 1,
                'total_pages': 0
            }
    
    async def get_story_by_id(self, story_id):
        """Get specific story by ID"""
        try:
            if not self.stories_cache:
                await self.fetch_stories_from_website()
            return self.stories_cache.get(story_id)
        except Exception as e:
            logger.error(f"Error getting story by ID: {e}")
            return None

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
                is_banned BOOLEAN DEFAULT 0,
                stories_read_today INTEGER DEFAULT 0,
                last_story_date DATE
            )
        ''')
        
        # Stories read history
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS story_reads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                story_id INTEGER,
                read_date TIMESTAMP,
                reward_given BOOLEAN DEFAULT 0
            )
        ''')
        
        # Completed stories
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS completed_stories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                story_id INTEGER,
                story_title TEXT,
                completed_date TIMESTAMP,
                UNIQUE(user_id, story_id)
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
    
    def can_reward_story(self, user_id: int) -> bool:
        """Check if user can get reward for reading story"""
        conn = self.get_conn()
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT stories_read_today, last_story_date 
            FROM users WHERE user_id = ?
        """, (user_id,))
        
        result = cursor.fetchone()
        if result:
            stories_today, last_date = result
            
            # Reset counter if new day
            if last_date != today:
                cursor.execute("""
                    UPDATE users SET stories_read_today = 0, last_story_date = ?
                    WHERE user_id = ?
                """, (today, user_id))
                conn.commit()
                conn.close()
                return True
            
            if stories_today < DAILY_STORY_LIMIT:
                conn.close()
                return True
        
        conn.close()
        return False
    
    def record_story_read(self, user_id: int, story_id: int, rewarded: bool = False):
        """Record that user read a story"""
        conn = self.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO story_reads (user_id, story_id, read_date, reward_given)
            VALUES (?, ?, ?, ?)
        """, (user_id, story_id, datetime.now(), rewarded))
        
        if rewarded:
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                UPDATE users SET stories_read_today = stories_read_today + 1, last_story_date = ?
                WHERE user_id = ?
            """, (today, user_id))
            self.add_balance(user_id, STORY_READ_REWARD)
        
        conn.commit()
        conn.close()
    
    def complete_story(self, user_id: int, story_id: int, story_title: str) -> bool:
        """Mark story as completed and give reward"""
        conn = self.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO completed_stories (user_id, story_id, story_title, completed_date)
                VALUES (?, ?, ?, ?)
            """, (user_id, story_id, story_title, datetime.now()))
            
            if cursor.rowcount > 0:
                # Only reward if story wasn't completed before
                if self.can_reward_story(user_id):
                    self.record_story_read(user_id, story_id, rewarded=True)
                    conn.commit()
                    conn.close()
                    return True
            
            conn.commit()
            conn.close()
            return False
            
        except Exception as e:
            logger.error(f"Error completing story: {e}")
            conn.close()
            return False
    
    def get_completed_stories(self, user_id: int):
        """Get list of completed stories for user"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT story_id, story_title, completed_date 
            FROM completed_stories 
            WHERE user_id = ?
            ORDER BY completed_date DESC
            LIMIT 20
        """, (user_id,))
        results = cursor.fetchall()
        conn.close()
        return results
    
    def get_stories_read_today(self, user_id: int) -> int:
        """Get number of stories read today"""
        conn = self.get_conn()
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT stories_read_today, last_story_date FROM users WHERE user_id = ?
        """, (user_id,))
        
        result = cursor.fetchone()
        if result:
            stories_today, last_date = result
            if last_date == today:
                conn.close()
                return stories_today
        
        conn.close()
        return 0
    
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
            SELECT balance, total_earned, total_withdrawn, referral_count, joined_date, stories_read_today
            FROM users WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            completed = len(self.get_completed_stories(user_id))
            return {
                'balance': result[0],
                'total_earned': result[1],
                'total_withdrawn': result[2],
                'referral_count': result[3],
                'joined_date': result[4],
                'stories_today': result[5] if result[5] else 0,
                'completed_stories': completed
            }
        return {
            'balance': 0, 'total_earned': 0, 'total_withdrawn': 0,
            'referral_count': 0, 'joined_date': datetime.now(),
            'stories_today': 0, 'completed_stories': 0
        }
    
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
story_fetcher = StoryFetcher()

def get_main_keyboard(is_admin: bool = False):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📱 Open Stories App",
                web_app=WebAppInfo(url=WEBSITE_URL)
            )
        ],
        [
            InlineKeyboardButton(text="📚 Browse Stories", callback_data="stories_list"),
            InlineKeyboardButton(text="📖 My Reads", callback_data="my_stories")
        ],
        [
            InlineKeyboardButton(text="💰 Refer & Earn", callback_data="referral"),
            InlineKeyboardButton(text="💳 Balance", callback_data="balance")
        ],
        [
            InlineKeyboardButton(text="📊 Stats", callback_data="stats"),
            InlineKeyboardButton(text="🏧 Withdraw", callback_data="withdraw")
        ],
        [
            InlineKeyboardButton(text="❓ Help", callback_data="help")
        ]
    ])
    
    if is_admin:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text="👑 Admin", callback_data="admin_panel")
        ])
    
    return keyboard

def get_stories_keyboard(story_data, page=1):
    """Create paginated stories keyboard"""
    stories = story_data['stories']
    total_pages = story_data['total_pages']
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    for story in stories:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"📖 {story['title'][:40]}", 
                callback_data=f"read_story_{story['id']}"
            )
        ])
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"🌐 Read on Web", 
                url=story['url']
            ),
            InlineKeyboardButton(
                text="✅ Complete", 
                callback_data=f"complete_story_{story['id']}"
            )
        ])
    
    # Pagination buttons
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"stories_page_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="page_info"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"stories_page_{page+1}"))
    
    if nav_buttons:
        keyboard.inline_keyboard.append(nav_buttons)
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="stories_refresh"),
        InlineKeyboardButton(text="◀️ Back", callback_data="back")
    ])
    
    return keyboard

# ================= HANDLERS =================
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
    stories_today = db.get_stories_read_today(user.id)
    
    text = f"""🌟 *Welcome {user.first_name}!* 🌟

💰 Balance: ₹{balance}
📚 Stories Today: {stories_today}/{DAILY_STORY_LIMIT}
🎁 Earn ₹{STORY_READ_REWARD} per story read!

*How to earn:*
📖 Read & complete stories - ₹{STORY_READ_REWARD}/story
👥 Refer friends - ₹{REFERRAL_BONUS}/referral
💳 Min withdrawal: ₹{MINIMUM_WITHDRAWAL}

Start reading now! 👇"""
    
    await message.answer(text, reply_markup=get_main_keyboard(db.is_admin(user.id)), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "stories_list")
async def stories_list(callback: types.CallbackQuery):
    await callback.answer("📚 Fetching latest stories from website...")
    
    # Show loading message
    loading_msg = await callback.message.edit_text(
        "🔄 *Fetching stories from website...*\nPlease wait...",
        parse_mode="Markdown"
    )
    
    story_data = await story_fetcher.get_stories(force_refresh=True, page=1)
    
    if story_data['stories']:
        stories_today = Database(DATABASE_FILE).get_stories_read_today(callback.from_user.id)
        
        await loading_msg.edit_text(
            f"📚 *Latest Stories*\n"
            f"📖 Read today: {stories_today}/{DAILY_STORY_LIMIT}\n"
            f"🎁 Reward: ₹{STORY_READ_REWARD}/story\n"
            f"📊 Total stories: {story_data['total']}",
            reply_markup=get_stories_keyboard(story_data, 1),
            parse_mode="Markdown"
        )
    else:
        await loading_msg.edit_text(
            "❌ *No stories found on website!*\n\n"
            "Please try again later or contact admin.\n"
            "The website might be temporarily unavailable.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Try Again", callback_data="stories_list")],
                [InlineKeyboardButton(text="◀️ Back to Menu", callback_data="back")]
            ]),
            parse_mode="Markdown"
        )

@dp.callback_query(lambda c: c.data.startswith("stories_page_"))
async def stories_page(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    
    story_data = await story_fetcher.get_stories(page=page)
    
    if story_data['stories']:
        await callback.message.edit_reply_markup(
            reply_markup=get_stories_keyboard(story_data, page)
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "stories_refresh")
async def stories_refresh(callback: types.CallbackQuery):
    await callback.answer("🔄 Fetching fresh stories from website...")
    
    story_data = await story_fetcher.get_stories(force_refresh=True, page=1)
    
    if story_data['stories']:
        await callback.message.edit_text(
            f"📚 *Latest Stories (Refreshed)*\n"
            f"Total stories found: {story_data['total']}\n"
            f"🎁 Earn ₹{STORY_READ_REWARD} per complete story!",
            reply_markup=get_stories_keyboard(story_data, 1),
            parse_mode="Markdown"
        )
    else:
        await callback.answer("❌ Failed to fetch stories!", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("read_story_"))
async def read_story(callback: types.CallbackQuery):
    story_id = int(callback.data.split("_")[2])
    story = await story_fetcher.get_story_by_id(story_id)
    
    if story:
        text = f"""📖 *{story['title']}*

📝 *Summary:*
{story.get('snippet', 'An exciting story awaits!')}

*How to complete:*
1. Click 'Read on Web' to open story
2. Read the full story
3. Click '✅ Complete' when done
4. Earn ₹{STORY_READ_REWARD} reward!

💡 Make sure to read the complete story before marking it done."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌐 Read Full Story", url=story['url'])],
            [InlineKeyboardButton(text="✅ Mark as Complete", callback_data=f"complete_story_{story_id}")],
            [InlineKeyboardButton(text="📚 More Stories", callback_data="stories_list")],
            [InlineKeyboardButton(text="◀️ Back", callback_data="stories_list")]
        ])
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    else:
        await callback.answer("Story not found! Try refreshing stories.", show_alert=True)
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("complete_story_"))
async def complete_story(callback: types.CallbackQuery):
    story_id = int(callback.data.split("_")[2])
    story = await story_fetcher.get_story_by_id(story_id)
    
    if not story:
        await callback.answer("Story not found!", show_alert=True)
        return
    
    db = Database(DATABASE_FILE)
    user_id = callback.from_user.id
    
    # Check daily limit
    if not db.can_reward_story(user_id):
        await callback.answer(f"❌ Daily limit reached! Max {DAILY_STORY_LIMIT} stories/day", show_alert=True)
        return
    
    # Try to complete story
    if db.complete_story(user_id, story_id, story['title']):
        balance = db.get_balance(user_id)
        await callback.answer(f"✅ Story completed! +₹{STORY_READ_REWARD} earned!", show_alert=True)
        
        await callback.message.edit_text(
            f"✅ *Story Completed!*\n\n"
            f"📖 {story['title']}\n"
            f"💰 Reward: +₹{STORY_READ_REWARD}\n"
            f"💳 New Balance: ₹{balance}\n\n"
            f"Keep reading to earn more!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📚 Read More", callback_data="stories_list")],
                [InlineKeyboardButton(text="◀️ Back to Menu", callback_data="back")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await callback.answer("Already completed this story!", show_alert=True)

@dp.callback_query(lambda c: c.data == "my_stories")
async def my_stories(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    completed = db.get_completed_stories(callback.from_user.id)
    
    if completed:
        text = "📚 *Your Completed Stories*\n\n"
        for i, (story_id, title, date) in enumerate(completed[:10], 1):
            text += f"{i}. {title[:40]}\n   📅 {date[:10]}\n\n"
        
        text += f"💰 Total Earned: ₹{len(completed) * STORY_READ_REWARD}"
    else:
        text = "📚 *No stories completed yet!*\n\nStart reading to earn rewards!"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Browse Stories", callback_data="stories_list")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "balance")
async def balance_cmd(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""💳 *Your Balance*

💰 Current: ₹{balance}
📈 Total Earned: ₹{stats['total_earned']}
💸 Total Withdrawn: ₹{stats['total_withdrawn']}
👥 Referrals: {stats['referral_count']}
📖 Completed: {stats['completed_stories']} stories
📚 Today: {stats['stories_today']}/{DAILY_STORY_LIMIT} stories

🎁 Earn ₹{STORY_READ_REWARD} per story read!
👥 Earn ₹{REFERRAL_BONUS} per referral
💳 Min withdrawal: ₹{MINIMUM_WITHDRAWAL}"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "stats")
async def stats_cmd(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""📊 *Your Statistics*

💰 Balance: ₹{stats['balance']}
📈 Total Earned: ₹{stats['total_earned']}
💸 Withdrawn: ₹{stats['total_withdrawn']}
👥 Referrals: {stats['referral_count']}
📖 Stories Completed: {stats['completed_stories']}
📚 Today's Reads: {stats['stories_today']}/{DAILY_STORY_LIMIT}
📅 Joined: {str(stats['joined_date'])[:10]}

💪 Keep going! Earn more by reading stories!"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "referral")
async def referral_cmd(callback: types.CallbackQuery):
    bot_user = await bot.get_me()
    link = f"https://t.me/{bot_user.username}?start={callback.from_user.id}"
    db = Database(DATABASE_FILE)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""💰 *Refer & Earn*

🔗 Your Link:
`{link}`

📊 Your Stats:
• Referrals: {stats['referral_count']}
• Total Earned: ₹{stats['total_earned']}

🎁 Rewards:
• Per Referral: ₹{REFERRAL_BONUS}
• Per Story: ₹{STORY_READ_REWARD}

Share and earn!"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Share with Friends", switch_inline_query=f"Join and earn rewards! 📚💰")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "withdraw")
async def withdraw_cmd(callback: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    
    if balance < MINIMUM_WITHDRAWAL:
        await callback.answer(f"❌ Minimum withdrawal is ₹{MINIMUM_WITHDRAWAL}! You have ₹{balance}", show_alert=True)
        return
    
    await callback.message.answer(
        f"💰 Your balance: ₹{balance}\n"
        f"Min withdrawal: ₹{MINIMUM_WITHDRAWAL}\n\n"
        f"Enter amount to withdraw:\n"
        f"Type /cancel"
    )
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
        await message.answer("📱 Enter your UPI ID:\nExample: example@upi\nType /cancel")
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
        await message.answer("❌ Invalid UPI ID!")
        return
    
    data = await state.get_data()
    amount = data['amount']
    
    db = Database(DATABASE_FILE)
    
    if db.deduct_balance(message.from_user.id, amount):
        withdraw_id = db.create_withdrawal(message.from_user.id, amount, upi_id)
        
        await message.answer(
            f"✅ *Withdrawal Submitted!*\n\n"
            f"💰 Amount: ₹{amount}\n"
            f"📱 UPI: `{upi_id}`\n"
            f"🆔 ID: #{withdraw_id}\n\n"
            f"Processing in 24-48 hours.",
            parse_mode="Markdown"
        )
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"💰 *New Withdrawal*\n"
                    f"User: {message.from_user.first_name}\n"
                    f"Amount: ₹{amount}\n"
                    f"UPI: {upi_id}\n"
                    f"ID: #{withdraw_id}",
                    parse_mode="Markdown"
                )
            except:
                pass
    else:
        await message.answer("❌ Failed to process!")
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "help")
async def help_cmd(callback: types.CallbackQuery):
    text = f"""📖 *Help Guide*

*Earning:*
📖 Read stories: ₹{STORY_READ_REWARD}/story
👥 Referrals: ₹{REFERRAL_BONUS}/referral
📊 Daily limit: {DAILY_STORY_LIMIT} stories

*Withdrawal:*
💳 Min: ₹{MINIMUM_WITHDRAWAL}
📱 UPI transfer
⏱ 24-48 hours processing

*Commands:*
/start - Restart bot
/help - Help guide"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back")
async def back_cmd(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    
    await callback.message.edit_text(
        f"🌟 *Main Menu*\n\n💰 Balance: ₹{balance}",
        reply_markup=get_main_keyboard(db.is_admin(callback.from_user.id)),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "page_info")
async def page_info(callback: types.CallbackQuery):
    await callback.answer("Navigate using ◀️ ▶️ buttons", show_alert=True)

# Admin handlers
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
    await callback.answer("🔄 Refreshing stories...")
    
    story_data = await story_fetcher.get_stories(force_refresh=True)
    
    if story_data['stories']:
        await callback.message.answer(f"✅ Refreshed {story_data['total']} stories from website!")
    else:
        await callback.message.answer("❌ Failed to fetch stories!")
    
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
        [
            InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{withdraw_id}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{withdraw_id}")
        ],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_withdrawals")]
    ])
    
    text = f"""💰 *Withdrawal #{w[0]}*

👤 User: {w[11]}
📝 @{w[12] or 'N/A'}
💰 Amount: ₹{w[2]}
📱 UPI: {w[3]}
📅 Date: {w[5]}"""
    
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
            await bot.send_message(user_id, f"❌ Withdrawal of ₹{amount} rejected. Amount refunded.")
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

@dp.errors()
async def error_handler(update: types.Update, exception: Exception):
    logger.error(f"Error: {exception}")
    return True

# ================= MAIN =================
async def main():
    logger.info("🚀 Starting Story Bot...")
    
    db = Database(DATABASE_FILE)
    
    # Pre-fetch stories on startup
    story_data = await story_fetcher.get_stories()
    logger.info(f"Pre-loaded {story_data['total']} stories")
    
    logger.info("✅ Bot is ready!")
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
