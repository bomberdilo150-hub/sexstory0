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
import json

# ================= CONFIG =================
API_TOKEN = "8777177819:AAHuJtPJR8VmoWSfqHtrHW7WeVNWJ6sbV7o"
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

# ================= STORY FETCHER =================
class StoryFetcher:
    def __init__(self):
        self.stories_cache = {}
        self.last_fetch = None
        self.base_url = WEBSITE_URL.rstrip('/')
        self.story_page_url = STORY_PAGE_URL
    
    async def fetch_stories_from_website(self):
        """Fetch ALL stories from /story/ page"""
        stories = []
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0',
                }
                
                logger.info(f"🔍 Fetching stories from: {self.story_page_url}")
                
                async with session.get(self.story_page_url, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # Save HTML for debugging
                        with open('story_page_debug.html', 'w', encoding='utf-8') as f:
                            f.write(html)
                        logger.info("📄 Saved HTML to story_page_debug.html for debugging")
                        
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Log all links found
                        all_links = soup.find_all('a', href=True)
                        logger.info(f"🔗 Total links found: {len(all_links)}")
                        
                        # Print first 20 links for debugging
                        for i, link in enumerate(all_links[:20]):
                            href = link.get('href', '')
                            text = link.get_text(strip=True)[:50]
                            logger.info(f"  Link {i+1}: href='{href}' text='{text}'")
                        
                        # Strategy 1: Find UUID pattern links
                        for link in all_links:
                            href = link.get('href', '').strip()
                            
                            # Check for /story/ with UUID
                            uuid_match = re.search(r'/story/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', href, re.IGNORECASE)
                            
                            if uuid_match:
                                full_url = href if href.startswith('http') else self.base_url + href
                                title = link.get_text(strip=True)
                                
                                if not title or len(title) < 3:
                                    title = link.get('title', '').strip() or link.get('aria-label', '').strip()
                                
                                if not title or len(title) < 3:
                                    title = f"Story {len(stories) + 1}"
                                
                                stories.append({
                                    'id': abs(hash(full_url)) % 100000,
                                    'title': title[:80],
                                    'url': full_url,
                                    'snippet': 'Click to read this exciting story...'
                                })
                        
                        logger.info(f"✅ Strategy 1 found {len(stories)} stories")
                        
                        # Strategy 2: Look for any /story/ links (even without UUID)
                        if not stories:
                            logger.info("Trying Strategy 2: Any /story/ links...")
                            for link in all_links:
                                href = link.get('href', '').strip()
                                if '/story/' in href and href.count('/') >= 3:
                                    full_url = href if href.startswith('http') else self.base_url + href
                                    text = link.get_text(strip=True) or f"Story {len(stories) + 1}"
                                    
                                    # Skip the main /story/ page itself
                                    if full_url.rstrip('/') == self.story_page_url.rstrip('/'):
                                        continue
                                    
                                    stories.append({
                                        'id': abs(hash(full_url)) % 100000,
                                        'title': text[:80],
                                        'url': full_url,
                                        'snippet': 'Click to read this exciting story...'
                                    })
                            logger.info(f"✅ Strategy 2 found {len(stories)} stories")
                        
                        # Strategy 3: Check script tags for __NEXT_DATA__ or similar
                        if not stories:
                            logger.info("Trying Strategy 3: Script tag data...")
                            scripts = soup.find_all('script')
                            for script in scripts:
                                if script.string:
                                    # Try to find JSON data
                                    try:
                                        # Look for common patterns
                                        json_patterns = [
                                            r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                                            r'window\.__DATA__\s*=\s*({.*?});',
                                            r'__NEXT_DATA__\s*=\s*({.*?});',
                                        ]
                                        
                                        for pattern in json_patterns:
                                            match = re.search(pattern, script.string, re.DOTALL)
                                            if match:
                                                try:
                                                    data = json.loads(match.group(1))
                                                    self.extract_stories_from_json(data, stories)
                                                except:
                                                    pass
                                        
                                        # Look for story URLs in JavaScript
                                        js_urls = re.findall(r'["\']((?:https?:)?//[^"\']*?/story/[^"\']*?)["\']', script.string)
                                        for url in js_urls:
                                            if url.startswith('//'):
                                                url = 'https:' + url
                                            if url not in [s['url'] for s in stories]:
                                                stories.append({
                                                    'id': abs(hash(url)) % 100000,
                                                    'title': f"Story {len(stories) + 1}",
                                                    'url': url,
                                                    'snippet': 'Click to read this exciting story...'
                                                })
                                    except:
                                        pass
                            logger.info(f"✅ Strategy 3 found {len(stories)} stories")
                        
                    else:
                        logger.error(f"❌ HTTP {response.status} from {self.story_page_url}")
        
        except Exception as e:
            logger.error(f"❌ Fetch error: {e}")
        
        # If still no stories, add the known story as fallback
        if not stories:
            logger.info("Using fallback: Known story URL")
            known_url = f"{self.base_url}/story/268a9216-a142-49b7-91d3-6f0c911218e5"
            stories.append({
                'id': abs(hash(known_url)) % 100000,
                'title': "Story 1",
                'url': known_url,
                'snippet': 'Click to read this exciting story...'
            })
        
        # Cache stories
        if stories:
            seen_urls = set()
            unique_stories = []
            for story in stories:
                if story['url'] not in seen_urls:
                    seen_urls.add(story['url'])
                    unique_stories.append(story)
            
            # Update titles
            for i, story in enumerate(unique_stories, 1):
                if story['title'].startswith('Story ') and story['title'][6:].isdigit():
                    story['title'] = f"Story #{i}"
            
            self.stories_cache = {}
            for story in unique_stories[:50]:
                self.stories_cache[story['id']] = story
            
            self.last_fetch = datetime.now()
            logger.info(f"💾 Cached {len(unique_stories)} unique stories")
            return list(self.stories_cache.values())
        
        logger.warning("⚠️ No stories found")
        return []
    
    def extract_stories_from_json(self, data, stories):
        """Recursively extract story URLs from JSON data"""
        if isinstance(data, dict):
            for key, value in data.items():
                if key in ['stories', 'posts', 'items', 'articles', 'data']:
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                url = item.get('url') or item.get('slug') or item.get('id') or item.get('link')
                                title = item.get('title') or item.get('name') or ''
                                if url and '/story/' in str(url):
                                    full_url = url if url.startswith('http') else f"{self.base_url}/story/{url}"
                                    if full_url not in [s['url'] for s in stories]:
                                        stories.append({
                                            'id': abs(hash(full_url)) % 100000,
                                            'title': title[:80] if title else f"Story {len(stories) + 1}",
                                            'url': full_url,
                                            'snippet': item.get('description', item.get('excerpt', 'Click to read...'))[:150]
                                        })
                self.extract_stories_from_json(value, stories)
        elif isinstance(data, list):
            for item in data:
                self.extract_stories_from_json(item, stories)
    
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
        try:
            if not self.stories_cache:
                await self.fetch_stories_from_website()
            return self.stories_cache.get(story_id)
        except:
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
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS story_reads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                story_id INTEGER,
                read_date TIMESTAMP,
                reward_given BOOLEAN DEFAULT 0
            )
        ''')
        
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
        conn = self.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO completed_stories (user_id, story_id, story_title, completed_date)
                VALUES (?, ?, ?, ?)
            """, (user_id, story_id, story_title, datetime.now()))
            
            if cursor.rowcount > 0:
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
                text="🌐 Read on Web", 
                url=story['url']
            ),
            InlineKeyboardButton(
                text="✅ Complete", 
                callback_data=f"complete_story_{story['id']}"
            )
        ])
    
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"stories_page_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="page_info"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"stories_page_{page+1}"))
    
    if nav_buttons:
        keyboard.inline_keyboard.append(nav_buttons)
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh Stories", callback_data="stories_refresh"),
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

📱 Click 'Open Stories App' to browse all stories!
📚 Or click 'Browse Stories' to see available stories

Start reading now! 👇"""
    
    await message.answer(text, reply_markup=get_main_keyboard(db.is_admin(user.id)), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "stories_list")
async def stories_list(callback: types.CallbackQuery):
    await callback.answer("📚 Fetching stories from website...")
    
    loading_msg = await callback.message.edit_text(
        "🔄 *Fetching stories from /story/ page...*\n"
        "Please wait...",
        parse_mode="Markdown"
    )
    
    story_data = await story_fetcher.get_stories(force_refresh=True, page=1)
    
    if story_data['stories']:
        stories_today = Database(DATABASE_FILE).get_stories_read_today(callback.from_user.id)
        
        await loading_msg.edit_text(
            f"✅ *Stories Found!*\n\n"
            f"📖 Read today: {stories_today}/{DAILY_STORY_LIMIT}\n"
            f"🎁 Reward: ₹{STORY_READ_REWARD}/story\n"
            f"📊 Total stories: {story_data['total']}\n\n"
            f"Click on a story to read:",
            reply_markup=get_stories_keyboard(story_data, 1),
            parse_mode="Markdown"
        )
    else:
        # Show debug info and options
        await loading_msg.edit_text(
            "⚠️ *No stories found on /story/ page*\n\n"
            "Debug info saved to 'story_page_debug.html'\n"
            "Check logs for more details.\n\n"
            "Try these options:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📱 Open Web App", web_app=WebAppInfo(url=WEBSITE_URL))],
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
    await callback.answer("🔄 Refreshing stories from /story/ page...")
    
    story_data = await story_fetcher.get_stories(force_refresh=True, page=1)
    
    if story_data['stories']:
        await callback.message.edit_text(
            f"📚 *Stories Refreshed!*\n"
            f"Total stories found: {story_data['total']}\n"
            f"🎁 Earn ₹{STORY_READ_REWARD} per complete story!",
            reply_markup=get_stories_keyboard(story_data, 1),
            parse_mode="Markdown"
        )
    else:
        await callback.answer("❌ No stories found! Check debug file.", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("read_story_"))
async def read_story(callback: types.CallbackQuery):
    story_id = int(callback.data.split("_")[2])
    story = await story_fetcher.get_story_by_id(story_id)
    
    if story:
        text = f"""📖 *{story['title']}*

📝 *How to read:*
1. Click 'Read on Web' to open
2. Read the complete story
3. Click '✅ Complete' to earn
4. Get ₹{STORY_READ_REWARD} reward!

💡 Opens in your browser"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌐 Read Full Story", url=story['url'])],
            [InlineKeyboardButton(text="✅ Mark as Complete", callback_data=f"complete_story_{story_id}")],
            [InlineKeyboardButton(text="📚 More Stories", callback_data="stories_list")],
            [InlineKeyboardButton(text="◀️ Back", callback_data="stories_list")]
        ])
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    else:
        await callback.answer("Story not found!", show_alert=True)
    
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
    
    if not db.can_reward_story(user_id):
        await callback.answer(f"❌ Daily limit reached! Max {DAILY_STORY_LIMIT} stories/day", show_alert=True)
        return
    
    if db.complete_story(user_id, story_id, story['title']):
        balance = db.get_balance(user_id)
        await callback.answer(f"✅ +₹{STORY_READ_REWARD} earned!", show_alert=True)
        
        await callback.message.edit_text(
            f"✅ *Story Completed!*\n\n"
            f"📖 {story['title']}\n"
            f"💰 Reward: +₹{STORY_READ_REWARD}\n"
            f"💳 New Balance: ₹{balance}\n\n"
            f"Keep reading!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📚 Read More", callback_data="stories_list")],
                [InlineKeyboardButton(text="◀️ Back to Menu", callback_data="back")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await callback.answer("Already completed!", show_alert=True)

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
📚 Today: {stats['stories_today']}/{DAILY_STORY_LIMIT}
📅 Joined: {str(stats['joined_date'])[:10]}"""
    
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

📊 Stats:
• Referrals: {stats['referral_count']}
• Earned: ₹{stats['total_earned']}

🎁 Rewards:
• Per Referral: ₹{REFERRAL_BONUS}
• Per Story: ₹{STORY_READ_REWARD}"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Share", switch_inline_query=f"Join and earn! 📚💰")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "withdraw")
async def withdraw_cmd(callback: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    
    if balance < MINIMUM_WITHDRAWAL:
        await callback.answer(f"❌ Minimum ₹{MINIMUM_WITHDRAWAL} needed! You have ₹{balance}", show_alert=True)
        return
    
    await callback.message.answer(
        f"💰 Balance: ₹{balance}\nMin: ₹{MINIMUM_WITHDRAWAL}\n\nEnter amount:\nType /cancel"
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
            await message.answer(f"❌ Minimum ₹{MINIMUM_WITHDRAWAL}!")
            return
        if amount > balance:
            await message.answer(f"❌ Insufficient! You have ₹{balance}")
            return
        
        await state.update_data(amount=amount)
        await message.answer("📱 Enter UPI ID:\nType /cancel")
        await state.set_state(WithdrawState.upi)
    except ValueError:
        await message.answer("❌ Enter a valid number!")

@dp.message(WithdrawState.upi)
async def withdraw_upi(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await start_cmd(message)
        return
    
    upi_id = message.text.strip()
    if '@' not in upi_id or len(upi_id) < 5:
        await message.answer("❌ Invalid UPI!")
        return
    
    data = await state.get_data()
    amount = data['amount']
    db = Database(DATABASE_FILE)
    
    if db.deduct_balance(message.from_user.id, amount):
        withdraw_id = db.create_withdrawal(message.from_user.id, amount, upi_id)
        
        await message.answer(
            f"✅ *Submitted!*\n\n💰 ₹{amount}\n📱 `{upi_id}`\n🆔 #{withdraw_id}\n\n24-48 hours processing.",
            parse_mode="Markdown"
        )
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, f"💰 New Withdrawal\nUser: {message.from_user.first_name}\nAmount: ₹{amount}\nUPI: {upi_id}\nID: #{withdraw_id}")
            except:
                pass
    else:
        await message.answer("❌ Failed!")
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "help")
async def help_cmd(callback: types.CallbackQuery):
    text = f"""📖 *Help Guide*

*Earn:*
📖 Read: ₹{STORY_READ_REWARD}/story
👥 Refer: ₹{REFERRAL_BONUS}/referral
📊 Daily limit: {DAILY_STORY_LIMIT}

*Withdraw:*
💳 Min: ₹{MINIMUM_WITHDRAWAL}
📱 UPI transfer

/start - Restart
/help - Help"""
    
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
    await callback.answer("Use ◀️ ▶️ to navigate", show_alert=True)

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
    await callback.answer("🔄 Refreshing...")
    story_data = await story_fetcher.get_stories(force_refresh=True)
    
    if story_data['stories']:
        await callback.message.answer(f"✅ Found {story_data['total']} stories!")
    else:
        await callback.message.answer("❌ No stories! Check story_page_debug.html")
    
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
        name = w[11] or f"User_{w[1]}"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"💰 {name[:15]} - ₹{w[2]}", callback_data=f"process_{w[0]}")
        ])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")])
    
    await callback.message.edit_text(f"💰 Pending ({len(withdrawals)})", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("process_"))
async def process_withdrawal(callback: types.CallbackQuery):
    withdraw_id = int(callback.data.split("_")[1])
    db = Database(DATABASE_FILE)
    
    conn = db.get_conn()
    cursor = conn.cursor()
    cursor.execute('''SELECT w.*, u.first_name, u.username FROM withdrawals w JOIN users u ON w.user_id = u.user_id WHERE w.id = ?''', (withdraw_id,))
    w = cursor.fetchone()
    conn.close()
    
    if not w:
        await callback.message.answer("Not found!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{withdraw_id}"),
         InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{withdraw_id}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_withdrawals")]
    ])
    
    text = f"💰 Withdrawal #{w[0]}\n👤 {w[11]}\n💰 ₹{w[2]}\n📱 {w[3]}"
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("approve_"))
async def approve_withdrawal(callback: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    db.approve_withdrawal(int(callback.data.split("_")[1]))
    await callback.answer("✅ Approved!", show_alert=True)
    await callback.message.edit_text("✅ Approved!")
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
            await bot.send_message(user_id, f"❌ Withdrawal of ₹{amount} rejected. Refunded.")
        except:
            pass
    
    await callback.answer("❌ Rejected!", show_alert=True)
    await callback.message.edit_text("❌ Rejected!")
    await asyncio.sleep(2)
    await admin_withdrawals_cmd(callback)

@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def admin_broadcast_cmd(callback: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    await callback.message.answer("📢 Send message:\nType /cancel")
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
    
    await status_msg.edit_text(f"✅ Sent to {sent} users!")
    await state.clear()

@dp.errors()
async def error_handler(update: types.Update, exception: Exception):
    logger.error(f"Error: {exception}")
    return True

# ================= MAIN =================
async def main():
    logger.info("🚀 Starting Story Bot...")
    logger.info(f"📌 Story page: {STORY_PAGE_URL}")
    
    db = Database(DATABASE_FILE)
    
    story_data = await story_fetcher.get_stories(force_refresh=True)
    logger.info(f"📚 Pre-loaded {story_data['total']} stories")
    
    logger.info("✅ Bot ready!")
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
