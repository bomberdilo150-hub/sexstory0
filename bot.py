from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, BotCommand
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
import asyncio
import logging
from datetime import datetime, timedelta
import sqlite3
from typing import List, Dict, Optional, Tuple
import os
import re
import json
from functools import wraps
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# ================= CONFIG =================
API_TOKEN = os.getenv("BOT_TOKEN", "8777177819:AAHuJtPJR8VmoWSfqHtrHW7WeVNWJ6sbV7o")
WEBSITE_URL = "https://sexstory.lovable.app"
DATABASE_FILE = "bot_database.db"
ADMIN_IDS = [8459969831]  # Apni Telegram ID dalo
REFERRAL_BONUS = 10  # Per referral bonus points
WITHDRAWAL_MINIMUM = 100  # Minimum withdrawal amount

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
            
            # Users table - FIXED: Added all required columns
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    joined_date TIMESTAMP,
                    last_active TIMESTAMP,
                    referred_by INTEGER,
                    referral_count INTEGER DEFAULT 0,
                    total_interactions INTEGER DEFAULT 0,
                    total_reads INTEGER DEFAULT 0,
                    is_admin BOOLEAN DEFAULT 0,
                    is_banned BOOLEAN DEFAULT 0,
                    is_premium BOOLEAN DEFAULT 0,
                    premium_until TIMESTAMP
                )
            ''')
            
            # User Balance Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_balance (
                    user_id INTEGER PRIMARY KEY,
                    balance INTEGER DEFAULT 0,
                    total_earned INTEGER DEFAULT 0,
                    total_withdrawn INTEGER DEFAULT 0,
                    last_updated TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Withdrawal Requests Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS withdrawal_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount INTEGER,
                    upi_id TEXT,
                    status TEXT DEFAULT 'pending',
                    request_date TIMESTAMP,
                    processed_date TIMESTAMP,
                    processed_by INTEGER,
                    transaction_id TEXT,
                    admin_notes TEXT,
                    user_notes TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Transaction History Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transaction_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    type TEXT,
                    amount INTEGER,
                    balance_after INTEGER,
                    description TEXT,
                    reference_id INTEGER,
                    created_at TIMESTAMP,
                    created_by INTEGER
                )
            ''')
            
            # Referral Rewards Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS referral_rewards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    referred_user_id INTEGER,
                    reward_type TEXT,
                    amount INTEGER,
                    awarded_at TIMESTAMP
                )
            ''')
            
            # Stories table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    url TEXT UNIQUE,
                    content TEXT,
                    author TEXT,
                    category TEXT,
                    fetched_at TIMESTAMP,
                    views INTEGER DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    rating REAL DEFAULT 0
                )
            ''')
            
            # Feedback table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    message TEXT,
                    rating INTEGER,
                    created_at TIMESTAMP,
                    resolved BOOLEAN DEFAULT 0
                )
            ''')
            
            # Add indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_withdrawal_status ON withdrawal_requests(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user ON transaction_history(user_id, created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
            
            # Check if old table has missing columns and add them
            try:
                # Check if last_active column exists
                cursor.execute("SELECT last_active FROM users LIMIT 1")
            except sqlite3.OperationalError:
                # Add missing columns
                try:
                    cursor.execute("ALTER TABLE users ADD COLUMN last_active TIMESTAMP")
                    logger.info("Added last_active column to users table")
                except:
                    pass
                
                try:
                    cursor.execute("ALTER TABLE users ADD COLUMN premium_until TIMESTAMP")
                    logger.info("Added premium_until column to users table")
                except:
                    pass
                
                try:
                    cursor.execute("ALTER TABLE users ADD COLUMN total_reads INTEGER DEFAULT 0")
                    logger.info("Added total_reads column to users table")
                except:
                    pass
            
            # Add admins
            for admin_id in ADMIN_IDS:
                # Check if admin already exists
                cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (admin_id,))
                if not cursor.fetchone():
                    cursor.execute('''
                        INSERT INTO users (user_id, is_admin, joined_date, last_active)
                        VALUES (?, 1, ?, ?)
                    ''', (admin_id, datetime.now(), datetime.now()))
                else:
                    # Update existing user to admin if not already
                    cursor.execute('''
                        UPDATE users SET is_admin = 1, last_active = ? WHERE user_id = ?
                    ''', (datetime.now(), admin_id))
                
                # Initialize balance for admin
                cursor.execute('''
                    INSERT OR IGNORE INTO user_balance (user_id, balance, total_earned, total_withdrawn, last_updated)
                    VALUES (?, 0, 0, 0, ?)
                ''', (admin_id, datetime.now()))
            
            conn.commit()
            logger.info("Database initialized successfully with all tables")
    
    # ========== BALANCE MANAGEMENT METHODS ==========
    
    def init_user_balance(self, user_id: int):
        """Initialize balance for new user"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO user_balance (user_id, balance, total_earned, total_withdrawn, last_updated)
                VALUES (?, 0, 0, 0, ?)
            ''', (user_id, datetime.now()))
            conn.commit()
    
    def get_user_balance(self, user_id: int) -> Dict:
        """Get user's complete balance information"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT balance, total_earned, total_withdrawn, last_updated 
                FROM user_balance WHERE user_id = ?
            ''', (user_id,))
            result = cursor.fetchone()
            
            if result:
                return {
                    'balance': result[0],
                    'total_earned': result[1],
                    'total_withdrawn': result[2],
                    'last_updated': result[3]
                }
            else:
                self.init_user_balance(user_id)
                return {'balance': 0, 'total_earned': 0, 'total_withdrawn': 0, 'last_updated': None}
    
    def add_balance(self, user_id: int, amount: int, description: str, reference_id: int = None, created_by: int = None) -> bool:
        """Add balance to user with transaction record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get current balance
            cursor.execute("SELECT balance, total_earned FROM user_balance WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            
            if result:
                new_balance = result[0] + amount
                new_total_earned = result[1] + amount
                
                # Update balance
                cursor.execute('''
                    UPDATE user_balance 
                    SET balance = ?, total_earned = ?, last_updated = ?
                    WHERE user_id = ?
                ''', (new_balance, new_total_earned, datetime.now(), user_id))
                
                # Add transaction record
                cursor.execute('''
                    INSERT INTO transaction_history (user_id, type, amount, balance_after, description, reference_id, created_at, created_by)
                    VALUES (?, 'credit', ?, ?, ?, ?, ?, ?)
                ''', (user_id, amount, new_balance, description, reference_id, datetime.now(), created_by))
                
                conn.commit()
                return True
            return False
    
    def deduct_balance(self, user_id: int, amount: int, description: str, reference_id: int = None, created_by: int = None) -> bool:
        """Deduct balance from user with transaction record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get current balance
            cursor.execute("SELECT balance, total_withdrawn FROM user_balance WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            
            if result and result[0] >= amount:
                new_balance = result[0] - amount
                new_total_withdrawn = result[1] + amount
                
                # Update balance
                cursor.execute('''
                    UPDATE user_balance 
                    SET balance = ?, total_withdrawn = ?, last_updated = ?
                    WHERE user_id = ?
                ''', (new_balance, new_total_withdrawn, datetime.now(), user_id))
                
                # Add transaction record
                cursor.execute('''
                    INSERT INTO transaction_history (user_id, type, amount, balance_after, description, reference_id, created_at, created_by)
                    VALUES (?, 'debit', ?, ?, ?, ?, ?, ?)
                ''', (user_id, amount, new_balance, description, reference_id, datetime.now(), created_by))
                
                conn.commit()
                return True
            return False
    
    def get_transaction_history(self, user_id: int, limit: int = 20) -> List[Dict]:
        """Get user's transaction history"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, type, amount, balance_after, description, created_at
                FROM transaction_history
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
            ''', (user_id, limit))
            
            results = cursor.fetchall()
            return [{
                'id': r[0],
                'type': r[1],
                'amount': r[2],
                'balance_after': r[3],
                'description': r[4],
                'created_at': r[5]
            } for r in results]
    
    # ========== REFERRAL METHODS ==========
    
    def process_referral(self, referrer_id: int, new_user_id: int):
        """Process referral and add bonus"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Update user's referral count
            cursor.execute("UPDATE users SET referral_count = referral_count + 1 WHERE user_id = ?", (referrer_id,))
            
            # Add referral bonus to balance
            self.add_balance(
                referrer_id, 
                REFERRAL_BONUS, 
                f"Referral bonus for inviting user {new_user_id}",
                new_user_id
            )
            
            # Record referral reward
            cursor.execute('''
                INSERT INTO referral_rewards (user_id, referred_user_id, reward_type, amount, awarded_at)
                VALUES (?, ?, 'referral_bonus', ?, ?)
            ''', (referrer_id, new_user_id, REFERRAL_BONUS, datetime.now()))
            
            conn.commit()
    
    def get_referral_details(self, user_id: int) -> Dict:
        """Get detailed referral information"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get referred users
            cursor.execute('''
                SELECT user_id, username, first_name, joined_date
                FROM users
                WHERE referred_by = ?
                ORDER BY joined_date DESC
            ''', (user_id,))
            referred_users = cursor.fetchall()
            
            # Get referral rewards
            cursor.execute('''
                SELECT SUM(amount) as total_rewards, COUNT(*) as total_referrals
                FROM referral_rewards
                WHERE user_id = ?
            ''', (user_id,))
            reward_stats = cursor.fetchone()
            
            return {
                'referred_users': [{
                    'user_id': u[0],
                    'username': u[1],
                    'first_name': u[2],
                    'joined_date': u[3]
                } for u in referred_users],
                'total_rewards': reward_stats[0] if reward_stats[0] else 0,
                'total_referrals': reward_stats[1] if reward_stats[1] else 0
            }
    
    # ========== WITHDRAWAL METHODS WITH UPI ==========
    
    def create_withdrawal_request(self, user_id: int, amount: int, upi_id: str, notes: str = None) -> int:
        """Create a withdrawal request with UPI ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO withdrawal_requests (user_id, amount, upi_id, request_date, status, user_notes)
                VALUES (?, ?, ?, ?, 'pending', ?)
            ''', (user_id, amount, upi_id, datetime.now(), notes))
            conn.commit()
            return cursor.lastrowid
    
    def get_pending_withdrawals(self) -> List[Dict]:
        """Get all pending withdrawal requests with user details"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT w.*, u.username, u.first_name, u.last_name, b.balance
                FROM withdrawal_requests w
                JOIN users u ON w.user_id = u.user_id
                LEFT JOIN user_balance b ON u.user_id = b.user_id
                WHERE w.status = 'pending'
                ORDER BY w.request_date ASC
            ''')
            
            results = cursor.fetchall()
            return [{
                'id': r[0],
                'user_id': r[1],
                'amount': r[2],
                'upi_id': r[3],
                'status': r[4],
                'request_date': r[5],
                'processed_date': r[6],
                'processed_by': r[7],
                'transaction_id': r[8],
                'admin_notes': r[9],
                'user_notes': r[10],
                'username': r[11],
                'first_name': r[12],
                'last_name': r[13],
                'current_balance': r[14] if r[14] else 0
            } for r in results]
    
    def get_withdrawal_history(self, status: str = None, limit: int = 50) -> List[Dict]:
        """Get withdrawal history with optional status filter"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if status:
                cursor.execute('''
                    SELECT w.*, u.username, u.first_name, u.last_name
                    FROM withdrawal_requests w
                    JOIN users u ON w.user_id = u.user_id
                    WHERE w.status = ?
                    ORDER BY w.processed_date DESC, w.request_date DESC
                    LIMIT ?
                ''', (status, limit))
            else:
                cursor.execute('''
                    SELECT w.*, u.username, u.first_name, u.last_name
                    FROM withdrawal_requests w
                    JOIN users u ON w.user_id = u.user_id
                    ORDER BY w.request_date DESC
                    LIMIT ?
                ''', (limit,))
            
            results = cursor.fetchall()
            return [{
                'id': r[0],
                'user_id': r[1],
                'amount': r[2],
                'upi_id': r[3],
                'status': r[4],
                'request_date': r[5],
                'processed_date': r[6],
                'processed_by': r[7],
                'transaction_id': r[8],
                'admin_notes': r[9],
                'user_notes': r[10],
                'username': r[11],
                'first_name': r[12],
                'last_name': r[13]
            } for r in results]
    
    def approve_withdrawal(self, request_id: int, admin_id: int, transaction_id: str = None, admin_notes: str = None) -> bool:
        """Approve a withdrawal request"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get request details
            cursor.execute("SELECT user_id, amount FROM withdrawal_requests WHERE id = ? AND status = 'pending'", (request_id,))
            request = cursor.fetchone()
            
            if request:
                user_id, amount = request
                
                # Deduct balance (already deducted when request was made, so just update status)
                cursor.execute('''
                    UPDATE withdrawal_requests 
                    SET status = 'approved', 
                        processed_date = ?, 
                        processed_by = ?, 
                        transaction_id = ?,
                        admin_notes = ?
                    WHERE id = ?
                ''', (datetime.now(), admin_id, transaction_id, admin_notes, request_id))
                conn.commit()
                return True
            return False
    
    def reject_withdrawal(self, request_id: int, admin_id: int, admin_notes: str = None) -> bool:
        """Reject a withdrawal request and refund balance"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get request details
            cursor.execute("SELECT user_id, amount FROM withdrawal_requests WHERE id = ? AND status = 'pending'", (request_id,))
            request = cursor.fetchone()
            
            if request:
                user_id, amount = request
                
                # Refund the amount back to user
                self.add_balance(user_id, amount, f"Withdrawal refund - Request #{request_id}", request_id, admin_id)
                
                # Update request status
                cursor.execute('''
                    UPDATE withdrawal_requests 
                    SET status = 'rejected', 
                        processed_date = ?, 
                        processed_by = ?,
                        admin_notes = ?
                    WHERE id = ?
                ''', (datetime.now(), admin_id, admin_notes, request_id))
                conn.commit()
                return True
            return False
    
    def get_withdrawal_statistics(self) -> Dict:
        """Get withdrawal statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*), SUM(amount) FROM withdrawal_requests WHERE status = 'pending'")
            pending = cursor.fetchone()
            
            cursor.execute("SELECT COUNT(*), SUM(amount) FROM withdrawal_requests WHERE status = 'approved'")
            approved = cursor.fetchone()
            
            cursor.execute("SELECT COUNT(*), SUM(amount) FROM withdrawal_requests WHERE status = 'rejected'")
            rejected = cursor.fetchone()
            
            return {
                'pending_count': pending[0] or 0,
                'pending_amount': pending[1] or 0,
                'approved_count': approved[0] or 0,
                'approved_amount': approved[1] or 0,
                'rejected_count': rejected[0] or 0,
                'rejected_amount': rejected[1] or 0
            }
    
    # ========== USER MANAGEMENT ==========
    
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
    
    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = "", referred_by: int = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            if not cursor.fetchone():
                cursor.execute('''
                    INSERT INTO users (user_id, username, first_name, last_name, joined_date, last_active, referred_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, username, first_name, last_name, datetime.now(), datetime.now(), referred_by))
                
                # Initialize balance for new user
                self.init_user_balance(user_id)
                
                # Process referral if applicable
                if referred_by:
                    self.process_referral(referred_by, user_id)
            else:
                # Update last_active for existing user
                cursor.execute("UPDATE users SET last_active = ? WHERE user_id = ?", (datetime.now(), user_id))
            
            conn.commit()
            return True
    
    def update_last_active(self, user_id: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET last_active = ? WHERE user_id = ?", (datetime.now(), user_id))
            conn.commit()
    
    def get_user_stats(self, user_id: int) -> Dict:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT referral_count, total_interactions, total_reads, joined_date, is_premium 
                FROM users WHERE user_id = ?
            ''', (user_id,))
            result = cursor.fetchone()
            if result:
                return {
                    'referral_count': result[0], 
                    'interactions': result[1], 
                    'reads': result[2],
                    'joined_date': result[3],
                    'is_premium': result[4] if len(result) > 4 else False
                }
            return {}
    
    def update_interaction(self, user_id: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET total_interactions = total_interactions + 1 WHERE user_id = ?", (user_id,))
            conn.commit()
    
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

# ================= STATES =================
class AdminStates(StatesGroup):
    broadcasting = State()
    adjust_balance = State()
    withdrawal_action = State()
    search_user = State()

class UserStates(StatesGroup):
    withdrawal_amount = State()
    withdrawal_upi = State()
    feedback = State()

# ================= KEYBOARDS =================
async def get_main_keyboard(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Get main keyboard with balance display"""
    db = Database(DATABASE_FILE)
    balance = db.get_user_balance(user_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📱 Open Stories App",
                web_app=WebAppInfo(url=WEBSITE_URL)
            )
        ],
        [
            InlineKeyboardButton(text="📚 Latest Stories", callback_data="stories"),
            InlineKeyboardButton(text="💰 Refer & Earn", callback_data="referral")
        ],
        [
            InlineKeyboardButton(text=f"💳 Balance: {balance['balance']}💎", callback_data="my_balance"),
            InlineKeyboardButton(text="📊 My Stats", callback_data="stats")
        ],
        [
            InlineKeyboardButton(text="🏧 Withdraw", callback_data="withdraw"),
            InlineKeyboardButton(text="📜 Transactions", callback_data="transactions")
        ],
        [
            InlineKeyboardButton(text="❓ Help", callback_data="help"),
            InlineKeyboardButton(text="⭐ Rate Bot", callback_data="rate_bot")
        ]
    ])
    
    if is_admin:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text="👑 ADMIN PANEL", callback_data="admin_panel")
        ])
    
    return keyboard

# ================= COMMAND HANDLERS =================
@dp.message(Command("start"))
async def start_command(message: types.Message, state: FSMContext):
    user = message.from_user
    db = Database(DATABASE_FILE)
    
    if db.is_banned(user.id):
        await message.answer("❌ You have been banned from this bot.")
        return
    
    # Parse referral
    args = message.text.split()
    referred_by = None
    if len(args) > 1 and args[1].isdigit():
        referred_by = int(args[1])
        if referred_by == user.id:
            referred_by = None
    
    # Add user
    db.add_user(user.id, user.username, user.first_name, user.last_name or "", referred_by)
    db.update_last_active(user.id)
    
    is_admin = db.is_admin(user.id)
    balance = db.get_user_balance(user.id)
    
    welcome_text = (
        f"🌟 **Welcome {user.first_name}!** 🌟\n\n"
        f"💰 **Your Balance:** {balance['balance']} diamonds 💎\n"
        f"📈 **Total Earned:** {balance['total_earned']} diamonds\n\n"
        f"📱 Click 'Open Stories App' to read stories!\n"
        f"💰 Invite friends and earn {REFERRAL_BONUS} diamonds each!\n"
        f"💳 Withdraw when you reach {WITHDRAWAL_MINIMUM} diamonds\n\n"
        f"Choose an option below:"
    )
    
    await message.answer(
        welcome_text,
        reply_markup=await get_main_keyboard(user.id, is_admin),
        parse_mode="Markdown"
    )
    await state.clear()

# ================= WITHDRAWAL HANDLERS WITH UPI =================
@dp.callback_query(lambda c: c.data == "withdraw")
async def withdraw_handler(callback_query: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    balance = db.get_user_balance(callback_query.from_user.id)
    
    if balance['balance'] < WITHDRAWAL_MINIMUM:
        await callback_query.answer(f"Minimum withdrawal is {WITHDRAWAL_MINIMUM} diamonds! You have {balance['balance']} diamonds.", show_alert=True)
        return
    
    await callback_query.message.answer(
        f"🏧 **Withdrawal Request** 🏧\n\n"
        f"💰 Available Balance: {balance['balance']} diamonds\n"
        f"💳 Minimum: {WITHDRAWAL_MINIMUM} diamonds\n\n"
        f"**Enter amount to withdraw:**\n"
        f"(Type /cancel to cancel)",
        parse_mode="Markdown"
    )
    await state.set_state(UserStates.withdrawal_amount)
    await callback_query.answer()

@dp.message(UserStates.withdrawal_amount)
async def process_withdrawal_amount(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await start_command(message, state)
        return
    
    try:
        amount = int(message.text)
        db = Database(DATABASE_FILE)
        balance = db.get_user_balance(message.from_user.id)
        
        if amount < WITHDRAWAL_MINIMUM:
            await message.answer(f"❌ Minimum withdrawal amount is {WITHDRAWAL_MINIMUM} diamonds!")
            return
        
        if amount > balance['balance']:
            await message.answer(f"❌ Insufficient balance! You have {balance['balance']} diamonds.")
            return
        
        await state.update_data(withdraw_amount=amount)
        
        await message.answer(
            f"💰 **Amount:** {amount} diamonds\n\n"
            f"📱 **Enter your UPI ID:**\n"
            f"Example: `example@okhdfcbank` or `9876543210@paytm`\n\n"
            f"Type /cancel to cancel",
            parse_mode="Markdown"
        )
        await state.set_state(UserStates.withdrawal_upi)
        
    except ValueError:
        await message.answer("❌ Please enter a valid number!")

@dp.message(UserStates.withdrawal_upi)
async def process_withdrawal_upi(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await start_command(message, state)
        return
    
    upi_id = message.text.strip()
    
    # Basic UPI ID validation
    if not re.match(r'^[a-zA-Z0-9.\-_]{2,}@[a-zA-Z]{3,}$', upi_id):
        await message.answer(
            "❌ **Invalid UPI ID!**\n\n"
            "Please enter a valid UPI ID like:\n"
            "• example@okhdfcbank\n"
            "• 9876543210@paytm\n"
            "• username@ybl\n\n"
            "Type /cancel to cancel",
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    amount = data.get('withdraw_amount')
    
    db = Database(DATABASE_FILE)
    
    # Deduct balance first
    if db.deduct_balance(message.from_user.id, amount, f"Withdrawal request", None):
        request_id = db.create_withdrawal_request(
            message.from_user.id,
            amount,
            upi_id,
            None
        )
        
        await message.answer(
            f"✅ **Withdrawal Request Submitted!** ✅\n\n"
            f"💰 Amount: {amount} diamonds\n"
            f"📱 UPI ID: `{upi_id}`\n"
            f"🆔 Request ID: #{request_id}\n\n"
            f"⏳ **Status:** Pending Admin Approval\n\n"
            f"📌 Your balance has been deducted. If rejected, amount will be refunded.\n"
            f"📞 Admin will process within 24-48 hours.\n\n"
            f"Thank you for your patience! 🙏",
            parse_mode="Markdown"
        )
        
        # Send notification to ALL admins
        for admin_id in ADMIN_IDS:
            try:
                admin_text = (
                    f"💰 **NEW WITHDRAWAL REQUEST** 💰\n\n"
                    f"┌── 📋 **Request Details**\n"
                    f"├─ 🆔 Request ID: `#{request_id}`\n"
                    f"├─ 👤 User: {message.from_user.first_name} {message.from_user.last_name or ''}\n"
                    f"├─ 📝 Username: @{message.from_user.username or 'N/A'}\n"
                    f"├─ 🆔 User ID: `{message.from_user.id}`\n"
                    f"├─ 💰 Amount: `{amount}` diamonds\n"
                    f"├─ 📱 UPI ID: `{upi_id}`\n"
                    f"├─ 📅 Requested: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"└─ 💎 Balance: {db.get_user_balance(message.from_user.id)['balance']} diamonds\n\n"
                    f"**Use Admin Panel to process this request!**"
                )
                
                await bot.send_message(admin_id, admin_text, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
    else:
        await message.answer("❌ Failed to process withdrawal. Please try again.")
    
    await state.clear()

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
    withdrawal_stats = db.get_withdrawal_statistics()
    
    # Get total balance in system
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(balance), SUM(total_earned), SUM(total_withdrawn) FROM user_balance")
        financial_stats = cursor.fetchone()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Dashboard", callback_data="admin_dashboard")],
        [InlineKeyboardButton(text="💰 Withdrawal Requests", callback_data="admin_withdrawals")],
        [InlineKeyboardButton(text="📜 Withdrawal History", callback_data="admin_withdrawal_history")],
        [InlineKeyboardButton(text="👥 Users List", callback_data="admin_users")],
        [InlineKeyboardButton(text="💳 Adjust Balance", callback_data="admin_adjust_balance")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_panel")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    text = (
        f"👑 **Admin Control Panel** 👑\n\n"
        f"📊 **User Statistics:**\n"
        f"• Total Users: {total_users}\n"
        f"• Admins: {admin_count}\n"
        f"• Banned: {banned_count}\n\n"
        f"💰 **Financial Statistics:**\n"
        f"• Total Balance: {financial_stats[0] or 0} diamonds\n"
        f"• Total Earned: {financial_stats[1] or 0} diamonds\n"
        f"• Total Withdrawn: {financial_stats[2] or 0} diamonds\n\n"
        f"🏧 **Withdrawal Stats:**\n"
        f"• Pending: {withdrawal_stats['pending_count']} ({withdrawal_stats['pending_amount']}💎)\n"
        f"• Approved: {withdrawal_stats['approved_count']} ({withdrawal_stats['approved_amount']}💎)\n"
        f"• Rejected: {withdrawal_stats['rejected_count']} ({withdrawal_stats['rejected_amount']}💎)\n\n"
        f"Select an option:"
    )
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "admin_withdrawals")
async def admin_withdrawals_list(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    withdrawals = db.get_pending_withdrawals()
    
    if not withdrawals:
        await callback_query.message.answer("✅ No pending withdrawal requests!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for wd in withdrawals:
        upi_short = wd['upi_id'][:15] + "..." if len(wd['upi_id']) > 15 else wd['upi_id']
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"💰 {wd['first_name']} - {wd['amount']}💎 - {upi_short}",
                callback_data=f"admin_view_withdrawal_{wd['id']}"
            )
        ])
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_withdrawals"),
        InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")
    ])
    
    await callback_query.message.edit_text(
        f"💰 **Pending Withdrawals** ({len(withdrawals)} requests)\n\nSelect a request to process:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data.startswith("admin_view_withdrawal_"))
async def admin_view_withdrawal(callback_query: types.CallbackQuery):
    withdrawal_id = int(callback_query.data.split("_")[3])
    db = Database(DATABASE_FILE)
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT w.*, u.first_name, u.last_name, u.username, u.joined_date, b.balance
            FROM withdrawal_requests w
            JOIN users u ON w.user_id = u.user_id
            LEFT JOIN user_balance b ON u.user_id = b.user_id
            WHERE w.id = ?
        ''', (withdrawal_id,))
        result = cursor.fetchone()
    
    if not result:
        await callback_query.message.answer("Withdrawal request not found!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve", callback_data=f"admin_approve_{withdrawal_id}"),
         InlineKeyboardButton(text="❌ Reject", callback_data=f"admin_reject_{withdrawal_id}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_withdrawals")]
    ])
    
    text = (
        f"💰 **Withdrawal Request Details** 💰\n\n"
        f"┌── 📋 **Request Information**\n"
        f"├─ 🆔 Request ID: `#{result[0]}`\n"
        f"├─ 👤 User: {result[11]} {result[12] or ''}\n"
        f"├─ 📝 Username: @{result[13] or 'N/A'}\n"
        f"├─ 🆔 User ID: `{result[1]}`\n"
        f"├─ 📅 Joined: {result[14][:10] if result[14] else 'N/A'}\n"
        f"├─ 💰 Amount: `{result[2]}` diamonds\n"
        f"├─ 📱 UPI ID: `{result[3]}`\n"
        f"├─ 📅 Requested: {result[5]}\n"
        f"├─ 💎 Current Balance: {result[16] if result[16] else 0} diamonds\n"
        f"└─ 📝 Notes: {result[10] or 'None'}\n\n"
        f"**Actions:**\n"
        f"✅ Approve - Payment will be sent to UPI ID\n"
        f"❌ Reject - Amount will be refunded to user"
    )
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data.startswith("admin_approve_"))
async def admin_approve_withdrawal(callback_query: types.CallbackQuery):
    withdrawal_id = int(callback_query.data.split("_")[2])
    db = Database(DATABASE_FILE)
    
    # Get withdrawal details
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, amount, upi_id FROM withdrawal_requests WHERE id = ? AND status = 'pending'
        ''', (withdrawal_id,))
        result = cursor.fetchone()
    
    if result and db.approve_withdrawal(withdrawal_id, callback_query.from_user.id):
        user_id, amount, upi_id = result
        
        await callback_query.answer("✅ Withdrawal approved!", show_alert=True)
        await callback_query.message.edit_text(
            f"✅ **Withdrawal #{withdrawal_id} Approved!**\n\n"
            f"💰 Amount: {amount} diamonds\n"
            f"📱 UPI ID: `{upi_id}`\n\n"
            f"Payment has been approved.",
            parse_mode="Markdown"
        )
        
        # Notify user
        try:
            await bot.send_message(
                user_id,
                f"✅ **Withdrawal Approved!** ✅\n\n"
                f"💰 Amount: {amount} diamonds\n"
                f"🆔 Request ID: #{withdrawal_id}\n\n"
                f"Amount will be sent to your UPI ID: `{upi_id}`\n"
                f"within 24 hours.\n\n"
                f"Thank you for using our bot! 🙏",
                parse_mode="Markdown"
            )
        except:
            pass
    else:
        await callback_query.answer("❌ Failed to approve withdrawal!", show_alert=True)
    
    await asyncio.sleep(2)
    await admin_withdrawals_list(callback_query)

@dp.callback_query(lambda c: c.data.startswith("admin_reject_"))
async def admin_reject_withdrawal(callback_query: types.CallbackQuery):
    withdrawal_id = int(callback_query.data.split("_")[2])
    db = Database(DATABASE_FILE)
    
    # Get withdrawal details
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, amount FROM withdrawal_requests WHERE id = ? AND status = 'pending'
        ''', (withdrawal_id,))
        result = cursor.fetchone()
    
    if result and db.reject_withdrawal(withdrawal_id, callback_query.from_user.id):
        user_id, amount = result
        
        await callback_query.answer("❌ Withdrawal rejected!", show_alert=True)
        await callback_query.message.edit_text(
            f"❌ **Withdrawal #{withdrawal_id} Rejected!**\n\n"
            f"💰 Amount: {amount} diamonds\n\n"
            f"Amount has been refunded to user's balance.",
            parse_mode="Markdown"
        )
        
        # Notify user
        try:
            await bot.send_message(
                user_id,
                f"❌ **Withdrawal Rejected** ❌\n\n"
                f"💰 Amount: {amount} diamonds\n"
                f"🆔 Request ID: #{withdrawal_id}\n\n"
                f"Your withdrawal request has been rejected.\n"
                f"Amount has been refunded to your balance.\n\n"
                f"Please contact admin for more details.",
                parse_mode="Markdown"
            )
        except:
            pass
    else:
        await callback_query.answer("❌ Failed to reject withdrawal!", show_alert=True)
    
    await asyncio.sleep(2)
    await admin_withdrawals_list(callback_query)

@dp.callback_query(lambda c: c.data == "admin_withdrawal_history")
async def admin_withdrawal_history(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    approved = db.get_withdrawal_history('approved', 20)
    rejected = db.get_withdrawal_history('rejected', 10)
    
    text = "📜 **Withdrawal History** 📜\n\n"
    
    if approved:
        text += "✅ **Approved Withdrawals:**\n"
        for wd in approved[:10]:
            text += f"• #{wd['id']} - {wd['first_name']} - {wd['amount']}💎 - {wd['processed_date'][:10]}\n"
        text += "\n"
    
    if rejected:
        text += "❌ **Rejected Withdrawals:**\n"
        for wd in rejected[:10]:
            text += f"• #{wd['id']} - {wd['first_name']} - {wd['amount']}💎 - {wd['processed_date'][:10]}\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")]
    ])
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "my_balance")
async def my_balance_handler(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    user_id = callback_query.from_user.id
    balance = db.get_user_balance(user_id)
    stats = db.get_user_stats(user_id)
    
    text = (
        f"💳 **Your Balance Details** 💳\n\n"
        f"💰 **Current Balance:** {balance['balance']} diamonds\n"
        f"📈 **Total Earned:** {balance['total_earned']} diamonds\n"
        f"💸 **Total Withdrawn:** {balance['total_withdrawn']} diamonds\n"
        f"👥 **Referrals:** {stats.get('referral_count', 0)}\n\n"
        f"💡 **Earn More:**\n"
        f"• {REFERRAL_BONUS} diamonds per referral\n"
        f"• Bonus for active reading\n\n"
        f"💳 **Minimum Withdrawal:** {WITHDRAWAL_MINIMUM} diamonds"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Refer & Earn", callback_data="referral")],
        [InlineKeyboardButton(text="🏧 Withdraw", callback_data="withdraw")],
        [InlineKeyboardButton(text="📜 Transactions", callback_data="transactions")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "transactions")
async def transactions_handler(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    transactions = db.get_transaction_history(callback_query.from_user.id, 15)
    
    if not transactions:
        await callback_query.message.answer("No transactions found.")
        return
    
    text = "📜 **Transaction History** 📜\n\n"
    for tx in transactions:
        emoji = "➕" if tx['type'] == 'credit' else "➖"
        text += f"{emoji} {tx['amount']} diamonds - {tx['description'][:40]}\n"
        text += f"   📅 {tx['created_at'][:16]} | 💰 Balance: {tx['balance_after']}\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Back", callback_data="my_balance")]
    ])
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "referral")
async def referral_handler(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    bot_username = (await bot.get_me()).username
    link = f"https://t.me/{bot_username}?start={user_id}"
    
    db = Database(DATABASE_FILE)
    balance = db.get_user_balance(user_id)
    referral_details = db.get_referral_details(user_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Share Link", switch_inline_query=f"Join and earn: {link}")],
        [InlineKeyboardButton(text="👥 My Referrals ({})".format(referral_details['total_referrals']), callback_data="my_referrals")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(
        f"💰 **Refer & Earn Program** 💰\n\n"
        f"🔗 **Your Referral Link:**\n`{link}`\n\n"
        f"📊 **Your Earnings:**\n"
        f"💰 Balance: {balance['balance']} diamonds\n"
        f"📈 Total Earned: {balance['total_earned']} diamonds\n"
        f"👥 Total Referrals: {referral_details['total_referrals']}\n"
        f"⭐ Referral Bonus: {REFERRAL_BONUS} diamonds each\n\n"
        f"🎁 **Bonus Milestones:**\n"
        f"• 10 referrals: +50 bonus diamonds\n"
        f"• 25 referrals: +150 bonus diamonds\n"
        f"• 50 referrals: +500 bonus diamonds\n"
        f"• 100 referrals: Premium access for life!\n\n"
        f"Share your link and start earning!",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data == "my_referrals")
async def my_referrals_handler(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    referral_details = db.get_referral_details(callback_query.from_user.id)
    
    if not referral_details['referred_users']:
        await callback_query.message.answer("No referrals yet! Share your link to invite friends.")
        return
    
    text = f"👥 **Your Referrals** ({len(referral_details['referred_users'])} users)\n\n"
    for i, user in enumerate(referral_details['referred_users'][:15], 1):
        text += f"{i}. {user['first_name']} (@{user['username'] or 'N/A'})\n"
        text += f"   Joined: {user['joined_date'][:10]}\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Refer More", callback_data="referral")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "stats")
async def stats_handler(callback_query: types.CallbackQuery):
    db = Database(DATABASE_FILE)
    user_id = callback_query.from_user.id
    stats = db.get_user_stats(user_id)
    balance = db.get_user_balance(user_id)
    
    text = (
        f"📊 **Your Statistics** 📊\n\n"
        f"💰 **Balance:** {balance['balance']} diamonds\n"
        f"📈 **Total Earned:** {balance['total_earned']} diamonds\n"
        f"👥 **Referrals:** {stats.get('referral_count', 0)}\n"
        f"📖 **Stories Read:** {stats.get('reads', 0)}\n"
        f"🔄 **Interactions:** {stats.get('interactions', 0)}\n"
        f"📅 **Member Since:** {stats.get('joined_date', 'Unknown')[:10] if stats.get('joined_date') else 'Unknown'}"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="stats")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "stories")
async def stories_handler(callback_query: types.CallbackQuery):
    await callback_query.answer("📚 Fetching stories...")
    db = Database(DATABASE_FILE)
    db.update_interaction(callback_query.from_user.id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Sample Story 1", url=f"{WEBSITE_URL}/story/1")],
        [InlineKeyboardButton(text="📖 Sample Story 2", url=f"{WEBSITE_URL}/story/2")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="stories"),
         InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(
        "📚 **Latest Stories**\n\nTap to read:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data == "help")
async def help_handler(callback_query: types.CallbackQuery):
    help_text = f"""
📖 **Bot Features & Help** 📖

💰 **Earn Points:**
• {REFERRAL_BONUS} diamonds per referral
• Bonus at referral milestones

💳 **Withdraw Points:**
• Minimum: {WITHDRAWAL_MINIMUM} diamonds
• UPI transfer only
• Processed within 24-48 hours

📚 **Read Stories:**
• Open Stories App button
• Latest & Popular stories

👥 **Referral Program:**
• Share your unique link
• Track your referrals
• Earn unlimited points

❓ Need help? Contact @admin
"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(help_text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "rate_bot")
async def rate_bot_handler(callback_query: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ 1", callback_data="rate_1"),
         InlineKeyboardButton(text="⭐⭐ 2", callback_data="rate_2"),
         InlineKeyboardButton(text="⭐⭐⭐ 3", callback_data="rate_3"),
         InlineKeyboardButton(text="⭐⭐⭐⭐ 4", callback_data="rate_4"),
         InlineKeyboardButton(text="⭐⭐⭐⭐⭐ 5", callback_data="rate_5")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(
        "⭐ **Rate Our Bot** ⭐\n\nHow would you rate your experience?",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data.startswith("rate_"))
async def process_rating(callback_query: types.CallbackQuery):
    rating = int(callback_query.data.split("_")[1])
    await callback_query.answer(f"Thank you for rating us {rating} stars! ⭐", show_alert=True)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(
        f"✅ **Thanks for your {rating}⭐ rating!**\n\nYour feedback helps us improve.",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data == "admin_dashboard")
async def admin_dashboard(callback_query: types.CallbackQuery):
    await admin_panel(callback_query)

@dp.callback_query(lambda c: c.data == "admin_users")
async def admin_users(callback_query: types.CallbackQuery):
    await callback_query.message.answer("👥 User list feature coming soon!")
    await admin_panel(callback_query)

@dp.callback_query(lambda c: c.data == "admin_adjust_balance")
async def admin_adjust_balance(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer(
        "💰 **Adjust User Balance**\n\nSend: `user_id amount description`\nExample: `123456789 100 Bonus`\nUse negative for deduct: `123456789 -50 Penalty`",
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.adjust_balance)
    await callback_query.answer()

@dp.message(AdminStates.adjust_balance)
async def process_adjust_balance(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await admin_panel(message)
        return
    
    try:
        parts = message.text.split(maxsplit=2)
        user_id = int(parts[0])
        amount = int(parts[1])
        description = parts[2] if len(parts) > 2 else "Admin adjustment"
        
        db = Database(DATABASE_FILE)
        
        if amount > 0:
            db.add_balance(user_id, amount, description, created_by=message.from_user.id)
            await message.answer(f"✅ Added {amount} diamonds to user {user_id}")
        else:
            db.deduct_balance(user_id, abs(amount), description, created_by=message.from_user.id)
            await message.answer(f"✅ Deducted {abs(amount)} diamonds from user {user_id}")
            
    except Exception as e:
        await message.answer(f"❌ Error: {str(e)}")
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def admin_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer("📢 Send your broadcast message:")
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
    
    sent = 0
    failed = 0
    status_msg = await message.answer("⏳ Sending broadcast...")
    
    for user in users:
        try:
            await bot.send_message(user[0], message.text)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    
    await status_msg.edit_text(f"✅ Broadcast sent!\nSent: {sent}\nFailed: {failed}")
    await state.clear()

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await start_command(callback_query.message, state)

# ================= MAIN =================
async def main():
    logger.info("🚀 Starting bot with UPI Withdrawal System...")
    
    # Delete old database file if exists to recreate schema
    if os.path.exists(DATABASE_FILE):
        logger.info("Removing old database to create fresh schema...")
        os.remove(DATABASE_FILE)
    
    db = Database(DATABASE_FILE)
    logger.info("Bot is ready!")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())