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
import time

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ================= CONFIG =================
API_TOKEN = os.getenv("BOT_TOKEN", "8777177819:AAHuJtPJR8VmoWSfqHtrHW7WeVNWJ6sbV7o")
WEBSITE_URL = "https://sexstory.lovable.app"
DATABASE_FILE = "bot_database.db"
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "8459969831").split(",")]

# ========== REFERRAL & WITHDRAWAL SETTINGS ==========
REFERRAL_BONUS_DIAMONDS = 10  # Per referral 10 diamonds = ₹10
REFERRAL_BONUS_RUPEE = 10     # Per referral ₹10
DIAMOND_TO_RUPEE_RATE = 1     # 1 Diamond = ₹1

WITHDRAWAL_MINIMUM_DIAMONDS = 100  # Minimum 100 diamonds (₹100)
WITHDRAWAL_MINIMUM_RUPEE = 100     # Minimum ₹100

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= DATABASE WITH RETRY HANDLING =================
class Database:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.init_database()
    
    def get_connection(self, retries=3):
        """Get database connection with retry on lock"""
        for i in range(retries):
            try:
                conn = sqlite3.connect(self.db_file, timeout=30)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=30000")
                return conn
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and i < retries - 1:
                    time.sleep(0.5 * (i + 1))
                    continue
                raise
        return None
    
    def init_database(self):
        """Initialize database with all tables"""
        conn = self.get_connection()
        if not conn:
            logger.error("Failed to connect to database")
            return
        
        cursor = conn.cursor()
        
        # Users table
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
        
        # User Balance Table - 1 Diamond = 1 Rupee
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_balance (
                user_id INTEGER PRIMARY KEY,
                balance_diamonds INTEGER DEFAULT 0,
                balance_rupees INTEGER DEFAULT 0,
                total_earned_diamonds INTEGER DEFAULT 0,
                total_earned_rupees INTEGER DEFAULT 0,
                total_withdrawn_diamonds INTEGER DEFAULT 0,
                total_withdrawn_rupees INTEGER DEFAULT 0,
                last_updated TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Withdrawal Requests Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount_diamonds INTEGER,
                amount_rupees INTEGER,
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
                amount_diamonds INTEGER,
                amount_rupees INTEGER,
                balance_after_diamonds INTEGER,
                balance_after_rupees INTEGER,
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
                amount_diamonds INTEGER,
                amount_rupees INTEGER,
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
        
        # Add indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_withdrawal_status ON withdrawal_requests(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user ON transaction_history(user_id, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
        
        # Add admins
        for admin_id in ADMIN_IDS:
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (admin_id,))
            if not cursor.fetchone():
                cursor.execute('''
                    INSERT INTO users (user_id, is_admin, joined_date, last_active)
                    VALUES (?, 1, ?, ?)
                ''', (admin_id, datetime.now(), datetime.now()))
            else:
                cursor.execute('''
                    UPDATE users SET is_admin = 1, last_active = ? WHERE user_id = ?
                ''', (datetime.now(), admin_id))
            
            # Initialize balance for admin
            cursor.execute('''
                INSERT OR IGNORE INTO user_balance (user_id, balance_diamonds, balance_rupees, total_earned_diamonds, total_earned_rupees, total_withdrawn_diamonds, total_withdrawn_rupees, last_updated)
                VALUES (?, 0, 0, 0, 0, 0, 0, ?)
            ''', (admin_id, datetime.now()))
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully (1 Diamond = ₹1)")
    
    # ========== BALANCE MANAGEMENT METHODS ==========
    
    def init_user_balance(self, user_id: int):
        """Initialize balance for new user"""
        conn = self.get_connection()
        if not conn:
            return
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO user_balance (user_id, balance_diamonds, balance_rupees, total_earned_diamonds, total_earned_rupees, total_withdrawn_diamonds, total_withdrawn_rupees, last_updated)
            VALUES (?, 0, 0, 0, 0, 0, 0, ?)
        ''', (user_id, datetime.now()))
        conn.commit()
        conn.close()
    
    def get_user_balance(self, user_id: int) -> Dict:
        """Get user's complete balance information (Diamonds & Rupees)"""
        conn = self.get_connection()
        if not conn:
            return {'balance_diamonds': 0, 'balance_rupees': 0, 'total_earned_diamonds': 0, 'total_earned_rupees': 0, 'total_withdrawn_diamonds': 0, 'total_withdrawn_rupees': 0}
        
        cursor = conn.cursor()
        cursor.execute('''
            SELECT balance_diamonds, balance_rupees, total_earned_diamonds, total_earned_rupees, 
                   total_withdrawn_diamonds, total_withdrawn_rupees, last_updated 
            FROM user_balance WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'balance_diamonds': result[0],
                'balance_rupees': result[1],
                'total_earned_diamonds': result[2],
                'total_earned_rupees': result[3],
                'total_withdrawn_diamonds': result[4],
                'total_withdrawn_rupees': result[5],
                'last_updated': result[6]
            }
        else:
            self.init_user_balance(user_id)
            return {'balance_diamonds': 0, 'balance_rupees': 0, 'total_earned_diamonds': 0, 'total_earned_rupees': 0, 'total_withdrawn_diamonds': 0, 'total_withdrawn_rupees': 0}
    
    def add_balance(self, user_id: int, diamonds: int, description: str, reference_id: int = None, created_by: int = None) -> bool:
        """Add balance to user (Diamonds = Rupees)"""
        rupees = diamonds  # 1 Diamond = 1 Rupee
        
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Get current balance
        cursor.execute("SELECT balance_diamonds, balance_rupees, total_earned_diamonds, total_earned_rupees FROM user_balance WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        
        if result:
            new_balance_diamonds = result[0] + diamonds
            new_balance_rupees = result[1] + rupees
            new_total_earned_diamonds = result[2] + diamonds
            new_total_earned_rupees = result[3] + rupees
            
            # Update balance
            cursor.execute('''
                UPDATE user_balance 
                SET balance_diamonds = ?, balance_rupees = ?, 
                    total_earned_diamonds = ?, total_earned_rupees = ?,
                    last_updated = ?
                WHERE user_id = ?
            ''', (new_balance_diamonds, new_balance_rupees, new_total_earned_diamonds, new_total_earned_rupees, datetime.now(), user_id))
            
            # Add transaction record
            cursor.execute('''
                INSERT INTO transaction_history (user_id, type, amount_diamonds, amount_rupees, 
                    balance_after_diamonds, balance_after_rupees, description, reference_id, created_at, created_by)
                VALUES (?, 'credit', ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, diamonds, rupees, new_balance_diamonds, new_balance_rupees, description, reference_id, datetime.now(), created_by))
            
            conn.commit()
            conn.close()
            return True
        conn.close()
        return False
    
    def deduct_balance(self, user_id: int, diamonds: int, description: str, reference_id: int = None, created_by: int = None) -> bool:
        """Deduct balance from user (Diamonds = Rupees)"""
        rupees = diamonds  # 1 Diamond = 1 Rupee
        
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Get current balance
        cursor.execute("SELECT balance_diamonds, balance_rupees, total_withdrawn_diamonds, total_withdrawn_rupees FROM user_balance WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        
        if result and result[0] >= diamonds:
            new_balance_diamonds = result[0] - diamonds
            new_balance_rupees = result[1] - rupees
            new_total_withdrawn_diamonds = result[2] + diamonds
            new_total_withdrawn_rupees = result[3] + rupees
            
            # Update balance
            cursor.execute('''
                UPDATE user_balance 
                SET balance_diamonds = ?, balance_rupees = ?, 
                    total_withdrawn_diamonds = ?, total_withdrawn_rupees = ?,
                    last_updated = ?
                WHERE user_id = ?
            ''', (new_balance_diamonds, new_balance_rupees, new_total_withdrawn_diamonds, new_total_withdrawn_rupees, datetime.now(), user_id))
            
            # Add transaction record
            cursor.execute('''
                INSERT INTO transaction_history (user_id, type, amount_diamonds, amount_rupees, 
                    balance_after_diamonds, balance_after_rupees, description, reference_id, created_at, created_by)
                VALUES (?, 'debit', ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, diamonds, rupees, new_balance_diamonds, new_balance_rupees, description, reference_id, datetime.now(), created_by))
            
            conn.commit()
            conn.close()
            return True
        conn.close()
        return False
    
    def get_transaction_history(self, user_id: int, limit: int = 20) -> List[Dict]:
        """Get user's transaction history"""
        conn = self.get_connection()
        if not conn:
            return []
        
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, type, amount_diamonds, amount_rupees, balance_after_diamonds, balance_after_rupees, description, created_at
            FROM transaction_history
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        ''', (user_id, limit))
        
        results = cursor.fetchall()
        conn.close()
        
        return [{
            'id': r[0],
            'type': r[1],
            'amount_diamonds': r[2],
            'amount_rupees': r[3],
            'balance_after_diamonds': r[4],
            'balance_after_rupees': r[5],
            'description': r[6],
            'created_at': r[7]
        } for r in results]
    
    # ========== REFERRAL METHODS ==========
    
    def process_referral(self, referrer_id: int, new_user_id: int):
        """Process referral and add bonus (10 diamonds = ₹10)"""
        conn = self.get_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        # Update user's referral count
        cursor.execute("UPDATE users SET referral_count = referral_count + 1 WHERE user_id = ?", (referrer_id,))
        
        # Add referral bonus to balance
        self.add_balance(
            referrer_id, 
            REFERRAL_BONUS_DIAMONDS, 
            f"Referral bonus for inviting user {new_user_id} (₹{REFERRAL_BONUS_RUPEE})",
            new_user_id
        )
        
        # Record referral reward
        cursor.execute('''
            INSERT INTO referral_rewards (user_id, referred_user_id, reward_type, amount_diamonds, amount_rupees, awarded_at)
            VALUES (?, ?, 'referral_bonus', ?, ?, ?)
        ''', (referrer_id, new_user_id, REFERRAL_BONUS_DIAMONDS, REFERRAL_BONUS_RUPEE, datetime.now()))
        
        conn.commit()
        conn.close()
    
    def get_referral_details(self, user_id: int) -> Dict:
        """Get detailed referral information"""
        conn = self.get_connection()
        if not conn:
            return {'referred_users': [], 'total_rewards': 0, 'total_rewards_rupees': 0, 'total_referrals': 0}
        
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
            SELECT SUM(amount_diamonds) as total_rewards, SUM(amount_rupees) as total_rewards_rupees, COUNT(*) as total_referrals
            FROM referral_rewards
            WHERE user_id = ?
        ''', (user_id,))
        reward_stats = cursor.fetchone()
        conn.close()
        
        return {
            'referred_users': [{
                'user_id': u[0],
                'username': u[1],
                'first_name': u[2],
                'joined_date': u[3]
            } for u in referred_users],
            'total_rewards': reward_stats[0] if reward_stats[0] else 0,
            'total_rewards_rupees': reward_stats[1] if reward_stats[1] else 0,
            'total_referrals': reward_stats[2] if reward_stats[2] else 0
        }
    
    # ========== WITHDRAWAL METHODS ==========
    
    def create_withdrawal_request(self, user_id: int, diamonds: int, upi_id: str, notes: str = None) -> int:
        """Create a withdrawal request (Diamonds = Rupees)"""
        rupees = diamonds  # 1 Diamond = 1 Rupee
        
        conn = self.get_connection()
        if not conn:
            return 0
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO withdrawal_requests (user_id, amount_diamonds, amount_rupees, upi_id, request_date, status, user_notes)
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
        ''', (user_id, diamonds, rupees, upi_id, datetime.now(), notes))
        conn.commit()
        request_id = cursor.lastrowid
        conn.close()
        return request_id
    
    def get_pending_withdrawals(self) -> List[Dict]:
        """Get all pending withdrawal requests with user details"""
        conn = self.get_connection()
        if not conn:
            return []
        
        cursor = conn.cursor()
        cursor.execute('''
            SELECT w.*, u.username, u.first_name, u.last_name, b.balance_diamonds, b.balance_rupees
            FROM withdrawal_requests w
            JOIN users u ON w.user_id = u.user_id
            LEFT JOIN user_balance b ON u.user_id = b.user_id
            WHERE w.status = 'pending'
            ORDER BY w.request_date ASC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        return [{
            'id': r[0],
            'user_id': r[1],
            'amount_diamonds': r[2],
            'amount_rupees': r[3],
            'upi_id': r[4],
            'status': r[5],
            'request_date': r[6],
            'processed_date': r[7],
            'processed_by': r[8],
            'transaction_id': r[9],
            'admin_notes': r[10],
            'user_notes': r[11],
            'username': r[12],
            'first_name': r[13],
            'last_name': r[14],
            'current_balance_diamonds': r[15] if r[15] else 0,
            'current_balance_rupees': r[16] if r[16] else 0
        } for r in results]
    
    def get_withdrawal_history(self, status: str = None, limit: int = 50) -> List[Dict]:
        """Get withdrawal history"""
        conn = self.get_connection()
        if not conn:
            return []
        
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
        conn.close()
        
        return [{
            'id': r[0],
            'user_id': r[1],
            'amount_diamonds': r[2],
            'amount_rupees': r[3],
            'upi_id': r[4],
            'status': r[5],
            'request_date': r[6],
            'processed_date': r[7],
            'processed_by': r[8],
            'transaction_id': r[9],
            'admin_notes': r[10],
            'user_notes': r[11],
            'username': r[12],
            'first_name': r[13],
            'last_name': r[14]
        } for r in results]
    
    def approve_withdrawal(self, request_id: int, admin_id: int, transaction_id: str = None, admin_notes: str = None) -> bool:
        """Approve a withdrawal request"""
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Get request details
        cursor.execute("SELECT user_id, amount_diamonds FROM withdrawal_requests WHERE id = ? AND status = 'pending'", (request_id,))
        request = cursor.fetchone()
        
        if request:
            user_id, amount_diamonds = request
            
            # Update request status
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
            conn.close()
            return True
        conn.close()
        return False
    
    def reject_withdrawal(self, request_id: int, admin_id: int, admin_notes: str = None) -> bool:
        """Reject a withdrawal request and refund balance"""
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Get request details
        cursor.execute("SELECT user_id, amount_diamonds FROM withdrawal_requests WHERE id = ? AND status = 'pending'", (request_id,))
        request = cursor.fetchone()
        
        if request:
            user_id, amount_diamonds = request
            
            # Refund the amount back to user
            self.add_balance(user_id, amount_diamonds, f"Withdrawal refund - Request #{request_id}", request_id, admin_id)
            
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
            conn.close()
            return True
        conn.close()
        return False
    
    def get_withdrawal_statistics(self) -> Dict:
        """Get withdrawal statistics"""
        conn = self.get_connection()
        if not conn:
            return {'pending_count': 0, 'pending_amount_diamonds': 0, 'pending_amount_rupees': 0}
        
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*), SUM(amount_diamonds), SUM(amount_rupees) FROM withdrawal_requests WHERE status = 'pending'")
        pending = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*), SUM(amount_diamonds), SUM(amount_rupees) FROM withdrawal_requests WHERE status = 'approved'")
        approved = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*), SUM(amount_diamonds), SUM(amount_rupees) FROM withdrawal_requests WHERE status = 'rejected'")
        rejected = cursor.fetchone()
        conn.close()
        
        return {
            'pending_count': pending[0] or 0,
            'pending_amount_diamonds': pending[1] or 0,
            'pending_amount_rupees': pending[2] or 0,
            'approved_count': approved[0] or 0,
            'approved_amount_diamonds': approved[1] or 0,
            'approved_amount_rupees': approved[2] or 0,
            'rejected_count': rejected[0] or 0,
            'rejected_amount_diamonds': rejected[1] or 0,
            'rejected_amount_rupees': rejected[2] or 0
        }
    
    # ========== USER MANAGEMENT ==========
    
    def is_admin(self, user_id: int) -> bool:
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] == 1 if result else False
    
    def is_banned(self, user_id: int) -> bool:
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] == 1 if result else False
    
    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = "", referred_by: int = None):
        conn = self.get_connection()
        if not conn:
            return False
        
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
        conn.close()
        return True
    
    def update_last_active(self, user_id: int):
        conn = self.get_connection()
        if not conn:
            return
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET last_active = ? WHERE user_id = ?", (datetime.now(), user_id))
        conn.commit()
        conn.close()
    
    def get_user_stats(self, user_id: int) -> Dict:
        conn = self.get_connection()
        if not conn:
            return {}
        cursor = conn.cursor()
        cursor.execute('''
            SELECT referral_count, total_interactions, total_reads, joined_date, is_premium 
            FROM users WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        conn.close()
        
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
        conn = self.get_connection()
        if not conn:
            return
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET total_interactions = total_interactions + 1, last_active = ? WHERE user_id = ?", (datetime.now(), user_id))
        conn.commit()
        conn.close()
    
    def ban_user(self, user_id: int) -> bool:
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (user_id,))
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected > 0
    
    def unban_user(self, user_id: int) -> bool:
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (user_id,))
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected > 0
    
    def make_admin(self, user_id: int) -> bool:
        conn = self.get_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (user_id,))
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected > 0
    
    def get_user_count(self) -> int:
        conn = self.get_connection()
        if not conn:
            return 0
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0
    
    def get_admin_count(self) -> int:
        conn = self.get_connection()
        if not conn:
            return 0
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0
    
    def get_banned_count(self) -> int:
        conn = self.get_connection()
        if not conn:
            return 0
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0

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
            InlineKeyboardButton(text=f"💎 {balance['balance_diamonds']} (₹{balance['balance_rupees']})", callback_data="my_balance"),
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
        f"💰 **Your Balance:**\n"
        f"💎 {balance['balance_diamonds']} Diamonds = ₹{balance['balance_rupees']}\n"
        f"📈 **Total Earned:** ₹{balance['total_earned_rupees']}\n\n"
        f"📱 Click 'Open Stories App' to read stories!\n"
        f"💰 Invite friends and earn **₹{REFERRAL_BONUS_RUPEE}** each!\n"
        f"💳 Withdraw when you reach ₹{WITHDRAWAL_MINIMUM_RUPEE}\n\n"
        f"💡 **1 Diamond = ₹1**\n\n"
        f"Choose an option below:"
    )
    
    await message.answer(
        welcome_text,
        reply_markup=await get_main_keyboard(user.id, is_admin),
        parse_mode="Markdown"
    )
    await state.clear()

# ================= WITHDRAWAL HANDLERS =================
@dp.callback_query(lambda c: c.data == "withdraw")
async def withdraw_handler(callback_query: types.CallbackQuery, state: FSMContext):
    db = Database(DATABASE_FILE)
    balance = db.get_user_balance(callback_query.from_user.id)
    
    if balance['balance_rupees'] < WITHDRAWAL_MINIMUM_RUPEE:
        await callback_query.answer(f"Minimum withdrawal is ₹{WITHDRAWAL_MINIMUM_RUPEE}! You have ₹{balance['balance_rupees']}.", show_alert=True)
        return
    
    await callback_query.message.answer(
        f"🏧 **Withdrawal Request** 🏧\n\n"
        f"💰 Available Balance:\n"
        f"💎 {balance['balance_diamonds']} Diamonds = ₹{balance['balance_rupees']}\n"
        f"💳 Minimum: ₹{WITHDRAWAL_MINIMUM_RUPEE}\n\n"
        f"**Enter amount to withdraw (in ₹):**\n"
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
        amount_rupees = int(message.text)
        db = Database(DATABASE_FILE)
        balance = db.get_user_balance(message.from_user.id)
        
        if amount_rupees < WITHDRAWAL_MINIMUM_RUPEE:
            await message.answer(f"❌ Minimum withdrawal amount is ₹{WITHDRAWAL_MINIMUM_RUPEE}!")
            return
        
        if amount_rupees > balance['balance_rupees']:
            await message.answer(f"❌ Insufficient balance! You have ₹{balance['balance_rupees']}.")
            return
        
        amount_diamonds = amount_rupees  # 1 Diamond = ₹1
        await state.update_data(withdraw_amount_diamonds=amount_diamonds, withdraw_amount_rupees=amount_rupees)
        
        await message.answer(
            f"💰 **Amount:** ₹{amount_rupees} ({amount_diamonds} Diamonds)\n\n"
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
    amount_diamonds = data.get('withdraw_amount_diamonds')
    amount_rupees = data.get('withdraw_amount_rupees')
    
    db = Database(DATABASE_FILE)
    
    # Deduct balance first
    if db.deduct_balance(message.from_user.id, amount_diamonds, f"Withdrawal request of ₹{amount_rupees}", None):
        request_id = db.create_withdrawal_request(
            message.from_user.id,
            amount_diamonds,
            upi_id,
            None
        )
        
        await message.answer(
            f"✅ **Withdrawal Request Submitted!** ✅\n\n"
            f"💰 Amount: ₹{amount_rupees} ({amount_diamonds} Diamonds)\n"
            f"📱 UPI ID: `{upi_id}`\n"
            f"🆔 Request ID: #{request_id}\n\n"
            f"⏳ **Status:** Pending Admin Approval\n\n"
            f"📌 Your balance has been deducted. If rejected, amount will be refunded.\n"
            f"📞 Admin will process within 24-48 hours.\n\n"
            f"Thank you for your patience! 🙏",
            parse_mode="Markdown"
        )
        
        # Send notification to ALL admins
        new_balance = db.get_user_balance(message.from_user.id)
        for admin_id in ADMIN_IDS:
            try:
                admin_text = (
                    f"💰 **NEW WITHDRAWAL REQUEST** 💰\n\n"
                    f"┌── 📋 **Request Details**\n"
                    f"├─ 🆔 Request ID: `#{request_id}`\n"
                    f"├─ 👤 User: {message.from_user.first_name} {message.from_user.last_name or ''}\n"
                    f"├─ 📝 Username: @{message.from_user.username or 'N/A'}\n"
                    f"├─ 🆔 User ID: `{message.from_user.id}`\n"
                    f"├─ 💰 Amount: ₹{amount_rupees} ({amount_diamonds}💎)\n"
                    f"├─ 📱 UPI ID: `{upi_id}`\n"
                    f"├─ 📅 Requested: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"└─ 💎 Balance after deduction: ₹{new_balance['balance_rupees']}\n\n"
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
    conn = db.get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(balance_diamonds), SUM(balance_rupees), SUM(total_earned_diamonds), SUM(total_earned_rupees), SUM(total_withdrawn_diamonds), SUM(total_withdrawn_rupees) FROM user_balance")
        financial_stats = cursor.fetchone()
        conn.close()
    else:
        financial_stats = (0, 0, 0, 0, 0, 0)
    
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
        f"💰 **Financial Statistics (1💎 = ₹1):**\n"
        f"• Total Balance: ₹{financial_stats[1] or 0}\n"
        f"• Total Earned: ₹{financial_stats[3] or 0}\n"
        f"• Total Withdrawn: ₹{financial_stats[5] or 0}\n\n"
        f"🏧 **Withdrawal Stats:**\n"
        f"• Pending: {withdrawal_stats['pending_count']} (₹{withdrawal_stats['pending_amount_rupees']})\n"
        f"• Approved: {withdrawal_stats['approved_count']} (₹{withdrawal_stats['approved_amount_rupees']})\n"
        f"• Rejected: {withdrawal_stats['rejected_count']} (₹{withdrawal_stats['rejected_amount_rupees']})\n\n"
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
                text=f"💰 {wd['first_name']} - ₹{wd['amount_rupees']} - {upi_short}",
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
    
    conn = db.get_connection()
    if not conn:
        await callback_query.message.answer("Database error!")
        return
    
    cursor = conn.cursor()
    cursor.execute('''
        SELECT w.*, u.first_name, u.last_name, u.username, u.joined_date, b.balance_diamonds, b.balance_rupees
        FROM withdrawal_requests w
        JOIN users u ON w.user_id = u.user_id
        LEFT JOIN user_balance b ON u.user_id = b.user_id
        WHERE w.id = ?
    ''', (withdrawal_id,))
    result = cursor.fetchone()
    conn.close()
    
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
        f"├─ 👤 User: {result[13]} {result[14] or ''}\n"
        f"├─ 📝 Username: @{result[15] or 'N/A'}\n"
        f"├─ 🆔 User ID: `{result[1]}`\n"
        f"├─ 📅 Joined: {result[16][:10] if result[16] else 'N/A'}\n"
        f"├─ 💰 Amount: ₹{result[3]} ({result[2]}💎)\n"
        f"├─ 📱 UPI ID: `{result[4]}`\n"
        f"├─ 📅 Requested: {result[6]}\n"
        f"├─ 💎 Current Balance: {result[18] if result[18] else 0}💎 (₹{result[19] if result[19] else 0})\n"
        f"└─ 📝 Notes: {result[11] or 'None'}\n\n"
        f"**Actions:**\n"
        f"✅ Approve - Send ₹{result[3]} to UPI ID\n"
        f"❌ Reject - Refund amount to user"
    )
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data.startswith("admin_approve_"))
async def admin_approve_withdrawal(callback_query: types.CallbackQuery):
    withdrawal_id = int(callback_query.data.split("_")[2])
    db = Database(DATABASE_FILE)
    
    conn = db.get_connection()
    if not conn:
        await callback_query.answer("Database error!", show_alert=True)
        return
    
    cursor = conn.cursor()
    cursor.execute('''
        SELECT user_id, amount_diamonds, amount_rupees, upi_id FROM withdrawal_requests WHERE id = ? AND status = 'pending'
    ''', (withdrawal_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result and db.approve_withdrawal(withdrawal_id, callback_query.from_user.id):
        user_id, amount_diamonds, amount_rupees, upi_id = result
        
        await callback_query.answer("✅ Withdrawal approved!", show_alert=True)
        await callback_query.message.edit_text(
            f"✅ **Withdrawal #{withdrawal_id} Approved!**\n\n"
            f"💰 Amount: ₹{amount_rupees} ({amount_diamonds}💎)\n"
            f"📱 UPI ID: `{upi_id}`\n\n"
            f"Payment has been approved.",
            parse_mode="Markdown"
        )
        
        # Notify user
        try:
            await bot.send_message(
                user_id,
                f"✅ **Withdrawal Approved!** ✅\n\n"
                f"💰 Amount: ₹{amount_rupees} ({amount_diamonds} Diamonds)\n"
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
    
    conn = db.get_connection()
    if not conn:
        await callback_query.answer("Database error!", show_alert=True)
        return
    
    cursor = conn.cursor()
    cursor.execute('''
        SELECT user_id, amount_diamonds, amount_rupees FROM withdrawal_requests WHERE id = ? AND status = 'pending'
    ''', (withdrawal_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result and db.reject_withdrawal(withdrawal_id, callback_query.from_user.id):
        user_id, amount_diamonds, amount_rupees = result
        
        await callback_query.answer("❌ Withdrawal rejected!", show_alert=True)
        await callback_query.message.edit_text(
            f"❌ **Withdrawal #{withdrawal_id} Rejected!**\n\n"
            f"💰 Amount: ₹{amount_rupees} ({amount_diamonds}💎)\n\n"
            f"Amount has been refunded to user's balance.",
            parse_mode="Markdown"
        )
        
        # Notify user
        try:
            await bot.send_message(
                user_id,
                f"❌ **Withdrawal Rejected** ❌\n\n"
                f"💰 Amount: ₹{amount_rupees} ({amount_diamonds} Diamonds)\n"
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
            text += f"• #{wd['id']} - {wd['first_name']} - ₹{wd['amount_rupees']} - {wd['processed_date'][:10]}\n"
        text += "\n"
    
    if rejected:
        text += "❌ **Rejected Withdrawals:**\n"
        for wd in rejected[:10]:
            text += f"• #{wd['id']} - {wd['first_name']} - ₹{wd['amount_rupees']} - {wd['processed_date'][:10]}\n"
    
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
        f"💰 **Current Balance:**\n"
        f"💎 {balance['balance_diamonds']} Diamonds = ₹{balance['balance_rupees']}\n\n"
        f"📈 **Total Earned:** ₹{balance['total_earned_rupees']}\n"
        f"💸 **Total Withdrawn:** ₹{balance['total_withdrawn_rupees']}\n"
        f"👥 **Referrals:** {stats.get('referral_count', 0)}\n\n"
        f"💡 **Earn More:**\n"
        f"• ₹{REFERRAL_BONUS_RUPEE} per referral\n"
        f"• 1 Diamond = ₹1\n"
        f"• Bonus for active reading\n\n"
        f"💳 **Minimum Withdrawal:** ₹{WITHDRAWAL_MINIMUM_RUPEE}"
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
        text += f"{emoji} ₹{tx['amount_rupees']} - {tx['description'][:40]}\n"
        text += f"   📅 {tx['created_at'][:16]} | 💰 Balance: ₹{tx['balance_after_rupees']}\n\n"
    
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
        [InlineKeyboardButton(text="📤 Share Link", switch_inline_query=f"Join and earn ₹{REFERRAL_BONUS_RUPEE}: {link}")],
        [InlineKeyboardButton(text="👥 My Referrals ({})".format(referral_details['total_referrals']), callback_data="my_referrals")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back_to_menu")]
    ])
    
    await callback_query.message.edit_text(
        f"💰 **Refer & Earn Program** 💰\n\n"
        f"🔗 **Your Referral Link:**\n`{link}`\n\n"
        f"📊 **Your Earnings:**\n"
        f"💰 Balance: ₹{balance['balance_rupees']} ({balance['balance_diamonds']}💎)\n"
        f"📈 Total Earned: ₹{balance['total_earned_rupees']}\n"
        f"👥 Total Referrals: {referral_details['total_referrals']}\n"
        f"⭐ Referral Bonus: ₹{REFERRAL_BONUS_RUPEE} each\n\n"
        f"🎁 **Bonus Milestones:**\n"
        f"• 10 referrals: +₹50 bonus\n"
        f"• 25 referrals: +₹150 bonus\n"
        f"• 50 referrals: +₹500 bonus\n"
        f"• 100 referrals: Premium access for life!\n\n"
        f"💡 **1 Diamond = ₹1**\n\n"
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
        f"💰 **Balance:** ₹{balance['balance_rupees']} ({balance['balance_diamonds']}💎)\n"
        f"📈 **Total Earned:** ₹{balance['total_earned_rupees']}\n"
        f"👥 **Referrals:** {stats.get('referral_count', 0)}\n"
        f"📖 **Stories Read:** {stats.get('reads', 0)}\n"
        f"🔄 **Interactions:** {stats.get('interactions', 0)}\n"
        f"📅 **Member Since:** {stats.get('joined_date', 'Unknown')[:10] if stats.get('joined_date') else 'Unknown'}\n"
        f"💎 **1 Diamond = ₹1**"
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

💰 **Earn Money:**
• ₹{REFERRAL_BONUS_RUPEE} per referral
• 1 Diamond = ₹1
• Bonus at referral milestones

💳 **Withdraw Money:**
• Minimum: ₹{WITHDRAWAL_MINIMUM_RUPEE}
• UPI transfer only
• Processed within 24-48 hours

📚 **Read Stories:**
• Open Stories App button
• Latest & Popular stories

👥 **Referral Program:**
• Share your unique link
• Track your referrals
• Earn unlimited money

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
    db = Database(DATABASE_FILE)
    
    if not db.is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Admin only!", show_alert=True)
        return
    
    conn = db.get_connection()
    if not conn:
        await callback_query.message.answer("Database error!")
        return
    
    cursor = conn.cursor()
    cursor.execute('''
        SELECT u.user_id, u.first_name, u.username, u.is_admin, u.is_banned, 
               COALESCE(b.balance_diamonds, 0) as balance_diamonds,
               COALESCE(b.balance_rupees, 0) as balance_rupees
        FROM users u
        LEFT JOIN user_balance b ON u.user_id = b.user_id
        ORDER BY balance_rupees DESC
        LIMIT 20
    ''')
    users = cursor.fetchall()
    conn.close()
    
    if not users:
        await callback_query.message.answer("No users found!")
        return
    
    text = "👥 **Top Users by Balance** 👥\n\n"
    for user in users:
        status = "👑" if user[3] else "👤"
        if user[4]:
            status = "🚫"
        text += f"{status} {user[1]} (@{user[2] or 'N/A'})\n"
        text += f"   💎 {user[5]} Diamonds = ₹{user[6]}\n"
        text += f"   🆔 `{user[0]}`\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")]
    ])
    
    await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "admin_adjust_balance")
async def admin_adjust_balance(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer(
        "💰 **Adjust User Balance**\n\n"
        "Send: `user_id amount_rupees description`\n"
        "Example: `123456789 100 Bonus for activity`\n"
        "Use negative for deduct: `123456789 -50 Penalty`\n\n"
        "💰 **1 Diamond = ₹1**\n\n"
        "Type /cancel to cancel",
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
        amount_rupees = int(parts[1])
        description = parts[2] if len(parts) > 2 else "Admin adjustment"
        
        amount_diamonds = amount_rupees  # 1 Diamond = ₹1
        
        db = Database(DATABASE_FILE)
        
        if amount_diamonds > 0:
            if db.add_balance(user_id, amount_diamonds, description, created_by=message.from_user.id):
                await message.answer(f"✅ Added ₹{amount_rupees} ({amount_diamonds}💎) to user {user_id}")
                new_balance = db.get_user_balance(user_id)
                
                # Notify user
                try:
                    await bot.send_message(
                        user_id,
                        f"➕ **Balance Updated**\n\n"
                        f"Added: +₹{amount_rupees} (+{amount_diamonds}💎)\n"
                        f"Reason: {description}\n"
                        f"New Balance: ₹{new_balance['balance_rupees']} ({new_balance['balance_diamonds']}💎)",
                        parse_mode="Markdown"
                    )
                except:
                    pass
            else:
                await message.answer(f"❌ Failed to add balance to user {user_id}")
        else:
            if db.deduct_balance(user_id, abs(amount_diamonds), description, created_by=message.from_user.id):
                await message.answer(f"✅ Deducted ₹{abs(amount_rupees)} ({abs(amount_diamonds)}💎) from user {user_id}")
                new_balance = db.get_user_balance(user_id)
                
                # Notify user
                try:
                    await bot.send_message(
                        user_id,
                        f"➖ **Balance Updated**\n\n"
                        f"Deducted: -₹{abs(amount_rupees)} (-{abs(amount_diamonds)}💎)\n"
                        f"Reason: {description}\n"
                        f"New Balance: ₹{new_balance['balance_rupees']} ({new_balance['balance_diamonds']}💎)",
                        parse_mode="Markdown"
                    )
                except:
                    pass
            else:
                await message.answer(f"❌ Failed to deduct balance from user {user_id}")
            
    except Exception as e:
        await message.answer(f"❌ Error: {str(e)}")
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def admin_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer("📢 Send your broadcast message:\n(Type /cancel to cancel)")
    await state.set_state(AdminStates.broadcasting)
    await callback_query.answer()

@dp.message(AdminStates.broadcasting)
async def process_broadcast(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await admin_panel(message)
        return
    
    db = Database(DATABASE_FILE)
    
    conn = db.get_connection()
    if not conn:
        await message.answer("Database error!")
        return
    
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE is_banned = 0")
    users = cursor.fetchall()
    conn.close()
    
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
    
    await status_msg.edit_text(f"✅ Broadcast completed!\n✅ Sent: {sent}\n❌ Failed: {failed}")
    await state.clear()

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await start_command(callback_query.message, state)

# ================= MAIN =================
async def main():
    logger.info("🚀 Starting bot with Refer & Earn System (1💎 = ₹1)...")
    
    # Delete old database file if exists (optional - comment out if you want to keep data)
    if os.path.exists(DATABASE_FILE):
        logger.info("Removing old database to create fresh schema...")
        try:
            os.remove(DATABASE_FILE)
        except:
            pass
    
    db = Database(DATABASE_FILE)
    logger.info(f"Bot started! Admin IDs: {ADMIN_IDS}")
    logger.info(f"Referral Bonus: ₹{REFERRAL_BONUS_RUPEE} per referral")
    logger.info(f"Minimum Withdrawal: ₹{WITHDRAWAL_MINIMUM_RUPEE}")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
