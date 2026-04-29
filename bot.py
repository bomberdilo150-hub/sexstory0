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

# ================= CONFIG =================
API_TOKEN = "8777177819:AAHuJtPJR8VmoWSfqHtrHW7WeVNWJ6sbV7o"
WEBSITE_URL = "https://sexstory.lovable.app"
DATABASE_FILE = "bot_database.db"
ADMIN_IDS = [8459969831]

REFERRAL_BONUS = 10
MINIMUM_WITHDRAWAL = 100

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= SIMPLE DATABASE (NO LOCK ISSUES) =================
class Database:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._create_tables()
    
    def _create_tables(self):
        """Create tables if not exists - called only once"""
        conn = sqlite3.connect(self.db_file, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
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
                is_banned BOOLEAN DEFAULT 0
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
        
        # Add admin
        for admin_id in ADMIN_IDS:
            cursor.execute('''
                INSERT OR IGNORE INTO users (user_id, is_admin, joined_date, balance)
                VALUES (?, 1, ?, 0)
            ''', (admin_id, datetime.now()))
        
        conn.commit()
        conn.close()
        logger.info("Database tables created")
    
    def _query(self, query, params=(), fetch_one=False, fetch_all=False):
        """Simple query executor with auto-commit"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file, timeout=5)
            conn.execute("PRAGMA journal_mode=WAL")
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            if fetch_one:
                result = cursor.fetchone()
                conn.commit()
                return result
            elif fetch_all:
                result = cursor.fetchall()
                conn.commit()
                return result
            else:
                conn.commit()
                return cursor.lastrowid if query.strip().upper().startswith('INSERT') else None
        except Exception as e:
            logger.error(f"Database error: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = "", referred_by: int = None):
        # Check if user exists
        existing = self._query("SELECT user_id FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
        
        if not existing:
            # Add new user
            self._query('''
                INSERT INTO users (user_id, username, first_name, last_name, joined_date, balance)
                VALUES (?, ?, ?, ?, ?, 0)
            ''', (user_id, username, first_name, last_name, datetime.now()))
            
            # Add referral bonus
            if referred_by and referred_by != user_id:
                self.add_balance(referred_by, REFERRAL_BONUS)
                self._query("UPDATE users SET referral_count = referral_count + 1 WHERE user_id = ?", (referred_by,))
        
        return True
    
    def get_balance(self, user_id: int) -> int:
        result = self._query("SELECT balance FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
        return result[0] if result else 0
    
    def add_balance(self, user_id: int, amount: int):
        self._query("UPDATE users SET balance = balance + ?, total_earned = total_earned + ? WHERE user_id = ?", 
                   (amount, amount, user_id))
    
    def deduct_balance(self, user_id: int, amount: int) -> bool:
        balance = self.get_balance(user_id)
        if balance >= amount:
            self._query("UPDATE users SET balance = balance - ?, total_withdrawn = total_withdrawn + ? WHERE user_id = ?", 
                       (amount, amount, user_id))
            return True
        return False
    
    def create_withdrawal(self, user_id: int, amount: int, upi_id: str) -> int:
        return self._query('''
            INSERT INTO withdrawals (user_id, amount, upi_id, request_date)
            VALUES (?, ?, ?, ?)
        ''', (user_id, amount, upi_id, datetime.now()))
    
    def get_pending_withdrawals(self):
        return self._query('''
            SELECT w.id, w.user_id, w.amount, w.upi_id, w.status, w.request_date,
                   u.first_name, u.username
            FROM withdrawals w
            JOIN users u ON w.user_id = u.user_id
            WHERE w.status = 'pending'
            ORDER BY w.request_date ASC
        ''', fetch_all=True) or []
    
    def approve_withdrawal(self, withdraw_id: int):
        self._query("UPDATE withdrawals SET status = 'approved' WHERE id = ?", (withdraw_id,))
    
    def reject_withdrawal(self, withdraw_id: int, user_id: int, amount: int):
        self._query("UPDATE withdrawals SET status = 'rejected' WHERE id = ?", (withdraw_id,))
        self.add_balance(user_id, amount)  # Refund
    
    def get_user_stats(self, user_id: int):
        result = self._query('''
            SELECT balance, total_earned, total_withdrawn, referral_count, joined_date
            FROM users WHERE user_id = ?
        ''', (user_id,), fetch_one=True)
        
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
        result = self._query("SELECT is_admin FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
        return result[0] == 1 if result else False
    
    def is_banned(self, user_id: int) -> bool:
        result = self._query("SELECT is_banned FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
        return result[0] == 1 if result else False
    
    def get_all_users(self):
        return self._query("SELECT user_id FROM users WHERE is_banned = 0", fetch_all=True) or []

# ================= STATES =================
class WithdrawState(StatesGroup):
    amount = State()
    upi = State()

class AdminState(StatesGroup):
    broadcast = State()

# ================= KEYBOARDS =================
def get_main_keyboard(is_admin: bool = False):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Open App", web_app=WebAppInfo(url=WEBSITE_URL))],
        [InlineKeyboardButton(text="📚 Stories", callback_data="stories"), InlineKeyboardButton(text="💰 Refer", callback_data="referral")],
        [InlineKeyboardButton(text="💳 Balance", callback_data="balance"), InlineKeyboardButton(text="📊 Stats", callback_data="stats")],
        [InlineKeyboardButton(text="🏧 Withdraw", callback_data="withdraw"), InlineKeyboardButton(text="❓ Help", callback_data="help")]
    ])
    
    if is_admin:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="👑 Admin", callback_data="admin_panel")])
    
    return keyboard

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user = message.from_user
    db = Database(DATABASE_FILE)
    
    if db.is_banned(user.id):
        await message.answer("❌ You are banned!")
        return
    
    # Check referral
    args = message.text.split()
    referred_by = int(args[1]) if len(args) > 1 and args[1].isdigit() else None
    
    db.add_user(user.id, user.username, user.first_name, user.last_name or "", referred_by)
    balance = db.get_balance(user.id)
    
    text = f"""🌟 Welcome {user.first_name}! 🌟

💰 Balance: ₹{balance}

📱 Click 'Open App' to read stories!
💰 Invite friends - Earn ₹{REFERRAL_BONUS} each!
💳 Withdraw at ₹{MINIMUM_WITHDRAWAL}

1 Diamond = ₹1"""
    
    await message.answer(text, reply_markup=get_main_keyboard(db.is_admin(user.id)), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "balance")
async def balance_cmd(callback: types.CallbackQuery):
    await callback.answer()
    db = Database(DATABASE_FILE)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""💳 Your Balance

💰 Current: ₹{stats['balance']}
📈 Total Earned: ₹{stats['total_earned']}
💸 Withdrawn: ₹{stats['total_withdrawn']}
👥 Referrals: {stats['referral_count']}

Earn ₹{REFERRAL_BONUS}/referral!"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "stats")
async def stats_cmd(callback: types.CallbackQuery):
    await callback.answer()
    db = Database(DATABASE_FILE)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""📊 Your Stats

💰 Balance: ₹{stats['balance']}
📈 Earned: ₹{stats['total_earned']}
👥 Referrals: {stats['referral_count']}
📅 Joined: {str(stats['joined_date'])[:10]}"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "referral")
async def referral_cmd(callback: types.CallbackQuery):
    await callback.answer()
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

🎁 Per Referral: ₹{REFERRAL_BONUS}"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Share", switch_inline_query=f"Join: {link}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "withdraw")
async def withdraw_cmd(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    
    if balance < MINIMUM_WITHDRAWAL:
        await callback.answer(f"Need ₹{MINIMUM_WITHDRAWAL}, you have ₹{balance}", show_alert=True)
        return
    
    await callback.message.answer(f"💰 Balance: ₹{balance}\n\nEnter amount (min ₹{MINIMUM_WITHDRAWAL}):\nType /cancel")
    await state.set_state(WithdrawState.amount)

@dp.message(WithdrawState.amount)
async def withdraw_amount(msg: types.Message, state: FSMContext):
    if msg.text == "/cancel":
        await state.clear()
        await start_cmd(msg)
        return
    
    try:
        amount = int(msg.text)
        db = Database(DATABASE_FILE)
        balance = db.get_balance(msg.from_user.id)
        
        if amount < MINIMUM_WITHDRAWAL:
            await msg.answer(f"❌ Minimum ₹{MINIMUM_WITHDRAWAL}!")
            return
        if amount > balance:
            await msg.answer(f"❌ You have ₹{balance} only!")
            return
        
        await state.update_data(amount=amount)
        await msg.answer("📱 Enter UPI ID:\nExample: name@okhdfcbank\nType /cancel")
        await state.set_state(WithdrawState.upi)
    except ValueError:
        await msg.answer("❌ Enter valid number!")

@dp.message(WithdrawState.upi)
async def withdraw_upi(msg: types.Message, state: FSMContext):
    if msg.text == "/cancel":
        await state.clear()
        await start_cmd(msg)
        return
    
    upi_id = msg.text.strip()
    if '@' not in upi_id:
        await msg.answer("❌ Invalid UPI! Use: name@bank")
        return
    
    data = await state.get_data()
    amount = data['amount']
    db = Database(DATABASE_FILE)
    
    if db.deduct_balance(msg.from_user.id, amount):
        wd_id = db.create_withdrawal(msg.from_user.id, amount, upi_id)
        
        await msg.answer(f"""✅ Withdrawal Requested!

💰 Amount: ₹{amount}
📱 UPI: `{upi_id}`
🆔 ID: #{wd_id}

Admin will process soon.""", parse_mode="Markdown")
        
        # Notify admins
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, f"💰 WITHDRAWAL\nUser: {msg.from_user.first_name}\nAmount: ₹{amount}\nUPI: {upi_id}\nID: #{wd_id}")
            except:
                pass
    else:
        await msg.answer("❌ Failed!")
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "stories")
async def stories_cmd(callback: types.CallbackQuery):
    await callback.answer()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Story 1", url=f"{WEBSITE_URL}/story/1")],
        [InlineKeyboardButton(text="📖 Story 2", url=f"{WEBSITE_URL}/story/2")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back")]
    ])
    await callback.message.edit_text("📚 Stories:", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "help")
async def help_cmd(callback: types.CallbackQuery):
    await callback.answer()
    text = f"""📖 Help

💰 Earn: ₹{REFERRAL_BONUS}/referral
💳 Withdraw: Min ₹{MINIMUM_WITHDRAWAL}
📱 Payment: UPI transfer

/start - Restart
/help - This help"""
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "back")
async def back_cmd(callback: types.CallbackQuery):
    await callback.answer()
    db = Database(DATABASE_FILE)
    balance = db.get_balance(callback.from_user.id)
    await callback.message.edit_text(f"🌟 Main Menu\n\n💰 Balance: ₹{balance}", 
                                     reply_markup=get_main_keyboard(), parse_mode="Markdown")

# ================= ADMIN PANEL =================
@dp.callback_query(lambda c: c.data == "admin_panel")
async def admin_panel(callback: types.CallbackQuery):
    await callback.answer()
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    pending = db.get_pending_withdrawals()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💰 Withdrawals ({len(pending)})", callback_data="admin_wd")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_bc")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="back")]
    ])
    await callback.message.edit_text("👑 Admin Panel", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "admin_wd")
async def admin_withdrawals(callback: types.CallbackQuery):
    await callback.answer()
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback.from_user.id):
        return
    
    withdrawals = db.get_pending_withdrawals()
    if not withdrawals:
        await callback.message.answer("No pending withdrawals!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for w in withdrawals:
        name = w[6] if w[6] else f"User_{w[1]}"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"💰 {name[:12]} - ₹{w[2]}", callback_data=f"process_{w[0]}")
        ])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")])
    
    await callback.message.edit_text(f"💰 Pending ({len(withdrawals)})", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data.startswith("process_"))
async def process_wd(callback: types.CallbackQuery):
    await callback.answer()
    wd_id = int(callback.data.split("_")[1])
    db = Database(DATABASE_FILE)
    
    withdrawals = db.get_pending_withdrawals()
    w = next((x for x in withdrawals if x[0] == wd_id), None)
    
    if not w:
        await callback.message.answer("Not found!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{wd_id}"),
         InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{wd_id}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_wd")]
    ])
    
    text = f"""💰 Withdrawal #{w[0]}

👤 {w[6]} @{w[7] or 'N/A'}
💰 ₹{w[2]}
📱 {w[3]}
📅 {w[5][:16]}"""
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data.startswith("approve_"))
async def approve_wd(callback: types.CallbackQuery):
    wd_id = int(callback.data.split("_")[1])
    db = Database(DATABASE_FILE)
    db.approve_withdrawal(wd_id)
    await callback.answer("✅ Approved!", show_alert=True)
    await callback.message.edit_text("✅ Approved!")
    await asyncio.sleep(1)
    await admin_withdrawals(callback)

@dp.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_wd(callback: types.CallbackQuery):
    wd_id = int(callback.data.split("_")[1])
    db = Database(DATABASE_FILE)
    
    # Get user and amount
    result = db._query("SELECT user_id, amount FROM withdrawals WHERE id = ?", (wd_id,), fetch_one=True)
    if result:
        user_id, amount = result
        db.reject_withdrawal(wd_id, user_id, amount)
        try:
            await bot.send_message(user_id, f"❌ Withdrawal ₹{amount} rejected. Refunded.")
        except:
            pass
    
    await callback.answer("❌ Rejected!", show_alert=True)
    await callback.message.edit_text("❌ Rejected!")
    await asyncio.sleep(1)
    await admin_withdrawals(callback)

@dp.callback_query(lambda c: c.data == "admin_bc")
async def admin_broadcast_panel(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    db = Database(DATABASE_FILE)
    if not db.is_admin(callback.from_user.id):
        return
    
    await callback.message.answer("📢 Send broadcast message:\nType /cancel")
    await state.set_state(AdminState.broadcast)

@dp.message(AdminState.broadcast)
async def process_broadcast(msg: types.Message, state: FSMContext):
    if msg.text == "/cancel":
        await state.clear()
        await start_cmd(msg)
        return
    
    db = Database(DATABASE_FILE)
    users = db.get_all_users()
    
    sent = 0
    status = await msg.answer("⏳ Broadcasting...")
    
    for user in users:
        try:
            await bot.send_message(user[0], msg.text)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            pass
    
    await status.edit_text(f"✅ Sent to {sent} users!")
    await state.clear()

# ================= MAIN =================
async def main():
    logger.info("🚀 Starting bot...")
    
    # Remove old database to fix lock issues
    if os.path.exists(DATABASE_FILE):
        try:
            os.remove(DATABASE_FILE)
            logger.info("Old database removed")
        except:
            pass
    
    # Initialize database
    db = Database(DATABASE_FILE)
    logger.info("Database ready")
    
    # Clear webhook
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Start polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
