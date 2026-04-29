from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
import asyncio
import logging
from datetime import datetime
import json
import os
import re

# ================= CONFIG =================
API_TOKEN = "8777177819:AAHuJtPJR8VmoWSfqHtrHW7WeVNWJ6sbV7o"
WEBSITE_URL = "https://sexstory.lovable.app"
DATA_FILE = "bot_data.json"
ADMIN_IDS = [8459969831]

REFERRAL_BONUS = 10
MINIMUM_WITHDRAWAL = 100

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= JSON FILE DATABASE (NO LOCK ISSUES) =================
class Database:
    def __init__(self, data_file: str):
        self.data_file = data_file
        self._init_data()
    
    def _init_data(self):
        """Initialize data file if not exists"""
        if not os.path.exists(self.data_file):
            data = {
                "users": {},
                "withdrawals": {},
                "next_withdrawal_id": 1
            }
            # Add admins
            for admin_id in ADMIN_IDS:
                data["users"][str(admin_id)] = {
                    "user_id": admin_id,
                    "username": None,
                    "first_name": "Admin",
                    "last_name": "",
                    "joined_date": datetime.now().isoformat(),
                    "balance": 0,
                    "total_earned": 0,
                    "total_withdrawn": 0,
                    "referral_count": 0,
                    "is_admin": True,
                    "is_banned": False
                }
            self._save_data(data)
    
    def _load_data(self):
        """Load data from JSON file"""
        try:
            with open(self.data_file, 'r') as f:
                return json.load(f)
        except:
            return {"users": {}, "withdrawals": {}, "next_withdrawal_id": 1}
    
    def _save_data(self, data):
        """Save data to JSON file"""
        with open(self.data_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = "", referred_by: int = None):
        data = self._load_data()
        user_id_str = str(user_id)
        
        if user_id_str not in data["users"]:
            # Add new user
            data["users"][user_id_str] = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "joined_date": datetime.now().isoformat(),
                "balance": 0,
                "total_earned": 0,
                "total_withdrawn": 0,
                "referral_count": 0,
                "is_admin": user_id in ADMIN_IDS,
                "is_banned": False
            }
            
            # Add referral bonus
            if referred_by and referred_by != user_id and str(referred_by) in data["users"]:
                data["users"][str(referred_by)]["balance"] += REFERRAL_BONUS
                data["users"][str(referred_by)]["total_earned"] += REFERRAL_BONUS
                data["users"][str(referred_by)]["referral_count"] += 1
            
            self._save_data(data)
        return True
    
    def get_balance(self, user_id: int) -> int:
        data = self._load_data()
        user = data["users"].get(str(user_id))
        return user["balance"] if user else 0
    
    def get_user_stats(self, user_id: int):
        data = self._load_data()
        user = data["users"].get(str(user_id))
        if user:
            return {
                'balance': user['balance'],
                'total_earned': user['total_earned'],
                'total_withdrawn': user['total_withdrawn'],
                'referral_count': user['referral_count'],
                'joined_date': user['joined_date'][:10]
            }
        return {'balance': 0, 'total_earned': 0, 'total_withdrawn': 0, 'referral_count': 0, 'joined_date': datetime.now().isoformat()[:10]}
    
    def deduct_balance(self, user_id: int, amount: int) -> bool:
        data = self._load_data()
        user_id_str = str(user_id)
        
        if user_id_str in data["users"] and data["users"][user_id_str]["balance"] >= amount:
            data["users"][user_id_str]["balance"] -= amount
            data["users"][user_id_str]["total_withdrawn"] += amount
            self._save_data(data)
            return True
        return False
    
    def create_withdrawal(self, user_id: int, amount: int, upi_id: str) -> int:
        data = self._load_data()
        wd_id = data["next_withdrawal_id"]
        data["next_withdrawal_id"] += 1
        
        data["withdrawals"][str(wd_id)] = {
            "id": wd_id,
            "user_id": user_id,
            "amount": amount,
            "upi_id": upi_id,
            "status": "pending",
            "request_date": datetime.now().isoformat()
        }
        self._save_data(data)
        return wd_id
    
    def get_pending_withdrawals(self):
        data = self._load_data()
        pending = []
        for wd_id, wd in data["withdrawals"].items():
            if wd["status"] == "pending":
                user = data["users"].get(str(wd["user_id"]), {})
                pending.append({
                    "id": wd["id"],
                    "user_id": wd["user_id"],
                    "amount": wd["amount"],
                    "upi_id": wd["upi_id"],
                    "first_name": user.get("first_name", "Unknown"),
                    "username": user.get("username", ""),
                    "request_date": wd["request_date"]
                })
        return pending
    
    def approve_withdrawal(self, withdraw_id: int):
        data = self._load_data()
        if str(withdraw_id) in data["withdrawals"]:
            data["withdrawals"][str(withdraw_id)]["status"] = "approved"
            self._save_data(data)
    
    def reject_withdrawal(self, withdraw_id: int):
        data = self._load_data()
        if str(withdraw_id) in data["withdrawals"]:
            wd = data["withdrawals"][str(withdraw_id)]
            wd["status"] = "rejected"
            # Refund amount
            if str(wd["user_id"]) in data["users"]:
                data["users"][str(wd["user_id"])]["balance"] += wd["amount"]
            self._save_data(data)
    
    def is_admin(self, user_id: int) -> bool:
        data = self._load_data()
        user = data["users"].get(str(user_id))
        return user.get("is_admin", False) if user else False
    
    def is_banned(self, user_id: int) -> bool:
        data = self._load_data()
        user = data["users"].get(str(user_id))
        return user.get("is_banned", False) if user else False
    
    def get_all_users(self):
        data = self._load_data()
        return [(int(uid), user.get("first_name", "")) for uid, user in data["users"].items() if not user.get("is_banned", False)]

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

# ================= COMMANDS =================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user = message.from_user
    db = Database(DATA_FILE)
    
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
    db = Database(DATA_FILE)
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
    db = Database(DATA_FILE)
    stats = db.get_user_stats(callback.from_user.id)
    
    text = f"""📊 Your Stats

💰 Balance: ₹{stats['balance']}
📈 Earned: ₹{stats['total_earned']}
👥 Referrals: {stats['referral_count']}
📅 Joined: {stats['joined_date']}"""
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "referral")
async def referral_cmd(callback: types.CallbackQuery):
    await callback.answer()
    bot_user = await bot.get_me()
    link = f"https://t.me/{bot_user.username}?start={callback.from_user.id}"
    db = Database(DATA_FILE)
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
    db = Database(DATA_FILE)
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
        db = Database(DATA_FILE)
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
    db = Database(DATA_FILE)
    
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
    db = Database(DATA_FILE)
    balance = db.get_balance(callback.from_user.id)
    await callback.message.edit_text(f"🌟 Main Menu\n\n💰 Balance: ₹{balance}", 
                                     reply_markup=get_main_keyboard(), parse_mode="Markdown")

# ================= ADMIN PANEL =================
@dp.callback_query(lambda c: c.data == "admin_panel")
async def admin_panel(callback: types.CallbackQuery):
    await callback.answer()
    db = Database(DATA_FILE)
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
    db = Database(DATA_FILE)
    if not db.is_admin(callback.from_user.id):
        return
    
    withdrawals = db.get_pending_withdrawals()
    if not withdrawals:
        await callback.message.answer("No pending withdrawals!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for w in withdrawals:
        name = w.get("first_name", f"User_{w['user_id']}")
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"💰 {name[:12]} - ₹{w['amount']}", callback_data=f"process_{w['id']}")
        ])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_panel")])
    
    await callback.message.edit_text(f"💰 Pending ({len(withdrawals)})", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data.startswith("process_"))
async def process_wd(callback: types.CallbackQuery):
    await callback.answer()
    wd_id = int(callback.data.split("_")[1])
    db = Database(DATA_FILE)
    
    withdrawals = db.get_pending_withdrawals()
    w = next((x for x in withdrawals if x["id"] == wd_id), None)
    
    if not w:
        await callback.message.answer("Not found!")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{wd_id}"),
         InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{wd_id}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_wd")]
    ])
    
    text = f"""💰 Withdrawal #{w['id']}

👤 {w['first_name']} @{w['username'] or 'N/A'}
💰 ₹{w['amount']}
📱 {w['upi_id']}
📅 {w['request_date'][:16]}"""
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data.startswith("approve_"))
async def approve_wd(callback: types.CallbackQuery):
    wd_id = int(callback.data.split("_")[1])
    db = Database(DATA_FILE)
    db.approve_withdrawal(wd_id)
    await callback.answer("✅ Approved!", show_alert=True)
    await callback.message.edit_text("✅ Approved!")
    await asyncio.sleep(1)
    await admin_withdrawals(callback)

@dp.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_wd(callback: types.CallbackQuery):
    wd_id = int(callback.data.split("_")[1])
    db = Database(DATA_FILE)
    db.reject_withdrawal(wd_id)
    
    await callback.answer("❌ Rejected!", show_alert=True)
    await callback.message.edit_text("❌ Rejected!")
    await asyncio.sleep(1)
    await admin_withdrawals(callback)

@dp.callback_query(lambda c: c.data == "admin_bc")
async def admin_broadcast_panel(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    db = Database(DATA_FILE)
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
    
    db = Database(DATA_FILE)
    users = db.get_all_users()
    
    sent = 0
    status = await msg.answer("⏳ Broadcasting...")
    
    for user_id, name in users:
        try:
            await bot.send_message(user_id, msg.text)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            pass
    
    await status.edit_text(f"✅ Sent to {sent} users!")
    await state.clear()

# ================= MAIN =================
async def main():
    logger.info("🚀 Starting bot...")
    
    # Clear webhook first
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Initialize database
    db = Database(DATA_FILE)
    logger.info("Bot ready!")
    
    # Start polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
