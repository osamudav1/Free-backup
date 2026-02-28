import os
import json
import asyncio
import re
import shutil
from datetime import datetime, timedelta
from typing import Optional, Dict, List

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputFile, ContentType
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))

COOLDOWN = 90
BATCH_SIZE = 30
AUTO_DELETE_OPTIONS = [5, 10, 30]

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

ACTIVE_USERS = 0
WAITING_QUEUE = asyncio.Queue()
BATCH_LOCK = asyncio.Lock()
USER_PROCESSING_TIME = {}
MOVIES_DICT = {}

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ==================== JSON Functions ====================
def load_json(name):
    path = f"{DATA_DIR}/{name}.json"
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(name, data):
    path = f"{DATA_DIR}/{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# ==================== Movies ====================
async def get_movies():
    return load_json("movies")

async def load_movies_cache():
    global MOVIES_DICT
    movies = await get_movies()
    MOVIES_DICT = {}
    for m in movies:
        if m.get("movie_code"):
            MOVIES_DICT[m["movie_code"].upper()] = m
    print(f"✅ Loaded {len(MOVIES_DICT)} movies to cache")

async def reload_movies_cache():
    await load_movies_cache()

def find_movie_by_code(code: str) -> Optional[dict]:
    return MOVIES_DICT.get(code.upper())

async def add_movie_record(name, code, msgid, chatid):
    movies = load_json("movies")
    movies.append({
        "movie_name": name,
        "movie_code": code.upper(),
        "message_id": msgid,
        "storage_chat_id": chatid
    })
    save_json("movies", movies)
    await reload_movies_cache()

async def delete_movie(code):
    movies = load_json("movies")
    movies = [m for m in movies if m.get("movie_code", "").upper() != code.upper()]
    save_json("movies", movies)
    await reload_movies_cache()

# ==================== Ads ====================
async def get_ads():
    return load_json("ads")

async def add_ad(msgid, chatid):
    ads = load_json("ads")
    ads.append({
        "id": len(ads) + 1,
        "message_id": msgid,
        "storage_chat_id": chatid
    })
    save_json("ads", ads)

async def delete_ad(aid):
    ads = load_json("ads")
    ads = [a for a in ads if a["id"] != int(aid)]
    save_json("ads", ads)

# ==================== Users ====================
async def get_users():
    return load_json("users")

async def add_new_user(uid, name, mention):
    users = load_json("users")
    for u in users:
        if u["user_id"] == uid:
            return False
    
    users.append({
        "user_id": uid,
        "last_search": None,
        "join_date": datetime.now().isoformat(),
        "name": name,
        "mention": mention,
        "search_count": 0
    })
    save_json("users", users)
    return True

async def get_user_count():
    return len(load_json("users"))

async def update_user_search(uid):
    users = load_json("users")
    found = False
    for u in users:
        if u["user_id"] == uid:
            u["last_search"] = datetime.now().isoformat()
            u["search_count"] = u.get("search_count", 0) + 1
            found = True
            break
    if not found:
        users.append({
            "user_id": uid,
            "last_search": datetime.now().isoformat(),
            "join_date": datetime.now().isoformat(),
            "name": "Unknown",
            "mention": "",
            "search_count": 1
        })
    save_json("users", users)

async def get_user_last(uid):
    users = load_json("users")
    for u in users:
        if u["user_id"] == uid:
            return u.get("last_search")
    return None

async def get_top_searches(limit=5):
    users = load_json("users")
    filtered = [u for u in users if u.get("search_count", 0) > 0]
    sorted_users = sorted(filtered, key=lambda x: x.get("search_count", 0), reverse=True)
    return sorted_users[:limit]

async def get_daily_active_users():
    users = load_json("users")
    yesterday = datetime.now() - timedelta(days=1)
    count = 0
    for u in users:
        last = u.get("last_search")
        if last and datetime.fromisoformat(last) >= yesterday:
            count += 1
    return count

# ==================== Settings ====================
async def get_setting(key):
    settings = load_json("settings")
    for s in settings:
        if s["key"] == key:
            return s.get("value")
    return None

async def set_setting(key, value):
    settings = load_json("settings")
    found = False
    for s in settings:
        if s["key"] == key:
            s["value"] = value
            found = True
            break
    if not found:
        settings.append({"key": key, "value": value})
    save_json("settings", settings)

async def get_next_ad_index():
    current = await get_setting("last_ad_index")
    if current is None:
        current = 0
    else:
        try:
            current = int(current)
        except:
            current = 0

    ads = await get_ads()
    if not ads:
        return None

    next_idx = (current + 1) % len(ads)
    await set_setting("last_ad_index", next_idx)
    return current % len(ads)

# ==================== Auto Delete ====================
async def get_auto_delete_config():
    configs = load_json("auto_delete")
    if not configs:
        configs = [
            {"type": "group", "seconds": 0},
            {"type": "dm", "seconds": 0}
        ]
        save_json("auto_delete", configs)
    return configs

async def set_auto_delete_config(config_type, value):
    configs = load_json("auto_delete")
    found = False
    for c in configs:
        if c["type"] == config_type:
            c["seconds"] = value
            found = True
            break
    if not found:
        configs.append({"type": config_type, "seconds": value})
    save_json("auto_delete", configs)

# ==================== Force Channels ====================
async def get_force_channels():
    return load_json("force_channels")

async def add_force_channel(chat_id, title, invite, is_permanent=False):
    channels = load_json("force_channels")
    channels.append({
        "id": len(channels) + 1,
        "chat_id": chat_id,
        "title": title,
        "invite": invite,
        "is_permanent": is_permanent  # Permanent channels cannot be deleted from clones
    })
    save_json("force_channels", channels)

async def delete_force_channel(cid):
    channels = load_json("force_channels")
    channels = [c for c in channels if c["id"] != int(cid)]
    save_json("force_channels", channels)

# ==================== Custom Texts ====================
async def get_custom_text(key):
    texts = load_json("custom_texts")
    for t in texts:
        if t["key"] == key:
            return {
                "text": t.get("text", ""),
                "photo_id": t.get("photo_id"),
                "sticker_id": t.get("sticker_id"),
                "animation_id": t.get("animation_id")
            }
    return {"text": "", "photo_id": None, "sticker_id": None, "animation_id": None}

async def set_custom_text(key, text=None, photo_id=None, sticker_id=None, animation_id=None):
    texts = load_json("custom_texts")
    found = False
    for t in texts:
        if t["key"] == key:
            if text is not None:
                t["text"] = text
            if photo_id:
                t["photo_id"] = photo_id
            if sticker_id:
                t["sticker_id"] = sticker_id
            if animation_id:
                t["animation_id"] = animation_id
            found = True
            break
    if not found:
        texts.append({
            "key": key,
            "text": text or "",
            "photo_id": photo_id,
            "sticker_id": sticker_id,
            "animation_id": animation_id
        })
    save_json("custom_texts", texts)

# ==================== Start Welcome ====================
async def get_start_welcome():
    welcome = load_json("start_welcome")
    if not welcome:
        return [{
            "text": "👋 **Welcome to Movie Bot!**\n\nဇာတ်ကားရှာရန် Code ပို့ပေးပါ။",
            "photo_id": None,
            "caption": ""
        }]
    return welcome

async def get_next_welcome_photo():
    data = await get_start_welcome()
    if not data:
        return None

    current = await get_setting("welcome_photo_index")
    if current is None:
        current = 0
    else:
        try:
            current = int(current)
        except:
            current = 0

    next_idx = (current + 1) % len(data)
    await set_setting("welcome_photo_index", next_idx)

    return data[current % len(data)]

async def add_start_welcome(text=None, photo_id=None, caption=None):
    welcome = load_json("start_welcome")
    welcome.append({
        "id": len(welcome) + 1,
        "text": text or "👋 **Welcome to Movie Bot!**",
        "photo_id": photo_id,
        "caption": caption or ""
    })
    save_json("start_welcome", welcome)

async def delete_start_welcome(index):
    welcome = load_json("start_welcome")
    if 0 <= index < len(welcome):
        welcome.pop(index)
        save_json("start_welcome", welcome)
        return True
    return False

async def get_start_welcome_count():
    return len(load_json("start_welcome"))

# ==================== Start Buttons ====================
async def get_start_buttons():
    return load_json("start_buttons")

async def add_start_button(name, link, row=0, button_type="url", callback_data=None):
    buttons = load_json("start_buttons")
    if row == 0:
        if buttons:
            max_row = max(b.get("row", 0) for b in buttons)
            buttons_in_row = sum(1 for b in buttons if b.get("row") == max_row)
            if buttons_in_row >= 2:
                row = max_row + 1
            else:
                row = max_row
        else:
            row = 0

    buttons.append({
        "id": len(buttons) + 1,
        "name": name,
        "link": link,
        "row": row,
        "type": button_type,
        "callback_data": callback_data
    })
    save_json("start_buttons", buttons)

async def update_start_button(btn_id, name=None, link=None, row=None, button_type=None, callback_data=None):
    buttons = load_json("start_buttons")
    for b in buttons:
        if b["id"] == int(btn_id):
            if name:
                b["name"] = name
            if link:
                b["link"] = link
            if row is not None:
                b["row"] = row
            if button_type:
                b["type"] = button_type
            if callback_data:
                b["callback_data"] = callback_data
            break
    save_json("start_buttons", buttons)

async def delete_start_button(btn_id):
    buttons = load_json("start_buttons")
    buttons = [b for b in buttons if b["id"] != int(btn_id)]
    save_json("start_buttons", buttons)

async def get_start_buttons_by_row():
    buttons = await get_start_buttons()
    rows = {}
    for btn in buttons:
        row = btn.get("row", 0)
        if row not in rows:
            rows[row] = []
        rows[row].append(btn)
    return rows

# ==================== Helper Functions ====================
def parse_telegram_format(text, user_name="", user_mention=""):
    if not text:
        return text

    text = text.replace("{mention}", user_mention)
    text = text.replace("{name}", user_name)

    text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)
    text = re.sub(r'__(.*?)__', r'<u>\1</u>', text)
    text = re.sub(r'~~(.*?)~~', r'<s>\1</s>', text)
    text = re.sub(r'`(.*?)`', r'<code>\1</code>', text)
    text = re.sub(r'```(.*?)```', r'<pre>\1</pre>', text, flags=re.DOTALL)

    return text

auto_delete_tasks: Dict[str, asyncio.Task] = {}

async def schedule_auto_delete(chat_type: str, chat_id: int, message_id: int, seconds: int):
    if seconds <= 0:
        return
    await asyncio.sleep(seconds)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        print(f"Failed to delete message: {e}")

async def batch_worker():
    global ACTIVE_USERS

    while True:
        async with BATCH_LOCK:
            if ACTIVE_USERS >= BATCH_SIZE:
                await asyncio.sleep(0.5)
                continue

            slots = BATCH_SIZE - ACTIVE_USERS
            users_to_process = []

            for _ in range(slots):
                try:
                    user_id = WAITING_QUEUE.get_nowait()
                    users_to_process.append(user_id)
                    ACTIVE_USERS += 1
                except asyncio.QueueEmpty:
                    break

            for user_id in users_to_process:
                asyncio.create_task(process_user_request(user_id))

        await asyncio.sleep(0.1)

async def process_user_request(user_id: int):
    global ACTIVE_USERS

    try:
        await asyncio.sleep(0.1)
    except Exception as e:
        print(f"Error processing user {user_id}: {e}")
    finally:
        async with BATCH_LOCK:
            ACTIVE_USERS -= 1

async def is_maintenance():
    return await get_setting("maint") == "on"

async def check_force_join(user_id, is_clone=False):
    channels = await get_force_channels()
    if not channels:
        return True

    for ch in channels:
        # For clones, always include permanent channels
        if is_clone and ch.get("is_permanent"):
            try:
                m = await bot.get_chat_member(ch["chat_id"], user_id)
                if m.status in ("left", "kicked"):
                    return False
            except:
                return False
        # For main bot, check all channels
        elif not is_clone:
            try:
                m = await bot.get_chat_member(ch["chat_id"], user_id)
                if m.status in ("left", "kicked"):
                    return False
            except:
                return False
    
    return True

async def send_force_join(msg, is_clone=False):
    channels = await get_force_channels()
    if not channels:
        return True

    # Filter channels for clone (only permanent ones)
    if is_clone:
        channels = [ch for ch in channels if ch.get("is_permanent")]

    kb = InlineKeyboardMarkup()
    for ch in channels:
        kb.add(InlineKeyboardButton(ch["title"], url=ch["invite"]))
    
    if is_clone:
        kb.add(InlineKeyboardButton("✅ Done ✅", callback_data="clone_force_done"))
    else:
        kb.add(InlineKeyboardButton("✅ Done ✅", callback_data="force_done"))

    force_text = await get_custom_text("forcemsg")
    formatted_text = parse_telegram_format(
        force_text.get("text") or "⚠️ **BOTအသုံးပြုခွင့် ကန့်သတ်ထားပါသည်။**\n\nBOT ကိုအသုံးပြု နိုင်ရန်အတွက်အောက်ပါ Channel များကို အရင် Join ပေးထားရပါမည်။",
        msg.from_user.full_name,
        msg.from_user.get_mention(as_html=True)
    )

    await msg.answer(formatted_text, reply_markup=kb, protect_content=True)
    return False

async def send_searching_overlay(chat_id: int) -> Optional[int]:
    overlay = await get_custom_text("searching")

    try:
        if overlay.get("sticker_id"):
            msg = await bot.send_sticker(chat_id, overlay["sticker_id"], protect_content=True)
        elif overlay.get("animation_id"):
            msg = await bot.send_animation(chat_id, overlay["animation_id"],
                                         caption=overlay.get("text", ""), protect_content=True)
        elif overlay.get("photo_id"):
            msg = await bot.send_photo(chat_id, overlay["photo_id"],
                                     caption=overlay.get("text", ""), protect_content=True)
        else:
            text = overlay.get("text", "🔍 ရှာဖွေနေပါသည်...")
            msg = await bot.send_message(chat_id, text, protect_content=True)
        return msg.message_id
    except Exception as e:
        print(f"Error sending overlay: {e}")
        try:
            msg = await bot.send_message(chat_id, "🔍 ရှာဖွေနေပါသည်...", protect_content=True)
            return msg.message_id
        except:
            return None

async def safe_delete_message(chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id, message_id)
    except:
        pass

def main_menu(is_owner=False):
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🔍 Search Movie"))
    kb.add(KeyboardButton("📋 Movie List"))
    if is_owner:
        kb.add(KeyboardButton("🛠 Admin Panel"))
        kb.add(KeyboardButton("📊 Statistics"))
    return kb

# ==================== Start Command ====================
@dp.message_handler(commands=["start"])
async def start(msg: types.Message):
    is_owner = msg.from_user.id == OWNER_ID
    user_id = msg.from_user.id
    display_name = msg.from_user.full_name
    user_mention = msg.from_user.get_mention(as_html=True)

    is_new = await add_new_user(user_id, display_name, user_mention)

    if is_new:
        total_users = await get_user_count()

        notification_text = (
            f"👤 <b>New User Notification</b>\n\n"
            f"<b>Total Users:</b> {total_users}\n"
            f"<b>ID:</b> <code>{user_id}</code>\n"
            f"<b>Name:</b> {display_name}\n"
            f"<b>Mention:</b> {user_mention}"
        )
        try:
            await bot.send_message(OWNER_ID, notification_text, protect_content=True)
        except Exception as e:
            print(f"Failed to notify owner: {e}")

    if not await check_force_join(msg.from_user.id):
        await send_force_join(msg)
        return

    await send_start_welcome(msg, is_owner)

    await msg.answer(
        "🔝Main Menu",
        reply_markup=main_menu(is_owner),
        protect_content=True
    )

async def send_start_welcome(msg: types.Message, is_owner: bool):
    welcome_data = await get_next_welcome_photo()

    kb = InlineKeyboardMarkup(row_width=2)
    rows = await get_start_buttons_by_row()

    for row_num in sorted(rows.keys()):
        row_buttons = rows[row_num]
        buttons = []
        for btn in row_buttons[:2]:
            if btn.get("type") == "popup":
                buttons.append(InlineKeyboardButton(btn["name"], callback_data=btn.get("callback_data", f"popup_{btn['id']}")))
            else:
                buttons.append(InlineKeyboardButton(btn["name"], url=btn["link"]))
        if buttons:
            kb.row(*buttons)

    if is_owner:
        kb.add(InlineKeyboardButton("⚙️ Manage Start Buttons", callback_data="manage_start_buttons"))

    welcome_text = parse_telegram_format(
        welcome_data.get("caption") or welcome_data.get("text", "👋 Welcome!"),
        msg.from_user.full_name,
        msg.from_user.get_mention(as_html=True)
    )

    if welcome_data and welcome_data.get("photo_id"):
        try:
            await msg.answer_photo(
                photo=welcome_data["photo_id"],
                caption=welcome_text,
                reply_markup=kb,
                protect_content=True
            )
        except Exception as e:
            print(f"Error sending welcome photo: {e}")
            await msg.answer(
                welcome_text,
                reply_markup=kb,
                protect_content=True
            )
    else:
        await msg.answer(
            welcome_text,
            reply_markup=kb,
            protect_content=True
        )

# ==================== Start Button Management ====================
class StartButtonManagement(StatesGroup):
    waiting_for_name = State()
    waiting_for_link = State()
    waiting_for_type = State()
    waiting_for_popup_text = State()
    waiting_for_edit_id = State()
    waiting_for_edit_name = State()
    waiting_for_edit_link = State()
    waiting_for_edit_row = State()

@dp.callback_query_handler(lambda c: c.data == "manage_start_buttons")
async def manage_start_buttons(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    buttons = await get_start_buttons()
    text = "⚙️ **Start Buttons Management**\n\n"

    if not buttons:
        text += "Buttons မရှိသေးပါ။\n"
    else:
        rows = await get_start_buttons_by_row()
        for row_num in sorted(rows.keys()):
            text += f"\n🔹 Row {row_num + 1}:\n"
            for btn in rows[row_num]:
                btn_type = btn.get("type", "url")
                text += f"   • ID: {btn['id']} | {btn['name']} ({btn_type})\n"

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Add Button", callback_data="add_start_button"),
        InlineKeyboardButton("✏️ Edit Button", callback_data="edit_start_button")
    )
    kb.add(
        InlineKeyboardButton("🗑 Delete Button", callback_data="delete_start_button"),
        InlineKeyboardButton("🖼 Manage Welcome", callback_data="manage_start_welcome")
    )
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data="back_to_start"))

    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "add_start_button")
async def add_start_button_start(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    await StartButtonManagement.waiting_for_name.set()
    await call.message.answer("🔹 Button နာမည်ထည့်ပါ:", protect_content=True)
    await call.answer()

@dp.message_handler(state=StartButtonManagement.waiting_for_name)
async def add_start_button_name(msg: types.Message, state: FSMContext):
    await state.update_data(name=msg.text)
    await StartButtonManagement.waiting_for_type.set()

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔗 URL Button", callback_data="btn_type_url"),
        InlineKeyboardButton("📢 Popup Button", callback_data="btn_type_popup")
    )
    await msg.answer("Button အမျိုးအစားရွေးပါ:", reply_markup=kb, protect_content=True)

@dp.callback_query_handler(lambda c: c.data.startswith("btn_type_"), state=StartButtonManagement.waiting_for_type)
async def add_start_button_type(call: types.CallbackQuery, state: FSMContext):
    btn_type = call.data.split("_")[2]
    await state.update_data(button_type=btn_type)

    if btn_type == "url":
        await StartButtonManagement.waiting_for_link.set()
        await call.message.answer("🔗 Button Link ထည့်ပါ (https://t.me/... or https://...):", protect_content=True)
    else:
        await StartButtonManagement.waiting_for_popup_text.set()
        await call.message.answer("📝 Popup စာသားထည့်ပါ:", protect_content=True)
    await call.answer()

@dp.message_handler(state=StartButtonManagement.waiting_for_link)
async def add_start_button_link(msg: types.Message, state: FSMContext):
    if not msg.text.startswith(('http://', 'https://')):
        return await msg.answer("❌ Link မမှန်ပါ။ http:// သို့မဟုတ် https:// နဲ့စပါ။", protect_content=True)

    data = await state.get_data()
    await add_start_button(data['name'], msg.text, button_type="url")
    await msg.answer(f"✅ Button '{data['name']}' ထည့်ပြီးပါပြီ။", protect_content=True)
    await state.finish()

@dp.message_handler(state=StartButtonManagement.waiting_for_popup_text)
async def add_start_button_popup(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    callback_data = f"popup_{msg.text[:20]}"
    await add_start_button(data['name'], msg.text, button_type="popup", callback_data=callback_data)
    await msg.answer(f"✅ Popup Button '{data['name']}' ထည့်ပြီးပါပြီ။", protect_content=True)
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("popup_"))
async def handle_popup_button(call: types.CallbackQuery):
    buttons = await get_start_buttons()
    for btn in buttons:
        if btn.get("callback_data") == call.data:
            await call.answer(btn.get("link", ""), show_alert=True)
            return
    await call.answer("Popup text not found", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "delete_start_button")
async def delete_start_button_list(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    buttons = await get_start_buttons()
    if not buttons:
        await call.answer("❌ Button မရှိပါ။", show_alert=True)
        return

    kb = InlineKeyboardMarkup(row_width=1)
    for btn in buttons:
        kb.add(InlineKeyboardButton(
            f"🗑 {btn['name']} (Row {btn.get('row', 0)+1})",
            callback_data=f"delstartbtn_{btn['id']}"
        ))
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data="manage_start_buttons"))

    await call.message.edit_text("ဖျက်မည့် Button ကိုရွေးပါ:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("delstartbtn_"))
async def delete_start_button_confirm(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    btn_id = call.data.split("_")[1]
    await delete_start_button(btn_id)
    await call.answer("✅ Button ဖျက်ပြီးပါပြီ။", show_alert=True)
    await manage_start_buttons(call)

# ==================== Welcome Management ====================
class StartWelcomeManagement(StatesGroup):
    waiting_for_photo = State()
    waiting_for_delete_index = State()

@dp.callback_query_handler(lambda c: c.data == "manage_start_welcome")
async def manage_start_welcome(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    welcome_list = await get_start_welcome()
    text = f"🖼 **Start Welcome Management**\n\n"
    text += f"📸 စုစုပေါင်းပုံ: {len(welcome_list)} ပုံ\n\n"

    for i, w in enumerate(welcome_list):
        if w.get("photo_id"):
            text += f"{i+1}. 🖼 Photo - {w.get('caption', 'No caption')[:30]}\n"
        else:
            text += f"{i+1}. 📝 Text - {w.get('text', '')[:30]}\n"

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Add Photo", callback_data="add_welcome_photo"),
        InlineKeyboardButton("➕ Add Text", callback_data="add_welcome_text")
    )
    kb.add(
        InlineKeyboardButton("🗑 Delete", callback_data="delete_welcome_item"),
        InlineKeyboardButton("⬅️ Back", callback_data="manage_start_buttons")
    )

    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "add_welcome_photo")
async def add_welcome_photo_start(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    await StartWelcomeManagement.waiting_for_photo.set()
    await call.message.answer(
        "🖼 Welcome Photo ထည့်ရန် Photo ပို့ပါ။\n"
        "Caption ပါထည့်ချင်ရင် Photo နဲ့အတူ Caption ရေးပို့ပါ။\n\n"
        "📝 Formatting:\n"
        "• **bold text** - စာလုံးမဲအတွက်\n"
        "• *italic text* - စာလုံးစောင်းအတွက်\n"
        "• __underline__ - မျဉ်းသားအတွက်\n"
        "• ~~strikethrough~~ - ကြားမျဉ်းအတွက်\n"
        "• `code` - Code အတွက်\n"
        "• {mention} - User mention အတွက်\n"
        "• {name} - User name အတွက်\n\n"
        "မထည့်ချင်ရင် /cancel ရိုက်ပါ။",
        protect_content=True
    )
    await call.answer()

@dp.message_handler(state=StartWelcomeManagement.waiting_for_photo, content_types=['photo'])
async def add_welcome_photo_done(msg: types.Message, state: FSMContext):
    photo_id = msg.photo[-1].file_id
    caption = msg.caption or ""
    await add_start_welcome(photo_id=photo_id, caption=caption, text=caption)
    count = await get_start_welcome_count()
    await msg.answer(f"✅ Welcome Photo ထည့်ပြီးပါပြီ။\n📸 စုစုပေါင်းပုံ: {count} ပုံ", protect_content=True)
    await state.finish()

@dp.callback_query_handler(lambda c: c.data == "add_welcome_text")
async def add_welcome_text_start(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    await StartWelcomeManagement.waiting_for_photo.set()
    await call.message.answer(
        "📝 Welcome Text ထည့်ရန် စာသားပို့ပါ။\n\n"
        "📝 Formatting:\n"
        "• **bold text** - စာလုံးမဲအတွက်\n"
        "• *italic text* - စာလုံးစောင်းအတွက်\n"
        "• __underline__ - မျဉ်းသားအတွက်\n"
        "• {mention} - User mention အတွက်\n"
        "• {name} - User name အတွက်\n\n"
        "မထည့်ချင်ရင် /cancel ရိုက်ပါ။",
        protect_content=True
    )
    await call.answer()

@dp.message_handler(state=StartWelcomeManagement.waiting_for_photo, content_types=['text'])
async def add_welcome_text_done(msg: types.Message, state: FSMContext):
    if msg.text == '/cancel':
        await msg.answer("❌ Cancelled", protect_content=True)
        await state.finish()
        return

    await add_start_welcome(text=msg.text)
    count = await get_start_welcome_count()
    await msg.answer(f"✅ Welcome Text ထည့်ပြီးပါပြီ။\n📝 စုစုပေါင်း: {count} ခု", protect_content=True)
    await state.finish()

@dp.callback_query_handler(lambda c: c.data == "delete_welcome_item")
async def delete_welcome_item_list(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    welcome_list = await get_start_welcome()
    if not welcome_list:
        await call.answer("❌ ဖျက်စရာမရှိပါ။", show_alert=True)
        return

    kb = InlineKeyboardMarkup(row_width=1)
    for i, w in enumerate(welcome_list):
        if w.get("photo_id"):
            kb.add(InlineKeyboardButton(
                f"🗑 {i+1}. 🖼 Photo - {w.get('caption', 'No caption')[:20]}",
                callback_data=f"delwelcome_{i}"
            ))
        else:
            kb.add(InlineKeyboardButton(
                f"🗑 {i+1}. 📝 Text - {w.get('text', '')[:20]}",
                callback_data=f"delwelcome_{i}"
            ))
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data="manage_start_welcome"))

    await call.message.edit_text("ဖျက်မည့် Welcome Item ကိုရွေးပါ:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("delwelcome_"))
async def delete_welcome_item_confirm(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    index = int(call.data.split("_")[1])
    if await delete_start_welcome(index):
        await call.answer("✅ ဖျက်ပြီးပါပြီ။", show_alert=True)
    else:
        await call.answer("❌ ဖျက်လို့မရပါ။", show_alert=True)

    await manage_start_welcome(call)

# ==================== Admin Menu ====================
def admin_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("➕ Add Movie", callback_data="add_movie"),
           InlineKeyboardButton("🗑 Delete Movie", callback_data="del_movie"))
    kb.add(InlineKeyboardButton("📢 Broadcast", callback_data="broadcast"),
           InlineKeyboardButton("📡 Force Channels", callback_data="force"))
    kb.add(InlineKeyboardButton("📥 Backup", callback_data="backup"),
           InlineKeyboardButton("📤 Restore", callback_data="restore"))
    kb.add(InlineKeyboardButton("🛑 Maintenance", callback_data="maint"),
           InlineKeyboardButton("📺 Ads Manager", callback_data="ads_manager"))
    kb.add(InlineKeyboardButton("⏰ Auto Delete", callback_data="auto_delete"),
           InlineKeyboardButton("🗑 Clear All Data", callback_data="clear_all_data"))
    kb.add(InlineKeyboardButton("📝 Welcome Set", callback_data="edit_welcome"),
           InlineKeyboardButton("📢 Force Msg Set", callback_data="edit_forcemsg"))
    kb.add(InlineKeyboardButton("🔍 Searching Set", callback_data="edit_searching"),
           InlineKeyboardButton("⚙️ Start Buttons", callback_data="manage_start_buttons"))
    kb.add(InlineKeyboardButton("🤖 Clone Bot", callback_data="clone_bot_menu"))
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="back"))
    return kb

# ==================== Ads Management ====================
class AddAd(StatesGroup):
    msgid = State()
    chatid = State()

@dp.callback_query_handler(lambda c: c.data == "ads_manager")
async def ads_manager(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    ads = await get_ads()
    text = "📺 Ads Manager:\n\n"
    if not ads:
        text += "No ads added yet."
    else:
        for a in ads:
            text += f"ID: {a['id']} | MsgID: {a['message_id']} | ChatID: {a['storage_chat_id']}\n"

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("➕ Add Ad", callback_data="add_ad"))
    for a in ads:
        kb.add(InlineKeyboardButton(f"🗑 Delete Ad {a['id']}", callback_data=f"delad_{a['id']}"))
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="back_admin"))

    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "add_ad")
async def add_ad_start(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    await AddAd.msgid.set()
    await call.message.answer("Enter Ad Message ID:", protect_content=True)
    await call.answer()

@dp.message_handler(state=AddAd.msgid)
async def add_ad_msgid(msg: types.Message, state: FSMContext):
    if not msg.text.isdigit():
        return await msg.answer("Please enter a numeric Message ID.", protect_content=True)
    await state.update_data(msgid=int(msg.text))
    await AddAd.chatid.set()
    await msg.answer("Enter Storage Group Chat ID for this Ad:", protect_content=True)

@dp.message_handler(state=AddAd.chatid)
async def add_ad_chatid(msg: types.Message, state: FSMContext):
    try:
        chatid = int(msg.text)
    except:
        return await msg.answer("Invalid Chat ID.", protect_content=True)

    data = await state.get_data()
    await add_ad(data["msgid"], chatid)
    await msg.answer("✅ Ad added successfully!", protect_content=True)
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("delad_"))
async def del_ad_process(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    aid = call.data.split("_")[1]
    await delete_ad(aid)
    await call.answer("✅ Ad deleted", show_alert=True)
    await ads_manager(call)

# ==================== Admin Panel ====================
@dp.message_handler(lambda m: m.text == "🛠 Admin Panel")
async def admin_panel(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    await msg.answer("🛠 Admin Panel", reply_markup=admin_menu(), protect_content=True)

@dp.message_handler(lambda m: m.text == "📊 Statistics")
async def statistics_panel(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return

    total_users = await get_user_count()
    daily_active = await get_daily_active_users()
    top_users = await get_top_searches(5)
    total_movies = len(MOVIES_DICT)

    text = "📊 **Bot Statistics**\n\n"
    text += f"👥 Total Users: **{total_users}**\n"
    text += f"🟢 Daily Active: **{daily_active}**\n"
    text += f"🎬 Total Movies: **{total_movies}**\n\n"

    text += "🔝 **Top 5 Searchers:**\n"
    for i, user in enumerate(top_users, 1):
        name = user.get("name", "Unknown")
        count = user.get("search_count", 0)
        text += f"{i}. {name} - {count} searches\n"

    await msg.answer(text, protect_content=True)

# ==================== Navigation ====================
@dp.callback_query_handler(lambda c: c.data == "back")
async def back(call: types.CallbackQuery):
    await call.message.delete()
    await call.message.answer("Menu:", reply_markup=main_menu(call.from_user.id == OWNER_ID), protect_content=True)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_start")
async def back_to_start(call: types.CallbackQuery):
    await call.message.delete()
    await send_start_welcome(call.message, call.from_user.id == OWNER_ID)

@dp.callback_query_handler(lambda c: c.data == "back_admin")
async def back_admin(call: types.CallbackQuery):
    await call.message.edit_text("🛠 Admin Panel", reply_markup=admin_menu())

# ==================== Auto Delete ====================
@dp.callback_query_handler(lambda c: c.data == "auto_delete")
async def auto_delete_menu(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    config = await get_auto_delete_config()
    group_sec = next((c["seconds"] for c in config if c["type"] == "group"), 0)
    dm_sec = next((c["seconds"] for c in config if c["type"] == "dm"), 0)

    text = f"🕒 Auto Delete Settings:\n\n"
    text += f"Group Messages: {group_sec} seconds\n"
    text += f"DM Messages: {dm_sec} seconds\n\n"
    text += "Select option to change:"

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("👥 Group", callback_data="set_group_delete"),
           InlineKeyboardButton("💬 DM", callback_data="set_dm_delete"))
    kb.add(InlineKeyboardButton("❌ Disable All", callback_data="disable_auto_delete"))
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="back_admin"))

    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("set_") and "delete" in c.data)
async def set_auto_delete_type(call: types.CallbackQuery):
    delete_type = "group" if "group" in call.data else "dm"

    kb = InlineKeyboardMarkup(row_width=3)
    for sec in AUTO_DELETE_OPTIONS:
        kb.insert(InlineKeyboardButton(f"{sec}s", callback_data=f"set_time_{delete_type}_{sec}"))
    kb.add(InlineKeyboardButton("❌ Disable", callback_data=f"set_time_{delete_type}_0"))
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="auto_delete"))

    await call.message.edit_text(f"Select auto-delete time for {delete_type.upper()}:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("set_time_"))
async def confirm_auto_delete(call: types.CallbackQuery):
    parts = call.data.split("_")
    delete_type = parts[2]
    seconds = int(parts[3])

    await set_auto_delete_config(delete_type, seconds)

    if seconds > 0:
        await call.answer(f"{delete_type.upper()} auto-delete set to {seconds} seconds!", show_alert=True)
    else:
        await call.answer(f"{delete_type.upper()} auto-delete disabled!", show_alert=True)

    await auto_delete_menu(call)

@dp.callback_query_handler(lambda c: c.data == "disable_auto_delete")
async def disable_all_auto_delete(call: types.CallbackQuery):
    await set_auto_delete_config("group", 0)
    await set_auto_delete_config("dm", 0)
    await call.answer("All auto-delete disabled!", show_alert=True)
    await auto_delete_menu(call)

# ==================== Clear All Data ====================
@dp.callback_query_handler(lambda c: c.data == "clear_all_data")
async def clear_all_data_confirm(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Confirm Clear All", callback_data="confirm_clear_all"))
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="back_admin"))
    await call.message.edit_text("⚠️ <b>Are you sure you want to delete ALL data?</b>\nThis includes movies, users, ads, and settings.", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "confirm_clear_all")
async def process_clear_all_data(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    # Clear all JSON files
    save_json("movies", [])
    save_json("users", [])
    save_json("ads", [])
    save_json("settings", [])
    save_json("force_channels", [])
    save_json("custom_texts", [])
    save_json("auto_delete", [])
    save_json("start_buttons", [])
    save_json("start_welcome", [])

    await reload_movies_cache()

    await call.message.edit_text("✅ All data has been cleared!", reply_markup=admin_menu())
    await call.answer("Data cleared", show_alert=True)

# ==================== Force Channels ====================
@dp.callback_query_handler(lambda c: c.data == "force")
async def force(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    channels = await get_force_channels()
    text = "📡 Force Channels:\n\n"

    if not channels:
        text += "No force channels added yet."
    else:
        for ch in channels:
            perm = "🔒 Permanent" if ch.get("is_permanent") else "📌 Normal"
            text += f"{ch['id']}. {ch['title']} ({perm})\n"

    kb = InlineKeyboardMarkup(row_width=1)

    for ch in channels:
        kb.add(InlineKeyboardButton(f"❌ {ch['title']}", callback_data=f"delch_{ch['id']}"))

    kb.add(InlineKeyboardButton("➕ Add Channel", callback_data="add_force"))
    kb.add(InlineKeyboardButton("⭐ Add Permanent Channel", callback_data="add_permanent_force"))
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="back_admin"))

    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "add_force")
async def add_force(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    await call.message.answer(
        "📌 Channel link ပေးပါ (public/private OK)\n\n"
        "Example:\nhttps://t.me/yourchannel\nhttps://t.me/+AbCdEfGhIjKlMn==",
        protect_content=True
    )

@dp.callback_query_handler(lambda c: c.data == "add_permanent_force")
async def add_permanent_force(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    await call.message.answer(
        "🔒 **Permanent Channel ထည့်ရန်**\n\n"
        "ဒီ Channel ကို Clone Bot တွေမှာ ဖျက်လို့မရပါ။\n\n"
        "Channel link ပေးပါ:",
        protect_content=True
    )

@dp.message_handler(lambda m: m.text and m.text.startswith("https://t.me/"))
async def catch_force_link(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return

    link = msg.text.strip()
    chat_id = None
    chat = None
    is_permanent = False

    # Check if previous message was asking for permanent channel
    context = await dp.current_state().get_data()
    if context.get("adding_permanent"):
        is_permanent = True
        await dp.current_state().reset_state()

    if "+" not in link:
        username = link.split("t.me/")[1].replace("@", "").strip("/")
        try:
            chat = await bot.get_chat(f"@{username}")
            chat_id = chat.id
        except:
            return await msg.answer("❌ Public channel not found", protect_content=True)
    else:
        try:
            chat = await bot.get_chat(link)
            chat_id = chat.id
        except:
            return await msg.answer("❌ Private channel invalid", protect_content=True)

    try:
        bot_member = await bot.get_chat_member(chat_id, (await bot.get_me()).id)
        if bot_member.status not in ("administrator", "creator"):
            return await msg.answer("❌ Bot must be admin in channel", protect_content=True)
    except:
        return await msg.answer("❌ Cannot check admin status", protect_content=True)

    try:
        invite = await bot.export_chat_invite_link(chat_id)
    except:
        if chat.username:
            invite = f"https://t.me/{chat.username}"
        else:
            return await msg.answer("❌ Cannot create invite link", protect_content=True)

    await add_force_channel(chat_id, chat.title, invite, is_permanent)

    perm_text = "🔒 Permanent" if is_permanent else "📌 Normal"
    await msg.answer(f"✅ Added: {chat.title} ({perm_text})", protect_content=True)

@dp.callback_query_handler(lambda c: c.data.startswith("delch_"))
async def delch(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    cid = call.data.split("_")[1]
    await delete_force_channel(cid)
    await call.answer("✅ Deleted", show_alert=True)

    await force(call)

# ==================== Force Done ====================
@dp.callback_query_handler(lambda c: c.data == "force_done")
async def force_done(call: types.CallbackQuery):
    ok = await check_force_join(call.from_user.id)

    if not ok:
        await call.answer(
            "❌ Channel အားလုံးကို Join မလုပ်ရသေးပါ။\n"
            "ကျေးဇူးပြု၍ သတ်မှတ်ထားသော Channel များအားလုံးကို အရင် Join လုပ်ပါ။\n"
            "ပြီးရင် 'Done' ကို နှိပ်ပါ။",
            show_alert=True
        )
        return

    await call.answer("joinပေးတဲ့အတွက်ကျေးဇူးတင်ပါတယ်!", show_alert=True)
    await call.message.delete()
    await send_start_welcome(call.message, call.from_user.id == OWNER_ID)

# ==================== Edit Text ====================
class EditText(StatesGroup):
    waiting = State()

@dp.callback_query_handler(lambda c: c.data.startswith("edit_"))
async def edit_text_start(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    key = call.data.replace("edit_", "")
    await EditText.waiting.set()
    state = dp.current_state(user=call.from_user.id)
    await state.update_data(key=key)

    formatting_guide = (
        "\n\n📝 Formatting Guide:\n"
        "• **bold text** - စာလုံးမဲ\n"
        "• *italic text* - စာလုံးစောင်း\n"
        "• __underline__ - မျဉ်းသား\n"
        "• ~~strikethrough~~ - ကြားမျဉ်း\n"
        "• `code` - Code\n"
        "• {mention} - User mention\n"
        "• {name} - User name\n"
    )

    if key == "searching":
        await call.message.answer(
            "🔍 Searching overlay အတွက် content ပို့ပေးပါ:\n\n"
            "• Text message ပို့ရင် - စာသားအဖြစ်သိမ်းမယ်\n"
            "• Photo ပို့ရင် - Photo နဲ့ caption သိမ်းမယ်\n"
            "• Sticker ပို့ရင် - Sticker အဖြစ်သိမ်းမယ်\n"
            "• GIF/Animation ပို့ရင် - GIF အဖြစ်သိမ်းမယ်\n" +
            formatting_guide +
            "\nမပို့ချင်ရင် /cancel ရိုက်ပါ။",
            protect_content=True
        )
    else:
        await call.message.answer(
            f"'{key}' အတွက် စာအသစ်ပို့ပေးပါ (Photo ပါရင် Photo နဲ့အတူ Caption ထည့်ပေးပါ)" +
            formatting_guide,
            protect_content=True
        )

    await call.answer()

@dp.message_handler(state=EditText.waiting, content_types=types.ContentTypes.ANY)
async def edit_text_done(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    key = data['key']

    if msg.content_type == 'text' and msg.text == '/cancel':
        await msg.answer("❌ Cancelled", protect_content=True)
        await state.finish()
        return

    if msg.content_type == 'text':
        await set_custom_text(key, text=msg.text)
        await msg.answer(f"✅ {key} text updated successfully", protect_content=True)

    elif msg.content_type == 'photo':
        photo_id = msg.photo[-1].file_id
        caption = msg.caption or ""
        await set_custom_text(key, text=caption, photo_id=photo_id)
        await msg.answer(f"✅ {key} photo updated successfully", protect_content=True)

    elif msg.content_type == 'sticker':
        sticker_id = msg.sticker.file_id
        await set_custom_text(key, sticker_id=sticker_id)
        await msg.answer(f"✅ {key} sticker updated successfully", protect_content=True)

    elif msg.content_type == 'animation':
        animation_id = msg.animation.file_id
        caption = msg.caption or ""
        await set_custom_text(key, text=caption, animation_id=animation_id)
        await msg.answer(f"✅ {key} GIF updated successfully", protect_content=True)

    else:
        await msg.answer("❌ Unsupported content type", protect_content=True)

    await state.finish()

# ==================== Movie List ====================
@dp.message_handler(lambda m: m.text == " Movie CodeList")
async def movie_list_redirect(msg: types.Message):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(" Movie Code များကြည့်ရန်", url="https://t.me/Movie462"))
    await msg.answer("Code များကြည့်ရန် အောက်ပါ Button ကိုနှိပ်ပါ", reply_markup=kb, protect_content=True)

# ==================== Maintenance ====================
@dp.callback_query_handler(lambda c: c.data == "maint")
async def maint(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    current = await is_maintenance()
    new = "off" if current else "on"
    await set_setting("maint", new)
    await call.answer(f"Maintenance: {new.upper()}", show_alert=True)

# ==================== Add Movie ====================
class AddMovie(StatesGroup):
    name = State()
    code = State()
    msgid = State()
    chatid = State()

@dp.callback_query_handler(lambda c: c.data == "add_movie")
async def add_movie(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    await AddMovie.name.set()
    await call.message.answer("🎬 ဇာတ်ကားနာမည်?", protect_content=True)
    await call.answer()

@dp.message_handler(state=AddMovie.name)
async def add_movie_name(msg: types.Message, state: FSMContext):
    await state.update_data(name=msg.text)
    await AddMovie.code.set()
    await msg.answer("🔢 ဇာတ်ကား Code (ဥပမာ: 101010, MM101, etc):", protect_content=True)

@dp.message_handler(state=AddMovie.code)
async def add_movie_code(msg: types.Message, state: FSMContext):
    code = msg.text.strip().upper()
    if not code:
        return await msg.answer("❌ Code ထည့်ပါ။", protect_content=True)
    await state.update_data(code=code)
    await AddMovie.msgid.set()
    await msg.answer("📨 Message ID?", protect_content=True)

@dp.message_handler(state=AddMovie.msgid)
async def add_movie_msgid(msg: types.Message, state: FSMContext):
    if not msg.text.isdigit():
        return await msg.answer("❌ ဂဏန်းပဲထည့်ပါ။", protect_content=True)
    await state.update_data(msgid=int(msg.text))
    await AddMovie.chatid.set()
    await msg.answer("💬 Storage Group Chat ID?", protect_content=True)

@dp.message_handler(state=AddMovie.chatid)
async def add_movie_chatid(msg: types.Message, state: FSMContext):
    try:
        chatid = int(msg.text)
    except:
        return await msg.answer("❌ Chat ID မမှန်ပါ။", protect_content=True)

    data = await state.get_data()
    await add_movie_record(data["name"], data["code"], data["msgid"], chatid)

    await msg.answer(f"✅ ဇာတ်ကားထည့်ပြီးပါပြီ!\n\nနာမည်: {data['name']}\nCode: {data['code']}", protect_content=True)
    await state.finish()

# ==================== Delete Movie ====================
class DelMovie(StatesGroup):
    code = State()

@dp.callback_query_handler(lambda c: c.data == "del_movie")
async def del_movie(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    await DelMovie.code.set()
    await call.message.answer("🗑 ဖျက်မည့် ဇာတ်ကား Code ကိုထည့်ပါ:", protect_content=True)
    await call.answer()

@dp.message_handler(state=DelMovie.code)
async def del_movie_code(msg: types.Message, state: FSMContext):
    code = msg.text.strip().upper()
    await delete_movie(code)
    await msg.answer(f"✅ Code `{code}` ဖျက်ပြီးပါပြီ။", protect_content=True)
    await state.finish()

# ==================== Broadcast ====================
class Broadcast(StatesGroup):
    waiting_content = State()
    waiting_buttons = State()
    confirm = State()

@dp.callback_query_handler(lambda c: c.data == "broadcast")
async def bc(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    await Broadcast.waiting_content.set()
    await call.message.answer(
        "📢 Broadcast စာသား/ပုံ ပို့ပါ။\n\n"
        "📝 Formatting supported:\n"
        "• **bold**, *italic*, __underline__\n"
        "• {mention}, {name} - placeholders\n\n"
        "Photo/Video/GIF ပါ ပို့လို့ရပါတယ်။",
        protect_content=True
    )
    await call.answer()

@dp.message_handler(state=Broadcast.waiting_content, content_types=types.ContentTypes.ANY)
async def bc_content(msg: types.Message, state: FSMContext):
    content_type = msg.content_type

    if content_type == "text":
        await state.update_data(text=msg.text, content_type="text")
    elif content_type == "photo":
        photo_id = msg.photo[-1].file_id
        caption = msg.caption or ""
        await state.update_data(photo_id=photo_id, caption=caption, content_type="photo")
    elif content_type == "video":
        video_id = msg.video.file_id
        caption = msg.caption or ""
        await state.update_data(video_id=video_id, caption=caption, content_type="video")
    elif content_type == "animation":
        animation_id = msg.animation.file_id
        caption = msg.caption or ""
        await state.update_data(animation_id=animation_id, caption=caption, content_type="animation")
    else:
        return await msg.answer("❌ Unsupported content type", protect_content=True)

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ ပြန်ဖြစ်ရင်ပဲပို့မယ်", callback_data="bc_no_buttons"))
    kb.add(InlineKeyboardButton("➕ Buttons ထည့်မယ်", callback_data="bc_add_buttons"))

    await msg.answer("Buttons ထည့်မလား?", reply_markup=kb, protect_content=True)

@dp.callback_query_handler(lambda c: c.data == "bc_no_buttons", state=Broadcast.waiting_content)
async def bc_no_buttons(call: types.CallbackQuery, state: FSMContext):
    await state.update_data(buttons=[])
    await confirm_broadcast(call, state)

@dp.callback_query_handler(lambda c: c.data == "bc_add_buttons", state=Broadcast.waiting_content)
async def bc_add_buttons_start(call: types.CallbackQuery, state: FSMContext):
    await Broadcast.waiting_buttons.set()
    await call.message.answer(
        "📝 Buttons ထည့်ရန်:\n\n"
        "Format: Button Name | URL\n"
        "Example:\n"
        "Channel | https://t.me/yourchannel\n"
        "Group | https://t.me/yourgroup\n\n"
        "တစ်ကြောင်းကို button တစ်ခု၊ ပြီးရင် ပို့ပါ။\n"
        "ပြီးသွားရင် /done ရိုက်ပါ။",
        protect_content=True
    )
    await call.answer()

@dp.message_handler(state=Broadcast.waiting_buttons)
async def bc_buttons_collect(msg: types.Message, state: FSMContext):
    if msg.text == "/done":
        data = await state.get_data()
        if not data.get("buttons"):
            await state.update_data(buttons=[])
        await Broadcast.confirm.set()
        await confirm_broadcast_message(msg, state)
        return

    if "|" not in msg.text:
        return await msg.answer("❌ Format မမှန်ပါ။ Button Name | URL အဖြစ်ထည့်ပါ။", protect_content=True)

    parts = msg.text.split("|")
    if len(parts) != 2:
        return await msg.answer("❌ Format မမှန်ပါ။", protect_content=True)

    name = parts[0].strip()
    url = parts[1].strip()

    if not url.startswith(("http://", "https://")):
        return await msg.answer("❌ URL မမှန်ပါ။", protect_content=True)

    data = await state.get_data()
    buttons = data.get("buttons", [])
    buttons.append({"name": name, "url": url})
    await state.update_data(buttons=buttons)

    await msg.answer(f"✅ Button '{name}' ထည့်ပြီး။\nထပ်ထည့်မယ်ဆိုရင် ဆက်ပို့ပါ။\nပြီးရင် /done ရိုက်ပါ။", protect_content=True)

async def confirm_broadcast(call: types.CallbackQuery, state: FSMContext):
    await Broadcast.confirm.set()

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Confirm & Send", callback_data="bc_confirm"))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data="bc_cancel"))

    await call.message.answer("📢 Broadcast ပို့မှာသေချာပြီလား?", reply_markup=kb, protect_content=True)

async def confirm_broadcast_message(msg: types.Message, state: FSMContext):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Confirm & Send", callback_data="bc_confirm"))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data="bc_cancel"))

    await msg.answer("📢 Broadcast ပို့မှာသေချာပြီလား?", reply_markup=kb, protect_content=True)

@dp.callback_query_handler(lambda c: c.data == "bc_confirm", state=Broadcast.confirm)
async def bc_confirm(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    # Get main bot users
    users = await get_users()
    
    # Get all clone bots users
    clone_bots = await get_clone_bots()
    all_clone_users = []
    
    for bot in clone_bots:
        if bot.get("status") == "active":
            token = bot["token"]
            clone_users = load_json(f"clone_users_{token.replace(':', '_')}")
            all_clone_users.extend(clone_users)

    buttons = data.get("buttons", [])
    kb = None
    if buttons:
        kb = InlineKeyboardMarkup(row_width=1)
        for btn in buttons:
            kb.add(InlineKeyboardButton(btn["name"], url=btn["url"]))

    total = len(users) + len(all_clone_users)
    sent = 0
    failed = 0
    clone_sent = 0
    clone_failed = 0

    status_msg = await call.message.answer(f"📢 Broadcasting... 0/{total}", protect_content=True)

    # Send to main bot users
    for i, u in enumerate(users):
        try:
            if data["content_type"] == "text":
                await bot.send_message(u["user_id"], data["text"], reply_markup=kb, protect_content=True)
            elif data["content_type"] == "photo":
                await bot.send_photo(u["user_id"], data["photo_id"], caption=data.get("caption"), reply_markup=kb, protect_content=True)
            elif data["content_type"] == "video":
                await bot.send_video(u["user_id"], data["video_id"], caption=data.get("caption"), reply_markup=kb, protect_content=True)
            elif data["content_type"] == "animation":
                await bot.send_animation(u["user_id"], data["animation_id"], caption=data.get("caption"), reply_markup=kb, protect_content=True)
            sent += 1
        except:
            failed += 1

        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(f"📢 Broadcasting... {i+1}/{total}")
            except:
                pass

    # Send to clone bot users (with rate limiting: 20 users/sec)
    for i, user in enumerate(all_clone_users):
        try:
            if data["content_type"] == "text":
                await bot.send_message(user["user_id"], data["text"], reply_markup=kb, protect_content=True)
            elif data["content_type"] == "photo":
                await bot.send_photo(user["user_id"], data["photo_id"], caption=data.get("caption"), reply_markup=kb, protect_content=True)
            elif data["content_type"] == "video":
                await bot.send_video(user["user_id"], data["video_id"], caption=data.get("caption"), reply_markup=kb, protect_content=True)
            elif data["content_type"] == "animation":
                await bot.send_animation(user["user_id"], data["animation_id"], caption=data.get("caption"), reply_markup=kb, protect_content=True)
            clone_sent += 1
        except:
            clone_failed += 1

        # Rate limit: 20 users per second
        if (i + 1) % 20 == 0:
            await asyncio.sleep(1)

    await status_msg.edit_text(
        f"✅ Broadcast complete!\n\n"
        f"📊 **Main Bot:**\n"
        f"✅ Sent: {sent}\n❌ Failed: {failed}\n\n"
        f"📊 **Clone Bots:**\n"
        f"✅ Sent: {clone_sent}\n❌ Failed: {clone_failed}"
    )
    await state.finish()
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "bc_cancel", state="*")
async def bc_cancel(call: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await call.message.answer("❌ Broadcast cancelled", protect_content=True)
    await call.answer()

# ==================== OS Command ====================
@dp.message_handler(commands=["os"])
async def os_command(msg: types.Message):
    if msg.chat.type not in ["group", "supergroup"]:
        await msg.answer("This command can only be used in groups!", protect_content=True)
        return

    config = await get_auto_delete_config()
    group_sec = next((c["seconds"] for c in config if c["type"] == "group"), 0)

    response = await msg.reply(
        "**owner-@osamu1123**\n\n"
        "• Bot Status: ✅ Online\n"
        "• Queue System: 🟢 Active (Batch: 30)\n"
        "• Auto-Delete: " + ("✅ " + str(group_sec) + "s" if group_sec > 0 else "❌ Disabled") + "\n"
        "• Version: 5.0 (Clone System Enabled)\n\n"
        "Use /os name command.",
        protect_content=True
    )

    if group_sec > 0:
        asyncio.create_task(schedule_auto_delete("group", msg.chat.id, response.message_id, group_sec))
        asyncio.create_task(schedule_auto_delete("group", msg.chat.id, msg.message_id, group_sec))

# ==================== Search ====================
@dp.message_handler()
async def search(msg: types.Message):
    if msg.text == "🔍 Search Movie":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Movie Code များကြည့်ရန်", url="https://t.me/Movie462"))
        return await msg.answer("🔍 <b>ဇာတ်ကား Code ပို့ပေးပါ</b>", reply_markup=kb, protect_content=True)

    if msg.text.startswith("/"):
        return

    if await is_maintenance() and msg.from_user.id != OWNER_ID:
        return await msg.answer("🛠 Bot ပြုပြင်နေပါသဖြင့် ခေတ္တပိတ်ထားပါသည်။", protect_content=True)

    if not await check_force_join(msg.from_user.id):
        sent = await send_force_join(msg)
        if sent is False:
            return

    if msg.from_user.id != OWNER_ID:
        last = await get_user_last(msg.from_user.id)
        if last:
            diff = datetime.now() - datetime.fromisoformat(last)
            if diff.total_seconds() < COOLDOWN:
                remain = int(COOLDOWN - diff.total_seconds())
                return await msg.answer(f"⏳ ခေတ္တစောင့်ပေးပါ {remain} စက္ကန့်", protect_content=True)

    code = msg.text.strip().upper()
    movie = find_movie_by_code(code)

    if not movie:
        return await msg.answer(f"❌ Code `{code}` မရှိပါ။\n\n🔍 Search Movie နှိပ်ပြီး Code စစ်ပါ။", protect_content=True)

    global ACTIVE_USERS

    async with BATCH_LOCK:
        if ACTIVE_USERS >= BATCH_SIZE:
            await WAITING_QUEUE.put(msg.from_user.id)
            position = WAITING_QUEUE.qsize()

            queue_msg = await msg.answer(
                f"⏳ **စောင့်ဆိုင်းနေဆဲအသုံးပြုသူများ**\n\n"
                f"• သင့်နေရာ: **{position}**\n"
                f"• လက်ရှိအသုံးပြုနေသူ: **{ACTIVE_USERS}/{BATCH_SIZE}**\n\n"
                f"ကျေးဇူးပြု၍ စောင့်ဆိုင်းပေးပါ။",
                protect_content=True
            )

            await asyncio.sleep(5)
            await safe_delete_message(msg.chat.id, queue_msg.message_id)
            return

        ACTIVE_USERS += 1

    try:
        await update_user_search(msg.from_user.id)
        USER_PROCESSING_TIME[msg.from_user.id] = datetime.now()

        ads = await get_ads()
        if ads:
            idx = await get_next_ad_index()
            if idx is not None and idx < len(ads):
                ad = ads[idx]
                try:
                    ad_sent = await bot.copy_message(
                        chat_id=msg.from_user.id,
                        from_chat_id=ad["storage_chat_id"],
                        message_id=ad["message_id"],
                        protect_content=True
                    )
                    asyncio.create_task(schedule_auto_delete("dm", msg.from_user.id, ad_sent.message_id, 10))
                    await asyncio.sleep(10)
                except Exception as e:
                    print(f"Error sending ad: {e}")

        searching_msg_id = await send_searching_overlay(msg.from_user.id)

        sent = await bot.copy_message(
            chat_id=msg.from_user.id,
            from_chat_id=movie["storage_chat_id"],
            message_id=movie["message_id"],
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("⚜️Owner⚜️", url="https://t.me/osamu1123")
            ),
            protect_content=True
        )

        if searching_msg_id:
            await safe_delete_message(msg.from_user.id, searching_msg_id)

        config = await get_auto_delete_config()
        dm_sec = next((c["seconds"] for c in config if c["type"] == "dm"), 0)
        if dm_sec > 0:
            asyncio.create_task(schedule_auto_delete("dm", msg.from_user.id, sent.message_id, dm_sec))

    except Exception as e:
        print(f"Error sending movie: {e}")
        await msg.answer("❌ Error sending movie. Please try again.", protect_content=True)
    finally:
        async with BATCH_LOCK:
            ACTIVE_USERS -= 1

# ==================== Backup ====================
@dp.callback_query_handler(lambda c: c.data == "backup")
async def backup_db(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return

    # Collect main bot data
    data = {
        "movies": await get_movies(),
        "users": await get_users(),
        "settings": load_json("settings"),
        "force_channels": await get_force_channels(),
        "auto_delete": await get_auto_delete_config(),
        "custom_texts": load_json("custom_texts"),
        "start_buttons": await get_start_buttons(),
        "start_welcome": await get_start_welcome(),
        "ads": await get_ads(),
        "clone_bots": await get_clone_bots()
    }
    
    # Collect all clone bots data
    clone_bots = await get_clone_bots()
    clone_data = {}
    
    for bot in clone_bots:
        token = bot["token"]
        clone_data[token] = {
            "users": load_json(f"clone_users_{token.replace(':', '_')}"),
            "info": bot
        }
    
    data["clone_bots_data"] = clone_data

    # Save backup file
    backup_file = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    await bot.send_document(
        OWNER_ID,
        InputFile(backup_file),
        caption="📥 Complete Backup (Main + Clone Bots)",
        protect_content=False
    )
    
    os.remove(backup_file)
    await call.answer("Backup sent!", show_alert=True)

# ==================== Restore ====================
@dp.callback_query_handler(lambda c: c.data == "restore")
async def restore_request(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    await call.message.answer("📤 Upload backup.json file", protect_content=True)
    await call.answer()

@dp.message_handler(content_types=types.ContentTypes.DOCUMENT)
async def restore_process(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return

    try:
        file = await msg.document.download(destination_file="restore.json")

        with open("restore.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        # Restore main bot data
        if data.get("movies"):
            save_json("movies", data["movies"])
        if data.get("users"):
            save_json("users", data["users"])
        if data.get("settings"):
            save_json("settings", data["settings"])
        if data.get("force_channels"):
            save_json("force_channels", data["force_channels"])
        if data.get("auto_delete"):
            save_json("auto_delete", data["auto_delete"])
        if data.get("custom_texts"):
            save_json("custom_texts", data["custom_texts"])
        if data.get("start_buttons"):
            save_json("start_buttons", data["start_buttons"])
        if data.get("start_welcome"):
            save_json("start_welcome", data["start_welcome"])
        if data.get("ads"):
            save_json("ads", data["ads"])
        if data.get("clone_bots"):
            save_json("clone_bots", data["clone_bots"])

        # Restore clone bots data
        if data.get("clone_bots_data"):
            for token, bot_data in data["clone_bots_data"].items():
                if bot_data.get("users"):
                    save_json(f"clone_users_{token.replace(':', '_')}", bot_data["users"])

        await reload_movies_cache()
        await msg.answer("✅ Restore Completed! Restarting clone bots...", protect_content=True)
        
        # Restart clone bots
        await load_clone_bots_on_startup()
        
    except Exception as e:
        await msg.answer(f"❌ Restore Failed: {str(e)}", protect_content=True)

# ==================== Group Message Handler ====================
@dp.message_handler(content_types=ContentType.ANY, chat_type=["group", "supergroup"])
async def group_message_handler(msg: types.Message):
    config = await get_auto_delete_config()
    group_sec = next((c["seconds"] for c in config if c["type"] == "group"), 0)

    if group_sec > 0 and not msg.text.startswith('/'):
        asyncio.create_task(schedule_auto_delete("group", msg.chat.id, msg.message_id, group_sec))

# ==================== CLONE BOT SYSTEM ====================

async def get_clone_bots():
    return load_json("clone_bots")

async def add_clone_bot(token, bot_username, bot_name, owner_id, owner_name, owner_mention):
    bots = load_json("clone_bots")
    
    for bot in bots:
        if bot["token"] == token:
            return None
    
    new_bot = {
        "id": len(bots) + 1,
        "token": token,
        "bot_username": bot_username,
        "bot_name": bot_name,
        "owner_id": owner_id,
        "owner_name": owner_name,
        "owner_mention": owner_mention,
        "created_date": datetime.now().isoformat(),
        "status": "active",
        "total_users": 0,
        "total_searches": 0,
        "last_active": datetime.now().isoformat()
    }
    
    bots.append(new_bot)
    save_json("clone_bots", bots)
    
    # Create user database for this clone
    save_json(f"clone_users_{token.replace(':', '_')}", [])
    
    return new_bot

async def update_clone_bot(token, **kwargs):
    bots = load_json("clone_bots")
    for bot in bots:
        if bot["token"] == token:
            for key, value in kwargs.items():
                bot[key] = value
            bot["last_active"] = datetime.now().isoformat()
            break
    save_json("clone_bots", bots)

async def delete_clone_bot(token):
    # Stop the bot first
    if token in clone_manager.clone_bots:
        await clone_manager.clone_bots[token].stop()
        del clone_manager.clone_bots[token]
    
    # Remove from database
    bots = load_json("clone_bots")
    bots = [b for b in bots if b["token"] != token]
    save_json("clone_bots", bots)
    
    # Delete user data
    user_file = f"{DATA_DIR}/clone_users_{token.replace(':', '_')}.json"
    if os.path.exists(user_file):
        os.remove(user_file)
    
    return True

async def get_clone_bot(token):
    bots = load_json("clone_bots")
    for bot in bots:
        if bot["token"] == token:
            return bot
    return None

async def get_clone_bot_users(token):
    return load_json(f"clone_users_{token.replace(':', '_')}")

async def add_clone_bot_user(token, user_id, user_name, user_mention):
    filename = f"clone_users_{token.replace(':', '_')}"
    users = load_json(filename)
    
    for user in users:
        if user["user_id"] == user_id:
            return False
    
    users.append({
        "user_id": user_id,
        "user_name": user_name,
        "user_mention": user_mention,
        "join_date": datetime.now().isoformat(),
        "search_count": 0
    })
    save_json(filename, users)
    
    await update_clone_bot(token, total_users=len(users))
    return True

async def update_clone_bot_user_search(token, user_id):
    filename = f"clone_users_{token.replace(':', '_')}"
    users = load_json(filename)
    
    for user in users:
        if user["user_id"] == user_id:
            user["search_count"] = user.get("search_count", 0) + 1
            user["last_search"] = datetime.now().isoformat()
            break
    
    save_json(filename, users)
    
    total_searches = sum(u.get("search_count", 0) for u in users)
    await update_clone_bot(token, total_searches=total_searches)

async def get_clone_bot_stats(token):
    bot = await get_clone_bot(token)
    if not bot:
        return None
    
    users = await get_clone_bot_users(token)
    
    yesterday = datetime.now() - timedelta(days=1)
    daily_active = 0
    for user in users:
        last = user.get("last_search")
        if last and datetime.fromisoformat(last) >= yesterday:
            daily_active += 1
    
    return {
        "bot_name": bot.get("bot_name"),
        "bot_username": bot.get("bot_username"),
        "owner_name": bot.get("owner_name"),
        "created_date": bot.get("created_date"),
        "status": bot.get("status"),
        "total_users": len(users),
        "total_searches": bot.get("total_searches", 0),
        "daily_active": daily_active,
        "token": token[:15] + "..."
    }

# ==================== Clone Bot Process ====================
class CloneBotProcess:
    def __init__(self, token, bot_info):
        self.token = token
        self.bot_info = bot_info
        self.bot = None
        self.dp = None
        self.running = False
        self.storage = MemoryStorage()
    
    async def start(self):
        try:
            self.bot = Bot(token=self.token, parse_mode="HTML")
            self.dp = Dispatcher(self.bot, storage=self.storage)
            
            await self.register_handlers()
            
            asyncio.create_task(self.start_polling())
            self.running = True
            
            print(f"✅ Clone Bot @{self.bot_info.get('bot_username')} started")
            return True
        except Exception as e:
            print(f"❌ Clone bot start error: {e}")
            return False
    
    async def start_polling(self):
        try:
            await self.dp.start_polling()
        except Exception as e:
            print(f"Clone bot polling error: {e}")
            self.running = False
    
    async def stop(self):
        if self.running:
            try:
                await self.dp.stop_polling()
                await self.bot.session.close()
            except:
                pass
            self.running = False
            print(f"✅ Clone Bot @{self.bot_info.get('bot_username')} stopped")
    
    async def register_handlers(self):
        
        @self.dp.message_handler(commands=["start"])
        async def clone_start(msg: types.Message):
            user_id = msg.from_user.id
            user_name = msg.from_user.full_name
            user_mention = msg.from_user.get_mention(as_html=True)
            
            await add_clone_bot_user(self.token, user_id, user_name, user_mention)
            
            # Force Join Check - Only permanent channels
            channels = await get_force_channels()
            permanent_channels = [ch for ch in channels if ch.get("is_permanent")]
            
            if permanent_channels:
                not_joined = []
                for ch in permanent_channels:
                    try:
                        m = await self.bot.get_chat_member(ch["chat_id"], user_id)
                        if m.status in ("left", "kicked"):
                            not_joined.append(ch)
                    except:
                        not_joined.append(ch)
                
                if not_joined:
                    kb = InlineKeyboardMarkup()
                    for ch in not_joined:
                        kb.add(InlineKeyboardButton(ch["title"], url=ch["invite"]))
                    kb.add(InlineKeyboardButton("✅ Done ✅", callback_data="clone_force_done"))
                    
                    force_text = await get_custom_text("forcemsg")
                    text = force_text.get("text") or "⚠️ Channel Join လုပ်ပေးပါ။"
                    
                    await msg.answer(text, reply_markup=kb, protect_content=True)
                    return
            
            # Welcome Message with Watermark
            welcome_data = await get_next_welcome_photo()
            
            # PERMANENT WATERMARK - Cannot be removed
            watermark = "\n\n━━━━━━━━━━━━━━\n<i>This Bot Made by @seatvmm using @osamu1123's source</i>"
            
            # Create buttons
            kb = InlineKeyboardMarkup(row_width=2)
            rows = await get_start_buttons_by_row()
            
            for row_num in sorted(rows.keys()):
                row_buttons = rows[row_num]
                buttons = []
                for btn in row_buttons[:2]:
                    if btn.get("type") == "popup":
                        buttons.append(InlineKeyboardButton(btn["name"], callback_data=btn.get("callback_data")))
                    else:
                        buttons.append(InlineKeyboardButton(btn["name"], url=btn["link"]))
                if buttons:
                    kb.row(*buttons)
            
            # PERMANENT OWNER BUTTON - Cannot be removed
            kb.add(InlineKeyboardButton("👑 Bot Owner", callback_data="clone_owner_popup"))
            
            welcome_text = parse_telegram_format(
                welcome_data.get("caption") or welcome_data.get("text", "👋 Welcome!"),
                user_name,
                user_mention
            ) + watermark
            
            if welcome_data and welcome_data.get("photo_id"):
                try:
                    await msg.answer_photo(
                        photo=welcome_data["photo_id"],
                        caption=welcome_text,
                        reply_markup=kb,
                        protect_content=True
                    )
                except:
                    await msg.answer(welcome_text, reply_markup=kb, protect_content=True)
            else:
                await msg.answer(welcome_text, reply_markup=kb, protect_content=True)
            
            # Main Menu
            menu_kb = ReplyKeyboardMarkup(resize_keyboard=True)
            menu_kb.add(KeyboardButton("🔍 Search Movie"))
            menu_kb.add(KeyboardButton("📋 Movie List"))
            await msg.answer("🔝 Main Menu", reply_markup=menu_kb, protect_content=True)
        
        @self.dp.callback_query_handler(lambda c: c.data == "clone_owner_popup")
        async def clone_owner_popup(call: types.CallbackQuery):
            await call.answer(
                f"👑 Owner: @osamu1123\n"
                f"📅 Created by: @seatvmm\n"
                f"🤖 Clone Bot System v1.0",
                show_alert=True
            )
        
        @self.dp.callback_query_handler(lambda c: c.data == "clone_force_done")
        async def clone_force_done(call: types.CallbackQuery):
            channels = await get_force_channels()
            permanent_channels = [ch for ch in channels if ch.get("is_permanent")]
            ok = True
            
            for ch in permanent_channels:
                try:
                    m = await self.bot.get_chat_member(ch["chat_id"], call.from_user.id)
                    if m.status in ("left", "kicked"):
                        ok = False
                        break
                except:
                    ok = False
                    break
            
            if not ok:
                await call.answer("❌ Permanent Channel များကို Join မလုပ်ရသေးပါ။", show_alert=True)
                return
            
            await call.answer("✅ Join ပေးတဲ့အတွက်ကျေးဇူးတင်ပါတယ်!", show_alert=True)
            await call.message.delete()
            
            # Restart start command
            await clone_start(call.message)
        
        @self.dp.message_handler()
        async def clone_search(msg: types.Message):
            if msg.text == "🔍 Search Movie":
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("Movie Code များကြည့်ရန်", url="https://t.me/Movie462"))
                await msg.answer("🔍 <b>ဇာတ်ကား Code ပို့ပေးပါ</b>", reply_markup=kb, protect_content=True)
                return
            
            if msg.text.startswith("/"):
                return
            
            # Force Join Check - Permanent channels only
            channels = await get_force_channels()
            permanent_channels = [ch for ch in channels if ch.get("is_permanent")]
            
            if permanent_channels:
                for ch in permanent_channels:
                    try:
                        m = await self.bot.get_chat_member(ch["chat_id"], msg.from_user.id)
                        if m.status in ("left", "kicked"):
                            kb = InlineKeyboardMarkup()
                            for pc in permanent_channels:
                                kb.add(InlineKeyboardButton(pc["title"], url=pc["invite"]))
                            kb.add(InlineKeyboardButton("✅ Done ✅", callback_data="clone_force_done"))
                            await msg.answer(
                                "⚠️ Permanent Channel Join လုပ်ပေးပါ။",
                                reply_markup=kb,
                                protect_content=True
                            )
                            return
                    except:
                        pass
            
            code = msg.text.strip().upper()
            movie = find_movie_by_code(code)
            
            if not movie:
                await msg.answer(f"❌ Code `{code}` မရှိပါ။", protect_content=True)
                return
            
            # Send Ads (from main bot)
            ads = await get_ads()
            if ads:
                idx = await get_next_ad_index()
                if idx is not None and idx < len(ads):
                    ad = ads[idx]
                    try:
                        await self.bot.copy_message(
                            chat_id=msg.from_user.id,
                            from_chat_id=ad["storage_chat_id"],
                            message_id=ad["message_id"],
                            protect_content=True
                        )
                    except:
                        pass
            
            # Send searching overlay
            await msg.answer("🔍 ရှာဖွေနေပါသည်...", protect_content=True)
            
            try:
                # Send movie
                await self.bot.copy_message(
                    chat_id=msg.from_user.id,
                    from_chat_id=movie["storage_chat_id"],
                    message_id=movie["message_id"],
                    protect_content=True
                )
                
                # Send watermark
                await self.bot.send_message(
                    msg.from_user.id,
                    "━━━━━━━━━━━━━━\n<i>This Bot Made by @seatvmm using @osamu1123's source</i>",
                    protect_content=True
                )
                
                # Update user search count
                await update_clone_bot_user_search(self.token, msg.from_user.id)
                
            except Exception as e:
                await msg.answer("❌ Error sending movie.", protect_content=True)
        
        @self.dp.message_handler(lambda m: m.text == "📋 Movie List")
        async def clone_movie_list(msg: types.Message):
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("📋 Movie Code များကြည့်ရန်", url="https://t.me/Movie462"))
            await msg.answer("Code များကြည့်ရန် အောက်ပါ Button ကိုနှိပ်ပါ", reply_markup=kb, protect_content=True)
        
        @self.dp.message_handler(content_types=ContentType.ANY, chat_type=["group", "supergroup"])
        async def clone_group_message(msg: types.Message):
            config = await get_auto_delete_config()
            group_sec = next((c["seconds"] for c in config if c["type"] == "group"), 0)
            if group_sec > 0 and not msg.text.startswith('/'):
                await asyncio.sleep(group_sec)
                try:
                    await self.bot.delete_message(msg.chat.id, msg.message_id)
                except:
                    pass

# ==================== Clone Bot Manager ====================
class CloneBotManager:
    def __init__(self):
        self.clone_bots = {}
    
    async def load_all_bots(self):
        bots = await get_clone_bots()
        for bot_info in bots:
            if bot_info.get("status") == "active":
                await self.start_bot(bot_info)
    
    async def start_bot(self, bot_info):
        token = bot_info["token"]
        bot_process = CloneBotProcess(token, bot_info)
        if await bot_process.start():
            self.clone_bots[token] = bot_process
            return True
        return False
    
    async def stop_bot(self, token):
        if token in self.clone_bots:
            await self.clone_bots[token].stop()
            del self.clone_bots[token]
            await update_clone_bot(token, status="stopped")
            return True
        return False
    
    async def delete_bot(self, token):
        await self.stop_bot(token)
        await delete_clone_bot(token)
        return True
    
    async def get_bot_status(self, token):
        if token in self.clone_bots:
            return "running"
        return "stopped"

clone_manager = CloneBotManager()

# ==================== Clone Bot Admin Handlers ====================
class CloneBotStates(StatesGroup):
    waiting_token = State()
    confirm_clone = State()

@dp.callback_query_handler(lambda c: c.data == "clone_bot_menu")
async def clone_bot_menu(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    bots = await get_clone_bots()
    running_count = len(clone_manager.clone_bots)
    
    text = "🤖 **Clone Bot Management**\n\n"
    text += f"📊 **Statistics:**\n"
    text += f"• Total Bots: {len(bots)}\n"
    text += f"• Running: {running_count}\n"
    text += f"• Stopped: {len(bots) - running_count}\n\n"
    
    if bots:
        text += "**Bot List:**\n"
        for bot in bots[:5]:  # Show only first 5
            status = "✅" if bot["token"] in clone_manager.clone_bots else "❌"
            text += f"{status} @{bot['bot_username']} - {bot['total_users']} users\n"
    
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Create Clone", callback_data="create_clone"),
        InlineKeyboardButton("📋 All Clones", callback_data="list_clones")
    )
    kb.add(
        InlineKeyboardButton("📊 Clone Stats", callback_data="clone_stats_all"),
        InlineKeyboardButton("🛑 Stop/Restart", callback_data="manage_clones")
    )
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="back_admin"))
    
    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "create_clone")
async def create_clone_start(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    await CloneBotStates.waiting_token.set()
    await call.message.answer(
        "🤖 **Clone Bot ပြုလုပ်ရန်**\n\n"
        "1. @BotFather ဆီသွားပါ\n"
        "2. /newbot ဆိုပြီး Bot အသစ်လုပ်ပါ\n"
        "3. ရလာတဲ့ Token ကို ဒီမှာပို့ပါ\n\n"
        "Token Format: 1234567890:ABCdefGHIjklMNOpqrsTUVwxyz\n\n"
        "မလုပ်တော့ပါက /cancel ရိုက်ပါ။",
        protect_content=True
    )
    await call.answer()

@dp.message_handler(state=CloneBotStates.waiting_token)
async def create_clone_token(msg: types.Message, state: FSMContext):
    if msg.text == "/cancel":
        await msg.answer("❌ Cancelled", protect_content=True)
        await state.finish()
        return
    
    token = msg.text.strip()
    
    if not re.match(r'^\d+:[A-Za-z0-9_-]+$', token):
        await msg.answer("❌ Token Format မမှန်ပါ။", protect_content=True)
        return
    
    # Test token
    try:
        test_bot = Bot(token=token)
        me = await test_bot.get_me()
        await test_bot.session.close()
    except Exception as e:
        await msg.answer(f"❌ Token မမှန်ပါ။ Error: {str(e)}", protect_content=True)
        return
    
    # Check if token exists
    existing = await get_clone_bot(token)
    if existing:
        await msg.answer("❌ ဒီ Token ကို အသုံးပြုပြီးသားဖြစ်ပါသည်။", protect_content=True)
        return
    
    # Save to state
    await state.update_data(
        token=token,
        bot_username=me.username,
        bot_name=me.first_name
    )
    
    # Send to owner for confirmation
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ Confirm", callback_data="confirm_clone_yes"),
        InlineKeyboardButton("❌ Cancel", callback_data="confirm_clone_no")
    )
    
    owner_text = (
        f"🤖 **New Clone Bot Request**\n\n"
        f"👤 **Requester:** {msg.from_user.get_mention(as_html=True)}\n"
        f"🆔 **User ID:** {msg.from_user.id}\n\n"
        f"🤖 **Bot Info:**\n"
        f"• Username: @{me.username}\n"
        f"• Name: {me.first_name}\n"
        f"• Token: {token[:15]}...\n\n"
        f"Clone လုပ်ရန် Confirm လုပ်ပါ။"
    )
    
    await bot.send_message(OWNER_ID, owner_text, reply_markup=kb, protect_content=True)
    await msg.answer("✅ Owner ဆီသို့ ခွင့်ပြုချက်တောင်းခံထားပါသည်။", protect_content=True)
    
    await CloneBotStates.confirm_clone.set()

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_clone_"), state=CloneBotStates.confirm_clone)
async def confirm_clone(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != OWNER_ID:
        await call.answer("You are not owner!", show_alert=True)
        return
    
    data = await state.get_data()
    token = data.get("token")
    bot_username = data.get("bot_username")
    bot_name = data.get("bot_name")
    
    if call.data == "confirm_clone_yes":
        # Add to database
        bot_info = await add_clone_bot(
            token=token,
            bot_username=bot_username,
            bot_name=bot_name,
            owner_id=OWNER_ID,
            owner_name="Owner",
            owner_mention=call.from_user.get_mention(as_html=True)
        )
        
        if bot_info:
            # Start the bot
            await clone_manager.start_bot(bot_info)
            
            await call.message.edit_text(
                f"✅ **Clone Bot Created Successfully!**\n\n"
                f"🤖 Bot: @{bot_username}\n"
                f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            await call.answer("✅ Bot started!", show_alert=True)
        else:
            await call.message.edit_text("❌ Failed to create clone bot.")
    else:
        await call.message.edit_text("❌ Clone Bot creation cancelled.")
    
    await state.finish()

@dp.callback_query_handler(lambda c: c.data == "list_clones")
async def list_clones(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    bots = await get_clone_bots()
    
    if not bots:
        await call.answer("❌ No clone bots yet.", show_alert=True)
        return
    
    text = "📋 **All Clone Bots**\n\n"
    
    for bot in bots:
        status = "✅ Running" if bot["token"] in clone_manager.clone_bots else "❌ Stopped"
        text += f"**ID:** {bot['id']}\n"
        text += f"**Bot:** @{bot['bot_username']}\n"
        text += f"**Name:** {bot['bot_name']}\n"
        text += f"**Status:** {status}\n"
        text += f"**Users:** {bot['total_users']}\n"
        text += f"**Created:** {bot['created_date'][:10]}\n"
        text += "━━━━━━━━━━━━━━\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="clone_bot_menu"))
    
    # Split long message
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for i, part in enumerate(parts):
            if i == 0:
                await call.message.edit_text(part, reply_markup=kb if i == len(parts)-1 else None)
            else:
                await call.message.answer(part)
    else:
        await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "clone_stats_all")
async def clone_stats_all(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    bots = await get_clone_bots()
    
    total_users = sum(bot.get("total_users", 0) for bot in bots)
    total_searches = sum(bot.get("total_searches", 0) for bot in bots)
    running = len([b for b in bots if b["token"] in clone_manager.clone_bots])
    
    text = "📊 **Clone Bot Statistics**\n\n"
    text += f"🤖 **Total Bots:** {len(bots)}\n"
    text += f"✅ **Running:** {running}\n"
    text += f"❌ **Stopped:** {len(bots) - running}\n"
    text += f"👥 **Total Users:** {total_users}\n"
    text += f"🔍 **Total Searches:** {total_searches}\n\n"
    
    if bots:
        text += "**Top 5 Bots:**\n"
        sorted_bots = sorted(bots, key=lambda x: x.get("total_users", 0), reverse=True)[:5]
        for i, bot in enumerate(sorted_bots, 1):
            text += f"{i}. @{bot['bot_username']} - {bot['total_users']} users\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Refresh", callback_data="clone_stats_all"))
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="clone_bot_menu"))
    
    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "manage_clones")
async def manage_clones(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    bots = await get_clone_bots()
    
    if not bots:
        await call.answer("❌ No clone bots.", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(row_width=1)
    
    for bot in bots:
        status = "🟢" if bot["token"] in clone_manager.clone_bots else "🔴"
        kb.add(InlineKeyboardButton(
            f"{status} @{bot['bot_username']} ({bot['total_users']} users)",
            callback_data=f"manage_bot_{bot['token']}"
        ))
    
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="clone_bot_menu"))
    
    await call.message.edit_text("🛠 **Select Bot to Manage:**", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("manage_bot_"))
async def manage_single_bot(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    token = call.data.replace("manage_bot_", "")
    bot = await get_clone_bot(token)
    
    if not bot:
        await call.answer("❌ Bot not found!", show_alert=True)
        return
    
    status = "✅ Running" if token in clone_manager.clone_bots else "❌ Stopped"
    stats = await get_clone_bot_stats(token)
    
    text = f"🤖 **Bot: @{bot['bot_username']}**\n\n"
    text += f"**Name:** {bot['bot_name']}\n"
    text += f"**Status:** {status}\n"
    text += f"**Users:** {bot['total_users']}\n"
    text += f"**Searches:** {bot['total_searches']}\n"
    text += f"**Daily Active:** {stats['daily_active'] if stats else 0}\n"
    text += f"**Created:** {bot['created_date'][:10]}\n"
    text += f"**Token:** {token[:20]}...\n"
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    if token in clone_manager.clone_bots:
        kb.add(InlineKeyboardButton("🛑 Stop", callback_data=f"stop_bot_{token}"))
    else:
        kb.add(InlineKeyboardButton("▶️ Start", callback_data=f"start_bot_{token}"))
    
    kb.add(InlineKeyboardButton("🗑 Delete", callback_data=f"delete_bot_{token}"))
    kb.add(InlineKeyboardButton("📊 Details", callback_data=f"bot_details_{token}"))
    kb.add(InlineKeyboardButton("⬅ Back", callback_data="manage_clones"))
    
    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("start_bot_"))
async def start_bot(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    token = call.data.replace("start_bot_", "")
    bot_info = await get_clone_bot(token)
    
    if bot_info:
        await clone_manager.start_bot(bot_info)
        await update_clone_bot(token, status="active")
        await call.answer("✅ Bot started!", show_alert=True)
    else:
        await call.answer("❌ Bot not found!", show_alert=True)
    
    await manage_single_bot(call)

@dp.callback_query_handler(lambda c: c.data.startswith("stop_bot_"))
async def stop_bot(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    token = call.data.replace("stop_bot_", "")
    
    await clone_manager.stop_bot(token)
    await call.answer("✅ Bot stopped!", show_alert=True)
    
    await manage_single_bot(call)

@dp.callback_query_handler(lambda c: c.data.startswith("delete_bot_"))
async def delete_bot_confirm(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    token = call.data.replace("delete_bot_", "")
    
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_{token}"),
        InlineKeyboardButton("❌ No", callback_data=f"manage_bot_{token}")
    )
    
    await call.message.edit_text(
        f"⚠️ **Are you sure you want to delete this bot?**\n\n"
        f"This will delete all user data permanently.",
        reply_markup=kb
    )

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_delete_"))
async def confirm_delete_bot(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    token = call.data.replace("confirm_delete_", "")
    
    await clone_manager.delete_bot(token)
    await call.answer("✅ Bot deleted!", show_alert=True)
    
    await clone_bot_menu(call)

@dp.callback_query_handler(lambda c: c.data.startswith("bot_details_"))
async def bot_details(call: types.CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return
    
    token = call.data.replace("bot_details_", "")
    stats = await get_clone_bot_stats(token)
    
    if not stats:
        await call.answer("❌ Stats not found!", show_alert=True)
        return
    
    text = f"📊 **Detailed Stats for @{stats['bot_username']}**\n\n"
    text += f"👥 **Total Users:** {stats['total_users']}\n"
    text += f"📈 **Total Searches:** {stats['total_searches']}\n"
    text += f"📊 **Daily Active:** {stats['daily_active']}\n"
    text += f"📅 **Created:** {stats['created_date'][:10]}\n"
    text += f"📌 **Status:** {stats['status']}\n"
    text += f"🔑 **Token:** {stats['token']}\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("⬅ Back", callback_data=f"manage_bot_{token}"))
    
    await call.message.edit_text(text, reply_markup=kb)

# ==================== On Startup ====================
async def load_clone_bots_on_startup():
    await clone_manager.load_all_bots()
    print(f"✅ Loaded {len(clone_manager.clone_bots)} clone bots")

async def on_startup(dp):
    # Ensure all JSON files exist
    for file in ["movies", "users", "ads", "settings", "force_channels", 
                 "custom_texts", "auto_delete", "start_buttons", "start_welcome", "clone_bots"]:
        if not os.path.exists(f"{DATA_DIR}/{file}.json"):
            save_json(file, [])
    
    await load_movies_cache()
    await load_clone_bots_on_startup()
    asyncio.create_task(batch_worker())
    
    print("✅ Bot started with Clone System")
    print(f"✅ Movies in cache: {len(MOVIES_DICT)}")
    print(f"✅ Batch size: {BATCH_SIZE}")
    print(f"✅ Clone bots: {len(clone_manager.clone_bots)}")

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
