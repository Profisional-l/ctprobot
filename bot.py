# bot_student_control_full.py
"""
Бот для контроля ежемесячных платежей учеников с полным функционалом
- Ежемесячная оплата с гибкими сроками
- Система промокодов
- Полная админ-панель
- Разные способы оплаты
- Автоматическое управление доступом
"""
import os
import sqlite3
import time
import threading
import math
import logging
import re
import random
import string
from datetime import datetime, timedelta
import calendar
import pytz
import requests
import telebot
from telebot import types
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# ---------------- CONFIG ----------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
PROVIDER_TOKEN = os.environ.get("PROVIDER_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
CURRENCY = os.environ.get("CURRENCY", "BYN")
REFERRAL_PERCENT = int(os.environ.get("REFERRAL_PERCENT", "10"))
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "300"))
DB_PATH = os.environ.get("DB_PATH", "student_bot.db")

# Проверяем обязательные переменные
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")
if not PROVIDER_TOKEN:
    raise ValueError("PROVIDER_TOKEN не установлен в переменных окружения")
if not ADMIN_IDS:
    raise ValueError("ADMIN_IDS не установлены в переменных окружения")

LOCAL_TZ = pytz.timezone("Europe/Minsk")  # для GMT+3 подходит
def now_local():
    return datetime.now(LOCAL_TZ)

# ----------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

bot = telebot.TeleBot(BOT_TOKEN, threaded=True)

try:
    ME = bot.get_me()
    BOT_ID = ME.id
    logging.info(f"Bot started: @{ME.username} ({BOT_ID})")
except Exception as e:
    logging.exception("Can't get bot info - check BOT_TOKEN")
    raise


# ----------------- DB init + migrations -----------------
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

def init_db_and_migrate():
    # Таблица групп (чатов)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS managed_groups (
        chat_id INTEGER PRIMARY KEY,
        title TEXT,
        is_default INTEGER DEFAULT 0,
        type TEXT DEFAULT 'group',
        added_date INTEGER
    )
    """)
    
    # Таблица тарифов (планов)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS plans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        price_cents INTEGER,
        duration_days INTEGER DEFAULT 30,
        description TEXT,
        media_file_id TEXT,
        media_type TEXT,
        group_id INTEGER,
        created_ts INTEGER,
        media_file_ids TEXT,
        is_active INTEGER DEFAULT 1
    )
    """)
    
    # Таблица пользователей
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        referred_by INTEGER,
        cashback_cents INTEGER DEFAULT 0,
        username TEXT,
        join_date INTEGER
    )
    """)
    
    # Таблица подписок (переработанная для ежемесячных платежей)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        plan_id INTEGER,
        start_ts INTEGER,
        end_ts INTEGER,
        active INTEGER DEFAULT 1,
        invite_link TEXT,
        removed INTEGER DEFAULT 0,
        group_id INTEGER,
        payment_type TEXT DEFAULT 'full',
        current_period_month INTEGER,
        current_period_year INTEGER,
        part_paid TEXT DEFAULT 'none',
        next_payment_date INTEGER,
        last_notification_ts INTEGER
    )
    """)
    
    # Таблица счетов
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS invoices (
        payload TEXT PRIMARY KEY,
        user_id INTEGER,
        plan_id INTEGER,
        amount_cents INTEGER,
        created_ts INTEGER,
        payment_type TEXT DEFAULT 'full',
        period_month INTEGER,
        period_year INTEGER,
        promo_id INTEGER DEFAULT NULL
    )
    """)
    
    # Таблица медиа для тарифов
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS plan_media (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        plan_id INTEGER,
        file_id TEXT,
        media_type TEXT,
        ord INTEGER DEFAULT 0,
        added_ts INTEGER,
        FOREIGN KEY(plan_id) REFERENCES plans(id) ON DELETE CASCADE
    )
    """)
    
    # Таблица методов оплаты
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS payment_methods (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        type TEXT,
        is_active INTEGER DEFAULT 1,
        description TEXT,
        details TEXT
    )
    """)
    
    # Таблица ручных платежей
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manual_payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        plan_id INTEGER,
        amount_cents INTEGER,
        receipt_photo TEXT,
        full_name TEXT,
        status TEXT DEFAULT 'pending',
        created_ts INTEGER,
        admin_id INTEGER,
        reviewed_ts INTEGER,
        payment_type TEXT DEFAULT 'full',
        period_month INTEGER,
        period_year INTEGER,
        promo_id INTEGER DEFAULT NULL
    )
    """)
    
    # Таблица промокодов
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS promo_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        discount_percent INTEGER,
        discount_fixed_cents INTEGER,
        is_active INTEGER DEFAULT 1,
        used_count INTEGER DEFAULT 0,
        max_uses INTEGER DEFAULT NULL,
        created_ts INTEGER,
        expires_ts INTEGER DEFAULT NULL
    )
    """)
    
    # Таблица использования промокодов
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS promo_usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        promo_id INTEGER,
        user_id INTEGER,
        used_ts INTEGER,
        FOREIGN KEY(promo_id) REFERENCES promo_codes(id)
    )
    """)
    
    conn.commit()

    # Инициализация методов оплаты если их нет
    cursor.execute("SELECT COUNT(*) FROM payment_methods")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
        INSERT INTO payment_methods (name, type, is_active, description, details)
        VALUES 
        ('💳 Банковская карта', 'card', 1, 'Оплата банковской картой', ''),
        ('👨‍💻 Ручная оплата', 'manual', 1, 'Оплата по реквизитам с подтверждением чека', 'Реквизиты для оплаты:\\n\\nБанк: Пример Банк\\nСчет: 0000 0000 0000 0000\\nПолучатель: Иван Иванов\\nНазначение: Оплата подписки')
        """)
        conn.commit()

init_db_and_migrate()

# ----------------- Helpers -----------------
def price_str_from_cents(cents):
    if cents is None:
        cents = 0
    return f"{cents//100}.{cents%100:02d} {CURRENCY}"

def cents_from_str(s):
    try:
        s = s.strip()
        if "." in s:
            parts = s.split(".")
            whole = int(parts[0])
            frac = parts[1][:2].ljust(2, "0")
            return whole*100 + int(frac)
        else:
            return int(s)*100
    except Exception:
        return None

def safe_caption(text, limit=1024):
    if not text:
        return None
    if len(text) <= limit:
        return text
    return text[:limit-3] + "..."

def add_user_if_not_exists(user_id, referred_by=None):
    cursor.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if cursor.fetchone() is None:
        cursor.execute("INSERT INTO users (user_id, referred_by, cashback_cents, username, join_date) VALUES (?, ?, 0, NULL, ?)", 
                      (user_id, referred_by, int(time.time())))
        conn.commit()
    # Обновляем username если изменился
    try:
        cursor.execute("UPDATE users SET username = ? WHERE user_id = ?", 
                      (f"@{bot.get_chat(user_id).username}" if bot.get_chat(user_id).username else None, user_id))
        conn.commit()
    except:
        pass

def get_default_group():
    cursor.execute("SELECT chat_id FROM managed_groups WHERE is_default=1 LIMIT 1")
    r = cursor.fetchone()
    if r:
        logging.info(f"✅ Default group found: {r[0]}")
        return r[0]
    
    cursor.execute("SELECT chat_id FROM managed_groups LIMIT 1")
    r = cursor.fetchone()
    if r:
        logging.info(f"✅ First group found: {r[0]}")
        return r[0]
    
    logging.error("🚫 No groups found in database")
    
    # Выводим все группы для отладки
    cursor.execute("SELECT chat_id, title, is_default FROM managed_groups")
    all_groups = cursor.fetchall()
    if all_groups:
        logging.info(f"📋 All groups in DB: {all_groups}")
    else:
        logging.info("📭 No groups in DB at all")
    
    return None

@bot.message_handler(commands=["check_groups"])
def cmd_check_groups(message):
    """Проверка групп в базе данных"""
    if message.from_user.id not in ADMIN_IDS:
        return
    
    cursor.execute("SELECT chat_id, title, is_default, type FROM managed_groups")
    groups = cursor.fetchall()
    
    if not groups:
        bot.send_message(message.chat.id, "📭 В базе нет групп")
        return
    
    text = "📋 <b>Группы в базе данных:</b>\n\n"
    for chat_id, title, is_default, type_ in groups:
        default_text = "✅ ПО УМОЛЧАНИЮ" if is_default else ""
        text += f"🏷️ <b>{title}</b>\nID: <code>{chat_id}</code>\nТип: {type_} {default_text}\n\n"
    
    # Проверяем группы из планов
    cursor.execute("SELECT DISTINCT p.id, p.title, p.group_id, mg.title FROM plans p LEFT JOIN managed_groups mg ON p.group_id = mg.chat_id")
    plans = cursor.fetchall()
    
    if plans:
        text += "\n📚 <b>Группы в тарифах:</b>\n\n"
        for pid, ptitle, group_id, mg_title in plans:
            status = "✅ Найдена" if group_id else "❌ Нет группы"
            text += f"📝 {ptitle} (ID плана: {pid})\nГруппа ID: {group_id} - {mg_title or 'Неизвестно'}\nСтатус: {status}\n\n"
    
    bot.send_message(message.chat.id, text, parse_mode="HTML")

def set_default_group(chat_id):
    cursor.execute("UPDATE managed_groups SET is_default=0")
    cursor.execute("UPDATE managed_groups SET is_default=1 WHERE chat_id=?", (chat_id,))
    conn.commit()

def create_chat_invite_link_one_time(bot_token, chat_id, expire_seconds=7*24*3600, member_limit=1):
    url = f"https://api.telegram.org/bot{bot_token}/createChatInviteLink"
    expire_date = int(time.time()) + expire_seconds
    payload = {"chat_id": chat_id, "expire_date": expire_date, "member_limit": member_limit}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("ok"):
                return data["result"]["invite_link"]
    except Exception as e:
        logging.warning("createChatInviteLink failed: %s", e)
    return None

def get_bot_invite_link():
    username = bot.get_me().username
    return f"https://t.me/{username}?startgroup=true"

def is_bot_admin_in_chat(chat_id):
    try:
        member = bot.get_chat_member(chat_id, BOT_ID)
        return member.status in ['administrator', 'creator']
    except Exception:
        return False

def add_group_to_db(chat_id, title, chat_type='group'):
    try:
        cursor.execute("INSERT OR REPLACE INTO managed_groups (chat_id, title, type, added_date) VALUES (?, ?, ?, ?)",
                       (chat_id, title, chat_type, int(time.time())))
        cursor.execute("SELECT COUNT(*) FROM managed_groups")
        count = cursor.fetchone()[0]
        if count == 1:
            cursor.execute("UPDATE managed_groups SET is_default=1 WHERE chat_id=?", (chat_id,))
        conn.commit()
        return True
    except Exception as e:
        logging.exception("add_group_to_db error: %s", e)
        return False

def get_all_groups_with_bot():
    cursor.execute("SELECT chat_id, title, type FROM managed_groups ORDER BY added_date DESC")
    return cursor.fetchall()

def get_active_payment_methods():
    cursor.execute("SELECT id, name, type, description, details FROM payment_methods WHERE is_active=1 ORDER BY id")
    return cursor.fetchall()

def get_payment_method_by_id(method_id):
    cursor.execute("SELECT id, name, type, description, details FROM payment_methods WHERE id=?", (method_id,))
    return cursor.fetchone()

def get_current_period():
    """Возвращает текущий месяц и год"""
    now = now_local()

    return now.month, now.year

def get_payment_deadlines():
    """Возвращает дедлайны оплаты для текущего месяца"""
    now = now_local()

    year = now.year
    month = now.month
    
    # Дедлайн первой части: 5 число текущего месяца 23:59
    first_deadline = datetime(year, month, 5, 23, 59, 59)
    
    # Дедлайн второй части: 20 число текущего месяца 23:59
    second_deadline = datetime(year, month, 20, 23, 59, 59)
    
    return first_deadline, second_deadline

def is_payment_period_active():
    """Проверяет, активен ли сейчас период оплаты"""
    now = now_local()

    day = now.day
    return (1 <= day <= 5) or (15 <= day <= 20)

def get_active_payment_type():
    """Возвращает тип активного периода оплаты"""
    now = now_local()
    day = now.day
    
    if 1 <= day <= 5:
        return 'first'
    elif 15 <= day <= 20:
        return 'second'
    elif day >= 21:
        return 'half_month'  # Новый тип периода
    else:
        return 'full_anytime'

def can_user_pay_partial(user_id, plan_id):
    """Проверяет, может ли пользователь оплатить вторую часть"""
    month, year = get_current_period()
    cursor.execute("""
        SELECT id FROM subscriptions 
        WHERE user_id=? AND plan_id=? AND current_period_month=? AND current_period_year=? AND part_paid='first'
    """, (user_id, plan_id, month, year))
    return cursor.fetchone() is not None

def activate_subscription(user_id, plan_id, payment_type='full', group_id=None):
    """Активирует или продлевает подписку для пользователя с учетом типа оплаты"""
    cursor.execute("SELECT price_cents, title, group_id FROM plans WHERE id=?", (plan_id,))
    plan = cursor.fetchone()
    if not plan:
        return False, "Тариф не найден"
    
    price_cents, plan_title, plan_group_id = plan
    current_month, current_year = get_current_period()
    
    target_group_id = plan_group_id if plan_group_id else group_id
    if not target_group_id:
        return False, "Не указана группа для подписки"
    
    try:
        # Пробуем разбанить пользователя, если он забанен
        bot.unban_chat_member(target_group_id, user_id)
        logging.info(f"🔄 Попытка разбанить пользователя {user_id} в группе {target_group_id}")
    except Exception as e:
        # Ошибка может быть если пользователь не забанен или бот не админ
        logging.debug(f"⚠️ Не удалось разбанить пользователя {user_id}: {e}")
    

    start_ts = int(time.time())
    now = now_local()
    
    # Проверяем, есть ли уже активная подписка для этого пользователя и тарифа
    cursor.execute("""
        SELECT id, part_paid, current_period_month, current_period_year, end_ts 
        FROM subscriptions 
        WHERE user_id=? AND plan_id=? AND active=1
        ORDER BY end_ts DESC
        LIMIT 1
    """, (user_id, plan_id))
    
    existing_sub = cursor.fetchone()
    
    existing_end_ts = start_ts
    existing_month = current_month
    existing_year = current_year
    
    # Если есть активная подписка и мы продлеваем её
    if existing_sub:
        sub_id, existing_part_paid, existing_month, existing_year, existing_end_ts = existing_sub
        
        # Если подписка уже оплачена за текущий месяц полностью
        if existing_part_paid == 'full' and existing_month == current_month and existing_year == current_year:
            # Находим последнюю неактивную подписку для продления
            cursor.execute("""
                SELECT id, end_ts 
                FROM subscriptions 
                WHERE user_id=? AND plan_id=? AND active=0
                ORDER BY end_ts DESC
                LIMIT 1
            """, (user_id, plan_id))
            
            inactive_sub = cursor.fetchone()
            if inactive_sub:
                sub_id = inactive_sub[0]
                existing_end_ts = inactive_sub[1]
            else:
                # Создаем новую запись для продления
                existing_end_ts = start_ts
        # Если есть первая часть и доплачиваем вторую
        elif existing_part_paid == 'first' and existing_month == current_month and existing_year == current_year:
            if payment_type in ('second_part', 'second_part_late', 'full'):
                # Обновляем существующую запись
                pass
    
    # НОВАЯ ЛОГИКА: для подключения после 21 числа
    if payment_type == 'half_month':
        # Половина месяца - доступ до 5 числа следующего месяца
        if now.month == 12:
            next_month = 1
            next_year = now.year + 1
        else:
            next_month = now.month + 1
            next_year = now.year
        end_dt = LOCAL_TZ.localize(datetime(next_year, next_month, 5, 23, 59, 59))
        end_ts = int(end_dt.timestamp())
        part_paid = 'full'  # Считаем как полную оплату за оставшийся период
        
    # Рассчитываем end_ts в зависимости от типа оплаты
    elif payment_type in ('full', 'full_anytime'):
        # Полная оплата - доступ до 5 числа следующего месяца
        if existing_sub and existing_end_ts > start_ts:
            # Если есть действующая подписка, продлеваем её
            if now.month == 12:
                next_month = 1
                next_year = now.year + 1
            else:
                next_month = now.month + 1
                next_year = now.year
        else:
            # Новая подписка
            if now.month == 12:
                next_month = 1
                next_year = now.year + 1
            else:
                next_month = now.month + 1
                next_year = now.year
        
        end_dt = LOCAL_TZ.localize(datetime(next_year, next_month, 5, 23, 59, 59))
        end_ts = int(end_dt.timestamp())
        part_paid = 'full'
        
    elif payment_type == 'partial':
        # Первая часть - доступ до 15 числа текущего месяца
        end_dt = LOCAL_TZ.localize(datetime(now.year, now.month, 15, 23, 59, 59))
        end_ts = int(end_dt.timestamp())
        part_paid = 'first'
        
    elif payment_type in ('second_part', 'second_part_late'):
        # Вторая часть (вовремя или поздно) - доступ до 5 числа следующего месяца
        if now.month == 12:
            next_month = 1
            next_year = now.year + 1
        else:
            next_month = now.month + 1
            next_year = now.year
        end_dt = LOCAL_TZ.localize(datetime(next_year, next_month, 5, 23, 59, 59))
        end_ts = int(end_dt.timestamp())
        part_paid = 'full'
    
    # Всегда генерируем новую ссылку
    invite_link = create_chat_invite_link_one_time(BOT_TOKEN, target_group_id, expire_seconds=7*24*3600, member_limit=1)
    
    if not invite_link:
        logging.error(f"Не удалось создать ссылку для группы {target_group_id}")
        return False, "Не удалось создать пригласительную ссылку"
    
    if existing_sub and existing_sub[2] == current_month and existing_sub[3] == current_year:
        # Обновляем существующую подписку на ТЕКУЩИЙ период
        sub_id = existing_sub[0]
        
        # Если обновляем первую часть на полную (доплачиваем вторую)
        if existing_sub[1] == 'first' and part_paid == 'full':
            cursor.execute("""
                UPDATE subscriptions 
                SET payment_type=?, part_paid=?, end_ts=?, invite_link=?, active=1, removed=0
                WHERE id=?
            """, (payment_type, part_paid, end_ts, invite_link, sub_id))
        else:
            # Для других случаев
            cursor.execute("""
                UPDATE subscriptions 
                SET payment_type=?, part_paid=?, end_ts=?, invite_link=?, active=1, removed=0
                WHERE id=?
            """, (payment_type, part_paid, end_ts, invite_link, sub_id))
    elif existing_sub:
        # Продлеваем существующую подписку (обновляем период и срок)
        sub_id = existing_sub[0]
        cursor.execute("""
            UPDATE subscriptions 
            SET payment_type=?, part_paid=?, current_period_month=?, current_period_year=?, 
                end_ts=?, invite_link=?, active=1, removed=0
            WHERE id=?
        """, (payment_type, part_paid, current_month, current_year, end_ts, invite_link, sub_id))
    else:
        # Создаем новую подписку
        cursor.execute("""
            INSERT INTO subscriptions (user_id, plan_id, start_ts, end_ts, invite_link, active, removed, group_id, 
                                     payment_type, current_period_month, current_period_year, part_paid, next_payment_date) 
            VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?)
        """, (user_id, plan_id, start_ts, end_ts, invite_link, target_group_id, payment_type, 
              current_month, current_year, part_paid, end_ts))
    
    conn.commit()
    
    return True, invite_link

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("approve_payment:"))
def callback_approve_payment(call):
    """Одобрение ручной оплаты"""
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    payment_id = int(call.data.split(":")[1])
    
    cursor.execute("""
        SELECT mp.user_id, mp.plan_id, mp.payment_type, mp.promo_id
        FROM manual_payments mp
        WHERE mp.id = ? AND mp.status = 'pending'
    """, (payment_id,))
    
    payment = cursor.fetchone()
    if not payment:
        bot.answer_callback_query(call.id, "❌ Заявка не найдена или уже обработана.")
        return
        
    user_id, plan_id, payment_type, promo_id = payment
    
    # Активируем подписку
    success, result = activate_subscription(user_id, plan_id, payment_type)
    
    if success:
        cursor.execute("UPDATE manual_payments SET status='approved', admin_id=?, reviewed_ts=? WHERE id=?", 
                      (call.from_user.id, int(time.time()), payment_id))
        conn.commit()
        
        # Если был промокод, отмечаем его использование
        if promo_id:
            cursor.execute("INSERT INTO promo_usage (promo_id, user_id, used_ts) VALUES (?, ?, ?)",
                          (promo_id, user_id, int(time.time())))
            cursor.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE id=?", (promo_id,))
            conn.commit()
        
        # Уведомляем пользователя
        try:
            cursor.execute("SELECT title FROM plans WHERE id=?", (plan_id,))
            plan_title = cursor.fetchone()[0]
            
            text = (f"✅ Ваша заявка на оплату группы '{plan_title}' одобрена!\n\n"
                    f"🔗 Ваша приватная ссылка для входа в чат (одноразовая):\n{result}")
            
            bot.send_message(user_id, text, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error notifying user {user_id}: {e}")
        
        bot.answer_callback_query(call.id, "✅ Заявка одобрена!")
        try:
            bot.edit_message_caption(f"✅ ЗАЯВКА ОДОБРЕНА", call.message.chat.id, call.message.message_id)
        except:
            pass
    else:
        bot.answer_callback_query(call.id, f"❌ Ошибка активации: {result}")


def generate_promo_code(length=8):
    """Генерирует уникальный промокод"""
    while True:
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
        cursor.execute("SELECT id FROM promo_codes WHERE code=?", (code,))
        if not cursor.fetchone():
            return code

def get_promo_code(code):
    """Получает информацию о промокоде"""
    cursor.execute("""
        SELECT id, code, discount_percent, discount_fixed_cents, is_active, used_count, max_uses, expires_ts 
        FROM promo_codes WHERE code=?
    """, (code,))
    return cursor.fetchone()

def can_use_promo_code(promo_id, user_id):
    """Проверяет может ли пользователь использовать промокод"""
    cursor.execute("SELECT id FROM promo_usage WHERE promo_id=? AND user_id=?", (promo_id, user_id))
    if cursor.fetchone():
        return False, "Вы уже использовали этот промокод"
    
    cursor.execute("SELECT is_active, max_uses, used_count, expires_ts FROM promo_codes WHERE id=?", (promo_id,))
    promo = cursor.fetchone()
    if not promo:
        return False, "Промокод не найден"
    
    is_active, max_uses, used_count, expires_ts = promo
    
    if not is_active:
        return False, "Промокод неактивен"
    
    if max_uses and used_count >= max_uses:
        return False, "Промокод уже использован максимальное количество раз"
    
    if expires_ts and expires_ts < int(time.time()):
        return False, "Срок действия промокода истек"
    
    return True, "OK"

def apply_promo_code(price_cents, promo_data):
    """Применяет промокод к цене"""
    promo_id, code, discount_percent, discount_fixed_cents, is_active, used_count, max_uses, expires_ts = promo_data
    
    if discount_percent:
        discount = int(price_cents * discount_percent / 100)
        new_price = max(0, price_cents - discount)
        return new_price, f"Промокод {code} применен! Скидка {discount_percent}%"
    elif discount_fixed_cents:
        new_price = max(0, price_cents - discount_fixed_cents)
        return new_price, f"Промокод {code} применен! Скидка {price_str_from_cents(discount_fixed_cents)}"
    
    return price_cents, "Ошибка применения промокода"

def get_payment_options(user_id, plan_id):
    """Возвращает доступные варианты оплаты для пользователя"""
    # Проверяем существующую подписку
    has_active_sub, sub_info, message = check_existing_active_subscription(user_id, plan_id)
    
    # Если уже есть полная подписка - не показываем варианты
    if has_active_sub and sub_info and sub_info[2] == 'full':
        return []
    
    active_type = get_active_payment_type()
    cursor.execute("SELECT price_cents FROM plans WHERE id=?", (plan_id,))
    plan = cursor.fetchone()
    if not plan:
        return []
    
    price_cents = plan[0]
    first_part_price = price_cents // 2
    second_part_price = first_part_price
    
    options = []
    
    # Если есть подписка с первой частью
    has_paid_first_part = False
    if has_active_sub and sub_info and sub_info[2] == 'first':
        has_paid_first_part = True
    
    now = now_local()
    day = now.day
    
    # НОВАЯ ЛОГИКА: после 21 числа - всегда половина стоимости
    if day >= 21:
        # Если уже есть первая часть, предлагаем доплатить вторую
        if has_paid_first_part:
            options.append({
                'type': 'second_part_late',
                'price': second_part_price,
                'text': f"💳 Доплатить вторую часть",
                'description': "Доступ до 5 числа следующего месяца"
            })
        else:
            options.append({
                'type': 'half_month',
                'price': first_part_price,
                'text': f"💳 Оплатить половину месяца",
                'description': "Доступ до 5 числа следующего месяца"
            })
        
    elif active_type == 'first':
        # Период 1-5 чисел
        
        # Если есть первая часть - НЕ показываем полную оплату!
        if has_paid_first_part:
            # Только вторую часть
            options.append({
                'type': 'second_part',
                'price': second_part_price,
                'text': f"💳 Доплатить вторую часть",
                'description': "Оплачивается 15-20 числа, доступ до 5 числа следующего месяца"
            })
        else:
            # Нет первой части - показываем оба варианта
            options.append({
                'type': 'full',
                'price': price_cents,
                'text': f"💳 Полная оплата",
                'description': "Доступ до 5 числа следующего месяца"
            })
            
            options.append({
                'type': 'partial', 
                'price': first_part_price,
                'text': f"💳 Оплатить первой частью",
                'description': f"Вторая часть {price_str_from_cents(second_part_price)} оплачивается 15-20 числа"
            })
        
    elif active_type == 'second':
        # Период 15-20 чисел
        
        # Если есть первая часть
        if has_paid_first_part:
            options.append({
                'type': 'second_part',
                'price': second_part_price,
                'text': f"💳 Доплатить вторую часть",
                'description': "Доступ до 5 числа следующего месяца"
            })
        else:
            # Нет первой части - только полная оплата
            options.append({
                'type': 'full',
                'price': price_cents,
                'text': f"💳 Полная оплата", 
                'description': "Доступ до 5 числа следующего месяца"
            })
            
    else:  # full_anytime - между 6-14 числа
        # Между 6-14 числа предлагаем только полную оплату
        
        # Если есть первая часть, предлагаем доплатить вторую (пропустили период)
        if has_paid_first_part:
            options.append({
                'type': 'second_part_late',
                'price': second_part_price,
                'text': f"💳 Доплатить вторую часть",
                'description': "Доступ до 5 числа следующего месяца (восстановление доступа)"
            })
        else:
            # Нет первой части - только полная оплата
            options.append({
                'type': 'full',
                'price': price_cents,
                'text': f"💳 Полная оплата",
                'description': "Доступ до 5 числа следующего месяца"
            })
    
    return options
# admin ephemeral states
admin_states = {}

# user ephemeral states для ручной оплаты и промокодов
user_states = {}

# ----------------- Update listener (fallback) -----------------
def process_updates(updates):
    for u in updates:
        try:
            if hasattr(u, "my_chat_member") and u.my_chat_member is not None:
                cm = u.my_chat_member
                chat = cm.chat
                new = cm.new_chat_member
                if new.user and new.user.id == BOT_ID:
                    chat_id = chat.id
                    title = chat.title or chat.username or str(chat_id)
                    status = new.status
                    if status in ("administrator", "creator"):
                        add_group_to_db(chat_id, title, chat.type if hasattr(chat, "type") else "group")
                        for aid in ADMIN_IDS:
                            try:
                                bot.send_message(aid, f"✅ Бот получил права администратора в чате: {title} (ID: {chat_id})")
                            except:
                                pass
                    elif status in ("member",):
                        add_group_to_db(chat_id, title, chat.type if hasattr(chat, "type") else "group")
                        for aid in ADMIN_IDS:
                            try:
                                bot.send_message(aid, f"✅ Бот добавлен в чат: {title} (ID: {chat_id})")
                            except:
                                pass
                    elif status in ("left", "kicked"):
                        try:
                            cursor.execute("DELETE FROM managed_groups WHERE chat_id=?", (chat_id,))
                            conn.commit()
                        except:
                            pass
                        for aid in ADMIN_IDS:
                            try:
                                bot.send_message(aid, f"❌ Бот удалён из чата: {title} (ID: {chat_id})")
                            except:
                                pass
        except Exception:
            logging.exception("Error in process_updates")

bot.set_update_listener(process_updates)

@bot.message_handler(content_types=['successful_payment'])
def got_payment(message):
    sp = message.successful_payment
    payload = sp.invoice_payload
    user_id = message.from_user.id
    
    # Парсим payload для получения информации
    parts = payload.split(":")
    plan_id = int(parts[1])
    payment_type = parts[5]
    period_month = int(parts[7])
    period_year = int(parts[9])
    promo_id = int(parts[11]) if len(parts) > 11 and parts[11] != '0' else None

    success, result = activate_subscription(user_id, plan_id, payment_type)
    if not success:
        bot.send_message(user_id, f"❌ Ошибка активации подписки: {result}")
        return
    
    # Если был применен промокод, отмечаем его использование
    if promo_id and promo_id > 0:
        cursor.execute("INSERT INTO promo_usage (promo_id, user_id, used_ts) VALUES (?, ?, ?)",
                      (promo_id, user_id, int(time.time())))
        cursor.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE id=?", (promo_id,))
        conn.commit()
    
    # Реферальный кэшбэк
    cursor.execute("SELECT referred_by FROM users WHERE user_id=?", (user_id,))
    urow = cursor.fetchone()
    referred_by = urow[0] if urow else None
    
    if referred_by:
        cursor.execute("SELECT amount_cents FROM invoices WHERE payload=?", (payload,))
        inv_row = cursor.fetchone()
        if inv_row:
            amount_cents = inv_row[0]
            cashback = int(math.floor(amount_cents * REFERRAL_PERCENT / 100.0))
            cursor.execute("UPDATE users SET cashback_cents = cashback_cents + ? WHERE user_id=?", (cashback, referred_by))
            conn.commit()
            try:
                bot.send_message(referred_by, f"💰 Реферальный кэшбэк! Пользователь @{message.from_user.username or message.from_user.id} оплатил подписку. Вам начислен кэшбэк: {price_str_from_cents(cashback)}")
            except:
                pass
    
    # Формируем текст сообщения в зависимости от типа оплаты
    cursor.execute("SELECT title FROM plans WHERE id=?", (plan_id,))
    found = cursor.fetchone()
    if found:
        plan_title = found[0]
        
        if payment_type == 'half_month':
            txt = (f"✅ <b>Спасибо за оплату половины месяца в группе '{plan_title}'!</b>\n\n"
                   f"🔗 Ваша приватная ссылка для входа в чат (одноразовая):\n{result}\n\n"
                   f"⏰ Подписка активна до 5 числа следующего месяца")
        elif payment_type == 'partial':
            txt = (f"✅ <b>Спасибо за оплату первой части в группе '{plan_title}'!</b>\n\n"
                   f"🔗 Ваша приватная ссылка для входа в чат (одноразовая):\n{result}\n\n"
                   f"⏰ Подписка активна до 15 числа текущего месяца\n"
                   f"💳 <b>Вторая часть оплачивается 15-20 числа</b>")
        else:
            # Полная оплата - проверяем, продление это или новая подписка
            cursor.execute("""
                SELECT COUNT(*) FROM subscriptions 
                WHERE user_id=? AND plan_id=? AND active=1
            """, (user_id, plan_id))
            count = cursor.fetchone()[0]
            
            if count > 1:
                # Это продление
                txt = (f"✅ <b>Спасибо за продление подписки на группу '{plan_title}'!</b>\n\n"
                       f"🔗 Ваша новая приватная ссылка для входа в чат (одноразовая):\n{result}\n\n"
                       f"⏰ Подписка продлена до 5 числа следующего месяца")
            else:
                # Новая подписка
                txt = (f"✅ <b>Спасибо за оплату группы '{plan_title}'!</b>\n\n"
                       f"🔗 Ваша приватная ссылка для входа в чат (одноразовая):\n{result}\n\n"
                       f"⏰ Подписка активна до 5 числа следующего месяца")
        
        bot.send_message(user_id, txt, parse_mode="HTML")
    else:
        bot.send_message(user_id, f"✅ Платёж принят! 🔗 Ваша ссылка: {result}")
    
    # Очищаем состояние пользователя
    if user_id in user_states:
        user_states.pop(user_id)

@bot.message_handler(commands=["debug"])
def cmd_debug(message):
    """Отладочная информация"""
    if message.from_user.id not in ADMIN_IDS:
        return
    
    now = now_local()
    current_month, current_year = get_current_period()
    active_type = get_active_payment_type()
    
    text = (f"📊 <b>Отладочная информация</b>\n\n"
            f"🕐 Текущее время: {now.strftime('%d.%m.%Y %H:%M:%S')}\n"
            f"📅 Текущий период: {current_month}.{current_year}\n"
            f"💳 Активный тип оплаты: {active_type}\n\n")
    
    # Проверяем уведомления
    cursor.execute("SELECT COUNT(*) FROM subscriptions WHERE active=1")
    active_subs = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(*) FROM subscriptions 
        WHERE active=1 AND (current_period_month != ? OR current_period_year != ?)
    """, (current_month, current_year))
    needs_renewal = cursor.fetchone()[0]
    
    text += (f"📈 Статистика:\n"
             f"• Активных подписок: {active_subs}\n"
             f"• Требуют продления: {needs_renewal}\n\n")
    
    # Проверяем следующее уведомление
    next_notification = "Следующее уведомление: "
    if now.day == 1 and now.hour < 10:
        next_notification += "Сегодня в 10:00 (1 число)"
    elif now.day < 4 or (now.day == 4 and now.hour < 18):
        next_notification += "4 числа в 18:00"
    elif now.day < 15 or (now.day == 15 and now.hour < 10):
        next_notification += "15 числа в 10:00"
    elif now.day < 19 or (now.day == 19 and now.hour < 18):
        next_notification += "19 числа в 18:00"
    else:
        next_notification += "1 числа следующего месяца в 10:00"
    
    text += next_notification
    
    bot.send_message(message.chat.id, text, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("renew:"))
def callback_renew_plan(call):
    """Продление конкретной подписки"""
    try:
        plan_id = int(call.data.split(":")[1])
        user_id = call.from_user.id
        
        # Проверяем, есть ли уже активная подписка на этот план в текущем месяце
        current_month, current_year = get_current_period()
        cursor.execute("""
            SELECT id, part_paid FROM subscriptions 
            WHERE user_id=? AND plan_id=? AND current_period_month=? AND current_period_year=?
        """, (user_id, plan_id, current_month, current_year))
        
        existing_sub = cursor.fetchone()
        
        if existing_sub:
            sub_id, part_paid = existing_sub
            if part_paid == 'full':
                bot.answer_callback_query(call.id, "✅ Подписка уже оплачена за этот месяц")
                return
        
        # Показываем варианты оплаты
        show_plan_full_info(call.message.chat.id, user_id, plan_id, show_back_button=True)
        bot.answer_callback_query(call.id, "💳 Выберите вариант оплаты")
        
    except Exception as e:
        logging.exception("Error in callback_renew_plan")
        bot.answer_callback_query(call.id, "❌ Ошибка")

# ----------------- my_chat_member handler -----------------
@bot.my_chat_member_handler()
def handle_my_chat_member(update):
    try:
        chat = update.chat
        new = update.new_chat_member
        old = update.old_chat_member
        chat_id = chat.id
        title = chat.title or chat.username or str(chat_id)
        new_status = new.status
        old_status = old.status if old else None

        logging.info(f"my_chat_member update: chat={chat_id} status {old_status} -> {new_status}")

        if new_status in ("administrator", "creator", "member"):
            add_group_to_db(chat_id, title, getattr(chat, "type", "group"))
            for aid in ADMIN_IDS:
                try:
                    bot.send_message(aid, f"✅ Бот активирован/добавлен в чат: {title} (ID: {chat_id}). Статус: {new_status}")
                except:
                    pass
            try:
                if chat.type in ("group", "supergroup"):
                    bot.send_message(chat_id, "✅ Бот добавлен. Для работы функций с подписками назначьте ему права администратора и используйте /register_group внутри группы.")
            except Exception:
                pass

        if new_status in ("left", "kicked"):
            try:
                cursor.execute("DELETE FROM managed_groups WHERE chat_id=?", (chat_id,))
                conn.commit()
            except:
                pass
            for aid in ADMIN_IDS:
                try:
                    bot.send_message(aid, f"❌ Бот удалён из чата: {title} (ID: {chat_id})")
                except:
                    pass

    except Exception:
        logging.exception("Error in handle_my_chat_member")

# ----------------- Main menu / user handlers -----------------
def main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    btn_plans = types.KeyboardButton("📋 Группы обучения")
    # btn_balance = types.KeyboardButton("💰 Баланс")
    # btn_ref = types.KeyboardButton("👥 Реферальная ссылка")
    btn_sub = types.KeyboardButton("🎫 Мои подписки")
    btn_bonus = types.KeyboardButton("🎁 Бонусная программа")  # Новая кнопка
    # markup.row(btn_plans, btn_balance)
    # markup.row(btn_sub, btn_ref)
    markup.row(btn_plans)
    markup.row(btn_sub)
    markup.row(btn_bonus)
    if user_id in ADMIN_IDS:
        markup.row(types.KeyboardButton("⚙️ Админ меню"))
    return markup

@bot.message_handler(func=lambda message: message.text == "🎁 Бонусная программа")
# @only_private  # Убрать эту строку
def show_bonus_program(message):
    text = "🎁 Платим вознаграждение 40 byn за приведенного друга!"
    bot.send_message(message.chat.id, text)

@bot.message_handler(commands=["start"])
def cmd_start(message):
    args = message.text.split()
    ref = None
    if len(args) > 1:
        token = args[1]
        if token.startswith("ref"):
            try:
                ref = int(token[3:])
            except:
                ref = None
    user_id = message.from_user.id
    if ref and ref != user_id:
        add_user_if_not_exists(user_id, referred_by=ref)
        try:
            bot.send_message(ref, f"🎉 Новый реферал! Пользователь @{message.from_user.username or message.from_user.id} пришёл по вашей ссылке.")
        except:
            pass
        welcome_text = "👋 Привет! Вы пришли по реферальной ссылке."
    else:
        add_user_if_not_exists(user_id, None)
        welcome_text = "👋 Привет! Добро пожаловать!"

    if message.chat.type in ("group", "supergroup", "channel"):
        bot.send_message(message.chat.id, f"{welcome_text}\n\nℹ️ Для управления подписками откройте приватный чат со мной: @{ME.username}")
        return

    bot.send_message(message.chat.id, welcome_text, reply_markup=main_menu(user_id))

# All user-visible command handlers below will ignore non-private chats (so bot won't chat in groups)
def only_private(fn):
    def wrapper(message, *a, **k):
        if message.chat.type != "private":
            return
        return fn(message, *a, **k)
    return wrapper

@bot.message_handler(func=lambda message: message.text == "📋 Группы обучения")
@only_private
def show_plans(message):
    cursor.execute("""
        SELECT p.id, p.title, p.price_cents, p.duration_days, p.description, p.media_file_id, p.media_type, p.media_file_ids, p.group_id, mg.title
        FROM plans p
        LEFT JOIN managed_groups mg ON p.group_id = mg.chat_id
        WHERE p.is_active=1
        ORDER BY p.id
    """)
    rows = cursor.fetchall()
    if not rows:
        bot.send_message(message.chat.id, "📭 Группы обучения пока не созданы.", reply_markup=main_menu(message.from_user.id))
        return
    
    chat_id = message.chat.id
    
    # Если группа всего одна - сразу показываем полную информацию
    if len(rows) == 1:
        r = rows[0]
        pid, title, price_cents, days, desc, media_file_id, media_type, media_file_ids, group_id, group_title = r
        
        # Получаем доступные варианты оплаты
        payment_options = get_payment_options(message.from_user.id, pid)
        
        text = (f"<b>Оформление подписки на группу '{title}'</b>\n\n"
                f"💰 Цена в месяц: {price_str_from_cents(price_cents)}\n"
                f"📋 Описание: {desc}\n\n")
        
        markup = types.InlineKeyboardMarkup()
        
        if payment_options:
            text += "<b>Детали:</b>\n"
            for option in payment_options:
                text += f"• {option['text']}\n  {option['description']}\n\n"
            
            for option in payment_options:
                markup.add(types.InlineKeyboardButton(f"💸 Оплатить {price_str_from_cents(option['price'])}", callback_data=f"buy_{option['type']}:{pid}"))
        else:
            active_type = get_active_payment_type()
            if active_type == 'second':
                text += "❌ <b>У вас нет активной первой части оплаты для этой группы.</b>\n\n"
            else:
                text += "❌ <b>Сейчас не период оплаты.</b>\n\n"
            
            text += ("💳 <b>Периоды оплаты:</b>\n"
                    "• 1-5 числа: полная оплата или первая часть\n"
                    "• 15-20 числа: вторая часть (только при оплаченной первой)\n"
                    "• В другое время: полная оплата\n\n"
                    "Возвращайтесь в указанные даты!")
        
        # Отправляем медиа если есть
        media_ids_list = []
        if media_file_ids:
            media_ids_list = [m.strip() for m in media_file_ids.split(",") if m.strip() and is_valid_file_id(m.strip())]
        elif media_file_id and is_valid_file_id(media_file_id.strip()):
            media_ids_list = [media_file_id.strip()]
        
        try:
            if len(media_ids_list) > 1:
                media_group = []
                valid_media_count = 0
                
                for m in media_ids_list[:10]:
                    if media_type == "photo":
                        media_group.append(types.InputMediaPhoto(m))
                        valid_media_count += 1
                    elif media_type == "video":
                        media_group.append(types.InputMediaVideo(m))
                        valid_media_count += 1
                
                if valid_media_count > 0:
                    if valid_media_count == 1:
                        if media_type == "photo":
                            bot.send_photo(chat_id, media_ids_list[0], caption=text, parse_mode="HTML", reply_markup=markup)
                        elif media_type == "video":
                            bot.send_video(chat_id, media_ids_list[0], caption=text, parse_mode="HTML", reply_markup=markup)
                    else:
                        bot.send_media_group(chat_id, media_group)
                        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
                else:
                    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
                    
            elif len(media_ids_list) == 1:
                m = media_ids_list[0]
                if media_type == "photo":
                    bot.send_photo(chat_id, m, caption=text, parse_mode="HTML", reply_markup=markup)
                elif media_type == "video":
                    bot.send_video(chat_id, m, caption=text, parse_mode="HTML", reply_markup=markup)
                else:
                    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
            else:
                bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
                
        except Exception as e:
            logging.exception("Error sending plan media")
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    
    else:
        # Если несколько групп - показываем список
        text = "📚 <b>Доступные группы обучения</b>\n\nВыберите группу для просмотра подробной информации и оплаты:"
        markup = types.InlineKeyboardMarkup()
        
        for r in rows:
            pid, title, price_cents, days, desc, media_file_id, media_type, media_file_ids, group_id, group_title = r
            markup.add(types.InlineKeyboardButton(f"{title}", 
                                                callback_data=f"select_plan:{pid}"))
        
        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)


def show_plan_full_info(chat_id, user_id, plan_id, show_back_button=True):
    """Показывает полную информацию о группе с медиа и кнопками оплаты"""
    # Проверяем существующую подписку
    has_active_sub, sub_info, message = check_existing_active_subscription(user_id, plan_id)
    
    # Получаем информацию о группе
    cursor.execute("SELECT title, price_cents, description, group_id FROM plans WHERE id=?", (plan_id,))
    plan = cursor.fetchone()
    if not plan:
        return False
    
    title, price_cents, description, group_id = plan
    
    # Если уже есть ПОЛНАЯ подписка за текущий месяц
    if has_active_sub and sub_info and sub_info[2] == 'full':
        sub_id, plan_id, part_paid, period_month, period_year, end_ts, invite_link, plan_title_existing, payment_type_existing = sub_info
        
        # Проверяем, что это текущий месяц
        current_month, current_year = get_current_period()
        if period_month == current_month and period_year == current_year:
            # Показываем информацию о существующей подписке
            text = (f"✅ <b>У вас уже есть активная подписка на группу '{plan_title_existing}'!</b>\n\n"
                    f"📊 Статус: Полностью оплачено за текущий месяц\n"
                    f"⏰ Действует до: {datetime.fromtimestamp(end_ts, LOCAL_TZ).strftime('%d.%m.%Y %H:%M')}\n\n")
            
            if invite_link and end_ts > int(time.time()):
                text += f"🔗 Ваша ссылка для входа:\n{invite_link}\n\n"
            
            # НЕ предлагаем продление, только информация
            text += f"ℹ️ <i>Следующая оплата будет доступна ближе к окончанию срока</i>"
            
            markup = types.InlineKeyboardMarkup()
            
            if show_back_button:
                markup.add(types.InlineKeyboardButton("🔙 Назад к списку групп", callback_data="back_to_plans"))
            
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
            return True
    
    # Получаем доступные варианты оплаты (уже с учетом существующих подписок)
    payment_options = get_payment_options(user_id, plan_id)
    
    text = (f"💳 <b>Оформление подписки на группу '{title}'</b>\n\n"
            f"💰 Цена в месяц: {price_str_from_cents(price_cents)}\n"
            f"📋 Описание: {description}\n\n")
    
    # Добавляем предупреждение о существующей подписке с первой частью
    if has_active_sub and sub_info and sub_info[2] == 'first':
        text += f"⚠️ <b>Внимание:</b> У вас уже оплачена первая часть за этот период!\n\n"
    
    markup = types.InlineKeyboardMarkup()
    
    if payment_options:
        text += "<b>Доступные варианты оплаты:</b>\n"
        for option in payment_options:
            text += f"• {option['text']}\n  {option['description']}\n\n"
        
        # Кнопки оплаты
        for option in payment_options:
            # Если у пользователя есть первая часть, НЕ показываем кнопку полной оплаты
            if has_active_sub and sub_info and sub_info[2] == 'first' and option['type'] in ('full', 'full_anytime'):
                continue  # Пропускаем кнопку полной оплаты
                
            markup.add(types.InlineKeyboardButton(
                f"💸 {option['text'].split(' - ')[0]} - {price_str_from_cents(option['price'])}", 
                callback_data=f"buy_{option['type']}:{plan_id}"
            ))
        
        # Кнопка для ввода промокода
        markup.add(types.InlineKeyboardButton("🎫 Оплатить с промокодом", callback_data=f"enter_promo_main:{plan_id}"))
        
    else:
        # Если нет доступных вариантов оплаты
        if has_active_sub and sub_info and sub_info[2] == 'first':
            text += "❌ <b>Сейчас не период оплаты второй части.</b>\n\n"
            text += ("💳 <b>Период оплаты второй части:</b> 15-20 числа\n\n"
                    "Возвращайтесь в указанные даты!")
        elif has_active_sub and sub_info and sub_info[2] == 'full':
            text += "✅ <b>У вас уже есть активная подписка на эту группу.</b>\n\n"
            text += ("ℹ️ Следующая оплата будет доступна ближе к окончанию срока.")
        else:
            text += "❌ <b>Сейчас не период оплаты.</b>\n\n"
            text += ("💳 <b>Периоды оплаты:</b>\n"
                    "• 1-5 числа: полная оплата или первая часть\n"
                    "• 15-20 числа: вторая часть (только при оплаченной первой)\n"
                    "• В другое время: полная оплата\n\n"
                    "Возвращайтесь в указанные даты!")
    
    if show_back_button:
        markup.add(types.InlineKeyboardButton("🔙 Назад к списку групп", callback_data="back_to_plans"))
    
    # Получаем медиа для этой группы
    cursor.execute("""
        SELECT media_file_id, media_type, media_file_ids 
        FROM plans 
        WHERE id=?
    """, (plan_id,))
    media_row = cursor.fetchone()
    
    # Отправка медиа
    if media_row:
        media_file_id, media_type, media_file_ids = media_row
        
        media_ids_list = []
        if media_file_ids:
            media_ids_list = [m.strip() for m in media_file_ids.split(",") if m.strip() and is_valid_file_id(m.strip())]
        elif media_file_id and is_valid_file_id(media_file_id.strip()):
            media_ids_list = [media_file_id.strip()]
        
        try:
            if len(media_ids_list) > 1:
                media_group = []
                valid_media_count = 0
                
                for m in media_ids_list[:10]:
                    if media_type == "photo":
                        media_group.append(types.InputMediaPhoto(m))
                        valid_media_count += 1
                    elif media_type == "video":
                        media_group.append(types.InputMediaVideo(m))
                        valid_media_count += 1
                
                if valid_media_count > 0:
                    if valid_media_count == 1:
                        if media_type == "photo":
                            bot.send_photo(chat_id, media_ids_list[0], caption=text, parse_mode="HTML", reply_markup=markup)
                        elif media_type == "video":
                            bot.send_video(chat_id, media_ids_list[0], caption=text, parse_mode="HTML", reply_markup=markup)
                    else:
                        bot.send_media_group(chat_id, media_group)
                        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
                else:
                    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
                    
            elif len(media_ids_list) == 1:
                m = media_ids_list[0]
                if media_type == "photo":
                    bot.send_photo(chat_id, m, caption=text, parse_mode="HTML", reply_markup=markup)
                elif media_type == "video":
                    bot.send_video(chat_id, m, caption=text, parse_mode="HTML", reply_markup=markup)
                else:
                    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
            else:
                bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
                
        except Exception as e:
            logging.exception("Error sending plan media")
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    else:
        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    
    return True

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("pay_second_part_from_sub:"))
def callback_pay_second_part_from_sub(call):
    """Обработка кнопки доплаты второй части из раздела подписок"""
    try:
        sub_id = int(call.data.split(":")[1])
        user_id = call.from_user.id
        
        # Получаем информацию о подписке
        cursor.execute("""
            SELECT s.plan_id, p.title, s.part_paid, s.current_period_month, s.current_period_year, p.price_cents
            FROM subscriptions s
            LEFT JOIN plans p ON s.plan_id = p.id
            WHERE s.id=? AND s.user_id=? AND s.active=1
        """, (sub_id, user_id))
        
        subscription = cursor.fetchone()
        
        if not subscription:
            bot.answer_callback_query(call.id, "❌ Подписка не найдена")
            return
        
        plan_id, plan_title, part_paid, period_month, period_year, price_cents = subscription
        
        # Проверяем, что оплачена только первая часть
        if part_paid != 'first':
            bot.answer_callback_query(call.id, "❌ У вас уже оплачена полная подписка")
            return
        
        # Проверяем текущий период
        current_month, current_year = get_current_period()
        if period_month != current_month or period_year != current_year:
            bot.answer_callback_query(call.id, "❌ Это подписка за другой период")
            return
        
        # Рассчитываем стоимость второй части
        second_part_price = price_cents // 2
        
        # Определяем тип оплаты в зависимости от текущей даты
        now = now_local()
        day = now.day
        
        if 15 <= day <= 20:
            payment_type = 'second_part'
            period_text = "в период 15-20 чисел"
        else:
            payment_type = 'second_part_late'
            period_text = "после 20 числа"
        
        # Сохраняем информацию для оплаты
        user_states[user_id] = {
            'plan_id': plan_id,
            'original_price': second_part_price,
            'title': plan_title,
            'description': "Доплата второй части",
            'group_id': None,  # Будет получено из плана
            'payment_type': payment_type,
            'mode': 'payment_method_selection'
        }
        
        # Получаем доступные способы оплаты
        payment_methods = get_active_payment_methods()
        if not payment_methods:
            bot.answer_callback_query(call.id, "❌ Нет доступных способов оплаты")
            return
        
        text = (f"💳 <b>Доплата второй части для группы '{plan_title}'</b>\n\n"
                f"📊 Статус: Первая часть оплачена\n"
                f"💰 Стоимость второй части: {price_str_from_cents(second_part_price)}\n"
                f"⏰ {period_text}\n\n"
                f"После оплаты доступ будет продлен до 5 числа следующего месяца.")
        
        markup = types.InlineKeyboardMarkup()
        
        # ЕСЛИ СПОСОБ ОПЛАТЫ ВСЕГО ОДИН - СРАЗУ ПЕРЕХОДИМ К НЕМУ
        if len(payment_methods) == 1:
            method_id, name, mtype, method_desc, details = payment_methods[0]
            
            if mtype == "card":
                # Нужно получить group_id
                cursor.execute("SELECT group_id FROM plans WHERE id=?", (plan_id,))
                plan_group = cursor.fetchone()
                group_id = plan_group[0] if plan_group else None
                
                if not group_id:
                    group_id = get_default_group()
                
                # Сразу создаем счет
                process_card_payment(call, plan_id, call.from_user, plan_title, second_part_price, 
                                   "Доплата второй части", group_id, payment_type)
            else:
                # Сразу переходим к инструкциям
                process_manual_payment_start(call, plan_id, call.from_user, plan_title, second_part_price, 
                                           "Доплата второй части", details, payment_type)
            return
        
        # ЕСЛИ СПОСОБОВ НЕСКОЛЬКО - показываем выбор
        for method_id, name, mtype, method_desc, details in payment_methods:
            markup.add(types.InlineKeyboardButton(name, callback_data=f"paymethod_second_part:{sub_id}:{method_id}:{payment_type}"))
        
        markup.add(types.InlineKeyboardButton("🎫 Ввести промокод", callback_data=f"enter_promo_second:{sub_id}:{payment_type}"))
        markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_my_subscriptions"))
        
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, 
                             parse_mode="HTML", reply_markup=markup)
        bot.answer_callback_query(call.id, "💳 Выберите способ оплаты")
        
    except Exception as e:
        logging.exception("Error in callback_pay_second_part_from_sub")
        bot.answer_callback_query(call.id, "❌ Ошибка")

def get_plan_price(plan_id):
    """Возвращает цену плана в центах"""
    cursor.execute("SELECT price_cents FROM plans WHERE id=?", (plan_id,))
    result = cursor.fetchone()
    return result[0] if result else 0

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("paymethod_second_part:"))
def callback_paymethod_second_part(call):
    """Обработка выбора способа оплаты для второй части"""
    try:
        parts = call.data.split(":")
        sub_id = int(parts[1])
        method_id = int(parts[2])
        payment_type = parts[3]
        
        user = call.from_user
        
        # Получаем информацию о подписке
        cursor.execute("""
            SELECT s.plan_id, p.title, p.price_cents, p.description, p.group_id
            FROM subscriptions s
            LEFT JOIN plans p ON s.plan_id = p.id
            WHERE s.id=? AND s.user_id=? AND s.active=1
        """, (sub_id, user.id))
        
        subscription = cursor.fetchone()
        
        if not subscription:
            bot.answer_callback_query(call.id, "❌ Подписка не найдена")
            return
        
        plan_id, title, price_cents, description, group_id = subscription
        
        # Рассчитываем цену второй части
        amount_cents = price_cents // 2
        
        method = get_payment_method_by_id(method_id)
        if not method:
            bot.answer_callback_query(call.id, "❌ Способ оплаты не найден.")
            return
            
        method_id, name, mtype, method_desc, details = method
        
        if mtype == "card":
            process_card_payment(call, plan_id, user, title, amount_cents, 
                               "Доплата второй части", group_id, payment_type)
        else:  # manual
            process_manual_payment_start(call, plan_id, user, title, amount_cents, 
                                       "Доплата второй части", details, payment_type)
            
    except Exception as e:
        logging.exception("Error in callback_paymethod_second_part")
        bot.answer_callback_query(call.id, "❌ Ошибка при выборе способа оплаты")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("enter_promo_second:"))
def callback_enter_promo_second(call):
    try:
        parts = call.data.split(":")
        sub_id = int(parts[1])
        payment_type = parts[2]
        
        user = call.from_user
        
        # Получаем информацию о подписке
        cursor.execute("""
            SELECT s.plan_id, p.title, p.price_cents
            FROM subscriptions s
            LEFT JOIN plans p ON s.plan_id = p.id
            WHERE s.id=? AND s.user_id=? AND s.active=1
        """, (sub_id, user.id))
        
        subscription = cursor.fetchone()
        
        if not subscription:
            bot.answer_callback_query(call.id, "❌ Подписка не найдена")
            return
        
        plan_id, title, price_cents = subscription
        amount_cents = price_cents // 2
        
        # Сохраняем состояние
        user_states[user.id] = {
            'plan_id': plan_id,
            'sub_id': sub_id,
            'original_price': amount_cents,
            'title': title,
            'payment_type': payment_type,
            'mode': 'promo_input_second_part'
        }
        
        bot.answer_callback_query(call.id, "🎫 Введите промокод")
        bot.send_message(call.message.chat.id, 
                        f"🎫 Введите промокод для доплаты второй части группы '{title}':\n\n"
                        f"💰 Сумма к оплате: {price_str_from_cents(amount_cents)}\n\n"
                        f"Введите промокод или отправьте /cancel для отмены")
        
    except Exception as e:
        logging.exception("Error in callback_enter_promo_second")
        bot.answer_callback_query(call.id, "❌ Ошибка")

@bot.callback_query_handler(func=lambda call: call.data == "back_to_my_subscriptions")
def callback_back_to_my_subscriptions(call):
    """Возврат к списку подписок"""
    try:
        message = type('Message', (), {'chat': type('Chat', (), {'id': call.message.chat.id}), 
                                       'from_user': type('User', (), {'id': call.from_user.id})})()
        show_my_subscription(message)
    except:
        pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("enter_promo_main:"))
def callback_enter_promo_main(call):
    try:
        pid = int(call.data.split(":")[1])
        user = call.from_user
        
        # Получаем полную информацию о группе
        cursor.execute("SELECT title, price_cents, description, group_id FROM plans WHERE id=?", (pid,))
        plan = cursor.fetchone()
        if not plan:
            bot.answer_callback_query(call.id, "❌ Группа не найдена.")
            return
            
        title, price_cents, description, group_id = plan
        
        logging.info(f"🔍 DEBUG enter_promo_main: plan_id={pid}, group_id={group_id}")
        
        # Сохраняем ВСЮ информацию для возврата
        user_states[user.id] = {
            'plan_id': pid,
            'title': title,
            'description': description,
            'original_price': price_cents,
            'group_id': group_id,  # ⚠️ ВАЖНО: сохраняем group_id
            'mode': 'promo_input_main',
            'message_id': call.message.message_id
        }
        
        bot.answer_callback_query(call.id, "🎫 Введите промокод")
        
        # Создаем клавиатуру для отмены
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(types.KeyboardButton("❌ Отмена"))
        
        bot.send_message(call.message.chat.id, 
                        f"🎫 <b>Введите промокод для группы '{title}'</b>\n\n"
                        f"💰 Исходная цена: {price_str_from_cents(price_cents)}\n\n"
                        f"Введите промокод или нажмите '❌ Отмена'",
                        parse_mode="HTML", 
                        reply_markup=markup)
        
    except Exception as e:
        logging.exception("Error in callback_enter_promo_main")
        bot.answer_callback_query(call.id, "❌ Ошибка")

def is_valid_file_id(file_id):
    """Проверяет валидность file_id"""
    if not file_id or not isinstance(file_id, str):
        return False
    # file_id обычно состоит из букв, цифр и некоторых символов
    # Минимальная длина file_id обычно больше 10 символов
    if len(file_id) < 10:
        return False
    # Проверяем на наличие только допустимых символов
    import re
    pattern = r'^[A-Za-z0-9_-]+$'
    return bool(re.match(pattern, file_id))

@bot.message_handler(func=lambda message: message.text == "💰 Баланс")
@only_private
def show_balance(message):
    uid = message.from_user.id
    cursor.execute("SELECT cashback_cents FROM users WHERE user_id=?", (uid,))
    r = cursor.fetchone()
    bal = r[0] if r else 0
    bot.send_message(message.chat.id, f"💰 Ваш баланс кэшбэка: {price_str_from_cents(bal)}")

@bot.message_handler(func=lambda message: message.text == "👥 Реферальная ссылка")
@only_private
def show_ref(message):
    uid = message.from_user.id
    bot_username = bot.get_me().username
    link = f"https://t.me/{bot_username}?start=ref{uid}"
    bot.send_message(message.chat.id, f"👥 Ваша реферальная ссылка:\n\n{link}\n\n💡 Делитесь и получайте {REFERRAL_PERCENT}% кэшбэка!")


@bot.message_handler(func=lambda message: message.text == "🎫 Мои подписки")
@only_private
def show_my_subscription(message):
    uid = message.from_user.id
    cursor.execute("""
        SELECT s.id, s.plan_id, s.start_ts, s.end_ts, s.active, s.invite_link, p.title, 
               s.payment_type, s.part_paid, s.current_period_month, s.current_period_year,
               p.price_cents, s.group_id
        FROM subscriptions s
        LEFT JOIN plans p ON s.plan_id = p.id
        WHERE s.user_id=? AND s.active=1
        ORDER BY s.end_ts DESC
    """, (uid,))
    rows = cursor.fetchall()
    
    if not rows:
        bot.send_message(uid, "📭 У вас нет активных подписок.")
        return
    
    current_month, current_year = get_current_period()
    now_ts = int(time.time())
    
    for row in rows:
        sid, pid, start_ts, end_ts, active, invite_link, title, payment_type, part_paid, period_month, period_year, price_cents, group_id = row
        
        status_text = ""
        needs_renewal = False
        can_pay_second_part = False
        
        if period_month == current_month and period_year == current_year:
            if part_paid == 'full':
                status_text = "✅ Оплачено полностью"
            elif part_paid == 'first':
                status_text = "⏳ Ожидает вторую часть оплаты"
                # Проверяем, можем ли предложить доплатить вторую часть
                now = now_local()
                day = now.day
                # Проверяем период оплаты второй части (15-20 числа)
                if 15 <= day <= 20:
                    can_pay_second_part = True
                # Или если пропустили период, но можем восстановить доступ
                elif day > 20:
                    can_pay_second_part = True
                # Или если еще не начался период второй части
                elif day < 15:
                    status_text = "⏳ Первая часть оплачена. Вторая часть оплачивается 15-20 числа"
            else:
                status_text = "❌ Не оплачено"
                needs_renewal = True
        else:
            # Проверяем, истекла ли подписка
            if end_ts < now_ts:
                status_text = "⏰ Подписка истекла"
            else:
                status_text = "📅 Требуется оплата за новый месяц"
            needs_renewal = True
        
        # Форматируем дату окончания
        end_date_str = datetime.fromtimestamp(end_ts, LOCAL_TZ).strftime('%d.%m.%Y %H:%M')
        
        # Формируем текст
        txt = (f"🎫 Группа: <b>{title or pid}</b>\n"
               f"💳 Тип оплаты: {'Двумя частями' if payment_type == 'partial' else 'Полная'}\n"
               f"📊 Статус: {status_text}\n"
               f"⏰ Действует до: {end_date_str}\n"
               f"💰 Часть оплаты: {part_paid}")
        
        # Добавляем информацию о ссылке, если она есть
        if invite_link and active and end_ts > now_ts:
            txt += f"\n\n🔗 Ваша пригласительная ссылка:\n{invite_link}"
        
        markup = types.InlineKeyboardMarkup()
        
        if needs_renewal:
            # Кнопка для продления подписки
            markup.add(types.InlineKeyboardButton("🔄 Продлить подписку", callback_data=f"renew:{pid}"))
        elif invite_link and active and end_ts > now_ts:
            # Кнопка для получения ссылки, если подписка активна
            markup.add(types.InlineKeyboardButton("🔗 Получить ссылку", callback_data=f"get_link:{sid}"))
        
        # Добавляем кнопку для доплаты второй части
        if  part_paid == 'first':
            markup.add(types.InlineKeyboardButton("💳 Доплатить вторую часть", callback_data=f"pay_second_part_from_sub:{sid}"))
        
        bot.send_message(uid, txt, parse_mode="HTML", reply_markup=markup)

def check_existing_active_subscription(user_id, plan_id):
    """
    Проверяет, есть ли у пользователя активная подписка на этот план
    Возвращает (has_active_sub, sub_info, message)
    """
    current_month, current_year = get_current_period()
    now_ts = int(time.time())
    
    # Проверяем активные подписки
    cursor.execute("""
        SELECT s.id, s.plan_id, s.part_paid, s.current_period_month, s.current_period_year, 
               s.end_ts, s.invite_link, p.title, s.payment_type
        FROM subscriptions s
        LEFT JOIN plans p ON s.plan_id = p.id
        WHERE s.user_id=? AND s.plan_id=? AND s.active=1
        ORDER BY s.end_ts DESC
        LIMIT 1
    """, (user_id, plan_id))
    
    existing_sub = cursor.fetchone()
    
    if not existing_sub:
        return False, None, "Нет активной подписки"
    
    sub_id, plan_id, part_paid, period_month, period_year, end_ts, invite_link, plan_title, payment_type = existing_sub
    
    # Если подписка все еще активна по времени
    if end_ts > now_ts:
        # Если это текущий период
        if period_month == current_month and period_year == current_year:
            if part_paid == 'full':
                return True, existing_sub, f"У вас уже есть активная подписка на группу '{plan_title}' за текущий месяц!"
            elif part_paid == 'first':
                return True, existing_sub, f"У вас уже оплачена первая часть за группу '{plan_title}'. Вы можете доплатить вторую часть."
        else:
            # Подписка активна, но не за текущий период
            return True, existing_sub, f"У вас есть активная подписка на группу '{plan_title}', но за другой период."
    
    return False, existing_sub, "Подписка истекла"

# ----------------- Payment callbacks ----------------
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("select_plan:"))
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("select_plan:"))
def callback_select_plan(call):
    try:
        user = call.from_user
        pid = int(call.data.split(":")[1])
        
        # Просто показываем полную информацию о группе
        success = show_plan_full_info(call.message.chat.id, user.id, pid, show_back_button=True)
        
        if not success:
            bot.answer_callback_query(call.id, "❌ Группа не найдена.")
        else:
            bot.answer_callback_query(call.id)
        
    except Exception as e:
        logging.exception("Error in callback_select_plan")
        bot.answer_callback_query(call.id, "❌ Ошибка при выборе группы")
        
@bot.callback_query_handler(func=lambda call: call.data == "back_to_plans")
def callback_back_to_plans(call):
    """Возврат к списку групп"""
    try:
           show_plans(call.message)
    except:
        pass
    bot.answer_callback_query(call.id)

# Обработчики покупки
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("buy_"))
def callback_buy_handler(call):
    try:
        user = call.from_user
        
        # Парсим callback data в формате "buy_full:123" или "buy_partial:456"
        callback_data = call.data
        
        # Разделяем на часть до : и после :
        if ":" not in callback_data:
            bot.answer_callback_query(call.id, "❌ Ошибка в данных.")
            return
            
        buy_part, pid_str = callback_data.split(":", 1)
        payment_type = buy_part.replace("buy_", "")
        
        try:
            pid = int(pid_str)
        except ValueError:
            bot.answer_callback_query(call.id, "❌ Неверный ID группы.")
            return
        
        # Получаем информацию о группе
        cursor.execute("SELECT title, price_cents, description, group_id FROM plans WHERE id=?", (pid,))
        plan = cursor.fetchone()
        if not plan:
            bot.answer_callback_query(call.id, "❌ Группа не найдена.")
            return
        title, price_cents, description, group_id = plan
        
        # Проверяем существующую активную подписку
        has_active_sub, sub_info, message = check_existing_active_subscription(user.id, pid)
        
        # Если уже есть ПОЛНАЯ подписка за текущий месяц
        if has_active_sub and sub_info and sub_info[2] == 'full':
            sub_id, plan_id, part_paid, period_month, period_year, end_ts, invite_link, plan_title_existing, payment_type_existing = sub_info
            
            # Проверяем, что это текущий месяц
            current_month, current_year = get_current_period()
            if period_month == current_month and period_year == current_year:
                bot.answer_callback_query(call.id, "✅ Вы уже оплатили текущий месяц")
                
                text = (f"✅ <b>У вас уже оплачен текущий месяц в группе '{plan_title_existing}'!</b>\n\n"
                        f"⏰ Подписка активна до: {datetime.fromtimestamp(end_ts, LOCAL_TZ).strftime('%d.%m.%Y %H:%M')}\n\n")
                
                if invite_link and end_ts > int(time.time()):
                    text += f"🔗 Ваша ссылка для входа:\n{invite_link}\n\n"
                
                text += "ℹ️ Следующая оплата будет доступна ближе к окончанию срока."
                
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("🔙 Назад к группам", callback_data="back_to_plans"))
                markup.add(types.InlineKeyboardButton("🎫 Мои подписки", callback_data="show_my_subscriptions"))
                
                bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
                return
        
        # Если есть активная подписка с ПЕРВОЙ частью
        elif has_active_sub and sub_info and sub_info[2] == 'first':
            sub_id, plan_id, part_paid, period_month, period_year, end_ts, invite_link, plan_title_existing, payment_type_existing = sub_info
            
            # Проверяем, что это текущий месяц
            current_month, current_year = get_current_period()
            if period_month == current_month and period_year == current_year:
                # Пользователь пытается купить ПЕРВУЮ часть еще раз
                if payment_type == 'partial':
                    bot.answer_callback_query(call.id, "❌ У вас уже оплачена первая часть")
                    return
                
                # Пользователь пытается купить ПОЛНУЮ
                elif payment_type in ('full', 'full_anytime'):
                    # Перенаправляем на доплату второй части
                    bot.answer_callback_query(call.id, "⚠️ У вас уже есть частичная оплата")
                    
                    text = (f"⚠️ <b>У вас уже оплачена первая часть за группу '{plan_title_existing}'</b>\n\n"
                            f"💵 <b>Вы можете доплатить только вторую часть:</b> {price_str_from_cents(price_cents // 2)}\n"
                            f"⏰ После оплаты доступ будет до 5 числа следующего месяца.")
                    
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton(
                        f"💳 Доплатить вторую часть", 
                        callback_data=f"pay_second_part_from_sub:{sub_id}"
                    ))
                    markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_plans"))
                    
                    bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
                    return
        
        # Рассчитываем цену в зависимости от типа оплаты
        if payment_type in ('partial', 'second_part', 'half_month', 'second_part_late'):
            amount_cents = price_cents // 2
        else:  # full или full_anytime
            amount_cents = price_cents
        
        # Сохраняем информацию о выбранном тарифе
        user_states[user.id] = {
            'plan_id': pid,
            'original_price': amount_cents,
            'title': title,
            'description': description,
            'group_id': group_id,
            'payment_type': payment_type,
            'mode': 'payment_method_selection'
        }
        
        payment_type_text = {
            'full': 'полной',
            'full_anytime': 'полной', 
            'partial': 'первой части',
            'second_part': 'второй части',
            'half_month': 'половины месяца',
            'second_part_late': 'второй части'
        }.get(payment_type, '')
        
        # Получаем доступные способы оплаты
        payment_methods = get_active_payment_methods()
        if not payment_methods:
            bot.answer_callback_query(call.id, "❌ Нет доступных способов оплаты")
            return
            
        # ЕСЛИ СПОСОБ ОПЛАТЫ ВСЕГО ОДИН - СРАЗУ ПЕРЕХОДИМ К НЕМУ
        if len(payment_methods) == 1:
            method_id, name, mtype, method_desc, details = payment_methods[0]
            
            if mtype == "card":
                # Если только карта - сразу создаем счет
                process_card_payment(call, pid, user, title, amount_cents, description, group_id, payment_type)
            else:
                # Если только ручная оплата - сразу переходим к инструкциям
                process_manual_payment_start(call, pid, user, title, amount_cents, description, details, payment_type)
            return
            
        # ЕСЛИ СПОСОБОВ НЕСКОЛЬКО - показываем выбор
        text = (f"💳 <b>Оплата {payment_type_text} группы '{title}'</b>\n\n"
                f"💰 Сумма: {price_str_from_cents(amount_cents)}\n\n")
        
        # Добавляем информацию о существующей подписке, если есть
        if has_active_sub and sub_info and sub_info[2] == 'first':
            text += f"ℹ️ <i>У вас уже оплачена первая часть за этот период</i>\n\n"
        
        text += "Выберите способ оплаты:"
        
        markup = types.InlineKeyboardMarkup()
        
        # Кнопки способов оплаты
        for method_id, name, mtype, method_desc, details in payment_methods:
            markup.add(types.InlineKeyboardButton(name, callback_data=f"paymethod:{pid}:{method_id}:{payment_type}"))
        
        # Кнопка для ввода промокода
        markup.add(types.InlineKeyboardButton("🎫 Ввести промокод", callback_data=f"enter_promo:{pid}:{payment_type}"))
        
        bot.answer_callback_query(call.id, "💳 Выберите способ оплаты")
        bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
        
    except Exception as e:
        logging.exception("Error in callback_buy_handler")
        bot.answer_callback_query(call.id, "❌ Ошибка при оформлении заказа")


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("buy_full_override:"))
def callback_buy_full_override(call):
    """Обработка покупки полной суммы при наличии первой части"""
    try:
        pid = int(call.data.split(":")[1])
        user = call.from_user
        
        # Продолжаем как обычную покупку полной суммы
        callback_data = f"buy_full:{pid}"
        
        # Создаем искусственный callback с нужными данными
        class FakeCall:
            def __init__(self):
                self.data = callback_data
                self.id = call.id
                self.message = call.message
                self.from_user = call.from_user
        
        fake_call = FakeCall()
        callback_buy_handler(fake_call)
        
    except Exception as e:
        logging.exception("Error in callback_buy_full_override")
        bot.answer_callback_query(call.id, "❌ Ошибка")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("buy_second_part:"))
def callback_buy_second_part(call):
    """Обработка покупки только второй части"""
    try:
        pid = int(call.data.split(":")[1])
        user = call.from_user
        
        # Определяем какой тип второй части использовать
        now = now_local()
        day = now.day
        
        if 15 <= day <= 20:
            payment_type = 'second_part'
        else:
            payment_type = 'second_part_late'
        
        # Продолжаем как покупку второй части
        callback_data = f"buy_{payment_type}:{pid}"
        
        # Создаем искусственный callback с нужными данными
        class FakeCall:
            def __init__(self):
                self.data = callback_data
                self.id = call.id
                self.message = call.message
                self.from_user = call.from_user
        
        fake_call = FakeCall()
        callback_buy_handler(fake_call)
        
    except Exception as e:
        logging.exception("Error in callback_buy_second_part")
        bot.answer_callback_query(call.id, "❌ Ошибка")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("enter_promo:"))
def callback_enter_promo(call):
    try:
        parts = call.data.split(":")
        pid = int(parts[1])
        payment_type = parts[2]
        
        user = call.from_user
        
        if user.id not in user_states:
            bot.answer_callback_query(call.id, "❌ Сессия устарела")
            return
            
        state = user_states[user.id]
        state['mode'] = 'promo_input'
        
        bot.answer_callback_query(call.id, "🎫 Введите промокод")
        bot.send_message(call.message.chat.id, 
                        f"🎫 Введите промокод для группы '{state['title']}':\n\n"
                        f"Или отправьте /cancel для отмены")
        
    except Exception as e:
        logging.exception("Error in callback_enter_promo")
        bot.answer_callback_query(call.id, "❌ Ошибка")

@bot.callback_query_handler(func=lambda call: call.data == "show_my_subscriptions")
def callback_show_my_subscriptions(call):
    """Показывает подписки из callback"""
    try:
        # Создаем искусственное сообщение
        class FakeMessage:
            def __init__(self, chat_id, user_id):
                self.chat = type('obj', (object,), {'id': chat_id})()
                self.from_user = type('obj', (object,), {'id': user_id})()
                self.text = "🎫 Мои подписки"
        
        fake_message = FakeMessage(call.message.chat.id, call.from_user.id)
        show_my_subscription(fake_message)
    except Exception as e:
        logging.error(f"Error showing subscriptions: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка при загрузке подписок")

# Обработчик пропуска промокода
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("skip_promo:"))
def callback_skip_promo(call):
    try:
        user = call.from_user
        parts = call.data.split(":")
        pid = int(parts[1])
        payment_type = parts[2]
        
        if user.id not in user_states:
            bot.answer_callback_query(call.id, "❌ Сессия устарела")
            return
            
        state = user_states[user.id]
        state['mode'] = 'no_promo'
        
        # Получаем доступные способы оплаты
        payment_methods = get_active_payment_methods()
        if not payment_methods:
            bot.answer_callback_query(call.id, "❌ Нет доступных способов оплаты")
            return
            
        # ЕСЛИ СПОСОБ ОПЛАТЫ ВСЕГО ОДИН - СРАЗУ ПЕРЕХОДИМ К НЕМУ
        if len(payment_methods) == 1:
            method_id, name, mtype, method_desc, details = payment_methods[0]
            
            if mtype == "card":
                process_card_payment(call, pid, user, state['title'], state['original_price'], state['description'], state['group_id'], payment_type)
            else:
                process_manual_payment_start(call, pid, user, state['title'], state['original_price'], state['description'], details, payment_type)
        else:
            # ЕСЛИ СПОСОБОВ НЕСКОЛЬКО - показываем выбор
            markup = types.InlineKeyboardMarkup()
            for method_id, name, mtype, method_desc, details in payment_methods:
                markup.add(types.InlineKeyboardButton(name, callback_data=f"paymethod:{pid}:{method_id}:{payment_type}"))
            
            bot.answer_callback_query(call.id, "💳 Выберите способ оплаты")
            bot.send_message(call.message.chat.id, f"💳 <b>Выберите способ оплаты для группы '{state['title']}'</b>", parse_mode="HTML", reply_markup=markup)
            
    except Exception as e:
        logging.exception("Error in callback_skip_promo")
        bot.answer_callback_query(call.id, "❌ Ошибка при оформлении заказа")

# Обработчик ввода промокода
@bot.message_handler(func=lambda m: m.from_user.id in user_states and 
                    user_states[m.from_user.id].get('mode') in ['promo_input', 'promo_input_main'] and 
                    m.text and not m.text.startswith('/'))
def handle_promo_code_input(message):
    user_id = message.from_user.id
    state = user_states[user_id]
    mode = state.get('mode')
    
    # Обработка отмены
    if message.text.strip() == "❌ Отмена":
        # Убираем клавиатуру отмены
        markup = types.ReplyKeyboardRemove()
        bot.send_message(message.chat.id, "❌ Ввод промокода отменен.", reply_markup=markup)
        
        bot.send_message(message.chat.id, "📋 Главное меню:", reply_markup=main_menu(user_id))
        

        if mode == 'promo_input_main':
            # Возвращаемся к просмотру группы
            show_plan_full_info(message.chat.id, user_id, state['plan_id'], show_back_button=True)
        else:
            # Возвращаемся к выбору способа оплаты
            show_payment_methods(message.chat.id, user_id, state)
        
        user_states.pop(user_id, None)
        return
    
    promo_code = message.text.strip().upper()
    
    # Проверяем промокод
    promo_data = get_promo_code(promo_code)
    if not promo_data:
        bot.send_message(message.chat.id, "❌ Промокод не найден. Попробуйте другой или нажмите '❌ Отмена'.")
        return
        
    can_use, reason = can_use_promo_code(promo_data[0], user_id)
    if not can_use:
        bot.send_message(message.chat.id, f"❌ {reason}\n\nПопробуйте другой промокод или нажмите '❌ Отмена'.")
        return
        
    # Применяем промокод
    new_price, promo_message = apply_promo_code(state['original_price'], promo_data)
    
    # ВАЖНО: Получаем актуальную информацию о группе
    cursor.execute("SELECT group_id FROM plans WHERE id=?", (state['plan_id'],))
    plan_data = cursor.fetchone()
    if not plan_data:
        bot.send_message(message.chat.id, "❌ Ошибка: группа не найдена.")
        return
        
    group_id = plan_data[0]
    
    # Обновляем состояние ВСЕМИ необходимыми данными
    state.update({
        'promo_id': promo_data[0],
        'promo_code': promo_code,
        'final_price': new_price,
        'group_id': group_id,  # ⚠️ ЭТОГО НЕ БЫЛО!
        'mode': 'promo_applied'
    })
    
    # Убираем клавиатуру отмены
    markup = types.ReplyKeyboardRemove()
    bot.send_message(message.chat.id, f"✅ {promo_message}", reply_markup=markup)
    
    if mode == 'promo_input_main':
        # Переходим к выбору типа оплаты с примененным промокодом
        state['mode'] = 'payment_method_with_promo'
        show_payment_options_with_promo(message.chat.id, user_id, state)
    else:
        # Показываем способы оплаты с примененным промокодом
        state['mode'] = 'promo_applied'
        show_payment_methods_with_promo(message.chat.id, user_id, state)

def show_payment_methods_with_promo(chat_id, user_id, state):
    """Показывает способы оплаты с примененным промокодом"""
    payment_methods = get_active_payment_methods()
    if not payment_methods:
        bot.send_message(chat_id, "❌ Нет доступных способов оплаты")
        return
        
    # ЕСЛИ СПОСОБ ОПЛАТЫ ВСЕГО ОДИН - СРАЗУ ПЕРЕХОДИМ К НЕМУ
    if len(payment_methods) == 1:
        method_id, name, mtype, method_desc, details = payment_methods[0]
        
        if mtype == "card":
            # Создаем фиктивный call объект для process_card_payment
            class FakeCall:
                def __init__(self, chat_id):
                    self.message = type('Message', (), {'chat': type('Chat', (), {'id': chat_id})})()
                    self.id = "fake_call"
            
            fake_call = FakeCall(chat_id)
            process_card_payment(fake_call, state['plan_id'], type('User', (), {'id': user_id})(), 
                               state['title'], state['final_price'], state['description'], 
                               state['group_id'], state['payment_type'], state['promo_id'])
        else:
            # Для ручной оплаты отправляем сообщение с инструкциями
            process_manual_payment_start_from_message(
                type('Message', (), {'chat': type('Chat', (), {'id': chat_id}), 'from_user': type('User', (), {'id': user_id})})(),
                state['plan_id'], state['title'], state['final_price'], state['description'], 
                details, state['payment_type'], state['promo_id']
            )
        return
        
    # ЕСЛИ СПОСОБОВ НЕСКОЛЬКО - показываем выбор
    payment_type_text = get_payment_type_text(state['payment_type'])
    
    text = (f"💳 <b>Оплата {payment_type_text} группы '{state['title']}'</b>\n\n"
            f"💰 Исходная цена: {price_str_from_cents(state['original_price'])}\n"
            f"🎫 Промокод применен: {state['promo_code']}\n"
            f"💵 Итоговая цена: {price_str_from_cents(state['final_price'])}\n\n"
            f"Выберите способ оплаты:")
    
    markup = types.InlineKeyboardMarkup()
    
    for method_id, name, mtype, method_desc, details in payment_methods:
        markup.add(types.InlineKeyboardButton(name, callback_data=f"paymethod_promo:{state['plan_id']}:{method_id}:{state['payment_type']}:{state['promo_id']}"))
    
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    
def show_payment_options_with_promo(chat_id, user_id, state):
    """Показывает варианты оплаты с примененным промокодом"""
    plan_id = state['plan_id']
    
    logging.info(f"🔍 DEBUG show_payment_options_with_promo: plan_id={plan_id}, user_id={user_id}")
    logging.info(f"🔍 DEBUG Current state: {state}")
    
    # ВАЖНО: Получаем актуальную информацию о группе
    cursor.execute("SELECT title, price_cents, description, group_id FROM plans WHERE id=?", (plan_id,))
    plan = cursor.fetchone()
    if not plan:
        logging.error(f"🚫 Plan {plan_id} not found in database")
        bot.send_message(chat_id, "❌ Группа не найдена")
        return
        
    title, price_cents, description, group_id = plan
    
    logging.info(f"🔍 DEBUG Plan data from DB: title={title}, group_id={group_id}")
    
    # Обновляем состояние актуальными данными
    state.update({
        'title': title,
        'description': description,
        'group_id': group_id,
        'original_price': price_cents
    })
    
    # Получаем доступные варианты оплаты
    payment_options = get_payment_options(user_id, plan_id)
    
    text = (f"💳 <b>Оформление подписки на группу '{title}'</b>\n\n"
            f"💰 Исходная цена: {price_str_from_cents(state['original_price'])}\n"
            f"🎫 Промокод применен: {state['promo_code']}\n"
            f"💵 Итоговая цена: {price_str_from_cents(state['final_price'])}\n\n")
    
    markup = types.InlineKeyboardMarkup()
    
    if payment_options:
        text += "<b>Детали</b>\n"
        
        # Получаем доступные способы оплаты
        payment_methods = get_active_payment_methods()
        if not payment_methods:
            bot.send_message(chat_id, "❌ Нет доступных способов оплаты")
            return
            
        # ЕСЛИ СПОСОБ ОПЛАТЫ ВСЕГО ОДИН - СРАЗУ ПЕРЕХОДИМ К НЕМУ
        if len(payment_methods) == 1:
            method_id, name, mtype, method_desc, details = payment_methods[0]
            
            # Для каждого варианта оплаты создаем отдельную кнопку
            for option in payment_options:
                # Пересчитываем цену с учетом промокода для каждого варианта
                if option['type'] in ('partial', 'second_part', 'half_month', 'second_part_late'):
                    discounted_price = state['final_price'] // 2
                else:
                    discounted_price = state['final_price']
                
                text += f"• {option['text']} → {price_str_from_cents(discounted_price)}\n  {option['description']}\n\n"
                
                # Сразу переходим к оплате для единственного способа
                if mtype == "card":
                    # Создаем callback data для прямой оплаты картой
                    callback_data = f"buy_with_promo:{option['type']}:{plan_id}:{state['promo_id']}"
                    markup.add(types.InlineKeyboardButton(
                        f"💳 Оплатить {price_str_from_cents(discounted_price)}", 
                        callback_data=callback_data
                    ))
                else:
                    # Для ручной оплаты создаем callback data для перехода к инструкциям
                    callback_data = f"paymethod_promo:{plan_id}:{method_id}:{option['type']}:{state['promo_id']}"
                    markup.add(types.InlineKeyboardButton(
                        f"💳 Оплатить {price_str_from_cents(discounted_price)}", 
                        callback_data=callback_data
                    ))
        else:
            # ЕСЛИ СПОСОБОВ НЕСКОЛЬКО - создаем кнопки с выбором типа оплаты
            for option in payment_options:
                # Пересчитываем цену с учетом промокода для каждого варианта
                if option['type'] in ('partial', 'second_part', 'half_month', 'second_part_late'):
                    discounted_price = state['final_price'] // 2
                else:
                    discounted_price = state['final_price']
                    
                text += f"• {option['text']} → {price_str_from_cents(discounted_price)}\n  {option['description']}\n\n"
                
                # Создаем кнопку которая ведет к выбору способа оплаты
                callback_data = f"buy_with_promo:{option['type']}:{plan_id}:{state['promo_id']}"
                logging.info(f"🔍 DEBUG Creating payment button: {callback_data}")
                
                markup.add(types.InlineKeyboardButton(
                    f"💳 Оплатить {price_str_from_cents(discounted_price)}", 
                    callback_data=callback_data
                ))
        
    else:
        active_type = get_active_payment_type()
        if active_type == 'second':
            text += "❌ <b>У вас нет активной первой части оплаты для этой группы.</b>\n\n"
        else:
            text += "❌ <b>Сейчас не период оплаты.</b>\n\n"
        
        text += ("💳 <b>Периоды оплаты:</b>\n"
                "• 1-5 числа: полная оплата или первая часть\n"
                "• 15-20 числа: вторая часть (только при оплаченной первой)\n"
                "• В другое время: полная оплата\n\n"
                "Возвращайтесь в указанные даты!")
    
    markup.add(types.InlineKeyboardButton("🔙 Назад к группе", callback_data=f"select_plan:{plan_id}"))
    
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("buy_with_promo:"))
def callback_buy_with_promo(call):
    try:
        user = call.from_user
        
        logging.info(f"🎯 buy_with_promo HANDLER TRIGGERED: {call.data}")
        
        # Парсим callback data в формате "buy_with_promo:full:123:456"
        callback_data = call.data
        
        if ":" not in callback_data:
            logging.error("🚫 No colon in callback data")
            bot.answer_callback_query(call.id, "❌ Ошибка в данных.")
            return
            
        parts = callback_data.split(":")
        if len(parts) < 4:
            logging.error(f"🚫 Invalid callback data format: {callback_data}, parts: {parts}")
            bot.answer_callback_query(call.id, "❌ Ошибка в данных.")
            return
            
        payment_type = parts[1]  # "full", "partial" и т.д.
        pid_str = parts[2]  # ID плана
        promo_id_str = parts[3]  # ID промокода
        
        try:
            pid = int(pid_str)
            promo_id = int(promo_id_str)
        except ValueError as e:
            logging.error(f"🚫 Invalid IDs in callback: pid={pid_str}, promo_id={promo_id_str}, error: {e}")
            bot.answer_callback_query(call.id, "❌ Неверные данные.")
            return
        
        logging.info(f"🔍 DEBUG Parsed successfully: pid={pid}, payment_type={payment_type}, promo_id={promo_id}")
        
        # ВАЖНО: Получаем полную информацию о группе
        cursor.execute("SELECT title, price_cents, description, group_id FROM plans WHERE id=?", (pid,))
        plan = cursor.fetchone()
        if not plan:
            logging.error(f"🚫 Plan {pid} not found in database")
            bot.answer_callback_query(call.id, "❌ Группа не найдена.")
            return
        title, price_cents, description, group_id = plan
        
        logging.info(f"🔍 DEBUG Plan data: title={title}, group_id={group_id}")
        
        # Рассчитываем базовую цену в зависимости от типа оплаты
        if payment_type in ('partial', 'second_part', 'half_month', 'second_part_late'):
            original_amount = price_cents // 2
        else:  # full или full_anytime
            original_amount = price_cents
        
        # Применяем промокод к цене
        promo_data = get_promo_code_by_id(promo_id)
        if promo_data:
            discounted_amount, _ = apply_promo_code(original_amount, promo_data)
            logging.info(f"🔍 DEBUG Promo applied: {original_amount} -> {discounted_amount}")
        else:
            discounted_amount = original_amount
            logging.warning(f"⚠️ Promo code {promo_id} not found, using original price")
        
        # ВАЖНО: Сохраняем ВСЮ информацию о группе
        user_states[user.id] = {
            'plan_id': pid,
            'original_price': original_amount,
            'final_price': discounted_amount,
            'title': title,
            'description': description,
            'group_id': group_id,
            'payment_type': payment_type,
            'promo_id': promo_id,
            'mode': 'payment_method_with_promo'
        }
        
        logging.info(f"✅ User state saved for user {user.id}")
        
        # Получаем доступные способы оплаты
        payment_methods = get_active_payment_methods()
        if not payment_methods:
            bot.answer_callback_query(call.id, "❌ Нет доступных способов оплаты")
            return
            
        # ЕСЛИ СПОСОБ ОПЛАТЫ ВСЕГО ОДИН - СРАЗУ ПЕРЕХОДИМ К НЕМУ
        if len(payment_methods) == 1:
            method_id, name, mtype, method_desc, details = payment_methods[0]
            
            if mtype == "card":
                # Если только карта - сразу создаем счет
                process_card_payment(call, pid, user, title, discounted_amount, description, group_id, payment_type, promo_id)
            else:
                # Если только ручная оплата - сразу переходим к инструкциям
                process_manual_payment_start(call, pid, user, title, discounted_amount, description, details, payment_type, promo_id)
            return
        
        # ЕСЛИ СПОСОБОВ НЕСКОЛЬКО - показываем выбор
        payment_type_text = get_payment_type_text(payment_type)
        
        text = (f"💳 <b>Оплата {payment_type_text} группы '{title}'</b>\n\n"
                f"💰 Исходная цена: {price_str_from_cents(original_amount)}\n"
                f"🎫 Применен промокод\n"
                f"💵 Итоговая цена: {price_str_from_cents(discounted_amount)}\n\n"
                f"Выберите способ оплаты:")
        
        markup = types.InlineKeyboardMarkup()
        for method_id, name, mtype, method_desc, details in payment_methods:
            callback_data = f"paymethod_promo:{pid}:{method_id}:{payment_type}:{promo_id}"
            logging.info(f"🔍 DEBUG Creating button: {callback_data}")
            markup.add(types.InlineKeyboardButton(name, callback_data=callback_data))
        
        bot.answer_callback_query(call.id, "💳 Выберите способ оплаты")
        bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
        
    except Exception as e:
        logging.exception("Error in callback_buy_with_promo")
        bot.answer_callback_query(call.id, "❌ Ошибка при оформлении заказа")

def get_promo_code_by_id(promo_id):
    """Получает информацию о промокоде по ID"""
    cursor.execute("""
        SELECT id, code, discount_percent, discount_fixed_cents, is_active, used_count, max_uses, expires_ts 
        FROM promo_codes WHERE id=?
    """, (promo_id,))
    return cursor.fetchone() 

def show_payment_methods(chat_id, user_id, state):
    """Показывает способы оплаты"""
    payment_methods = get_active_payment_methods()
    if not payment_methods:
        bot.send_message(chat_id, "❌ Нет доступных способов оплаты")
        return
        
    payment_type_text = get_payment_type_text(state['payment_type'])
    
    text = (f"💳 <b>Оплата {payment_type_text} группы '{state['title']}'</b>\n\n"
            f"💰 Сумма: {price_str_from_cents(state['original_price'])}\n\n"
            f"Выберите способ оплаты:")
    
    markup = types.InlineKeyboardMarkup()
    
    for method_id, name, mtype, method_desc, details in payment_methods:
        markup.add(types.InlineKeyboardButton(name, callback_data=f"paymethod:{state['plan_id']}:{method_id}:{state['payment_type']}"))
    
    markup.add(types.InlineKeyboardButton("🎫 Ввести промокод", callback_data=f"enter_promo:{state['plan_id']}:{state['payment_type']}"))
    
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

# Функции оплаты
def process_card_payment(call, pid, user, title, price_cents, description, group_id, payment_type, promo_id=None, renewal_end_ts=None):
    """Обработка оплаты картой"""
    logging.info(f"🔍 process_card_payment called: pid={pid}, payment_type={payment_type}, group_id={group_id}, renewal_end_ts={renewal_end_ts}")
    
    # ВАЖНО: Проверяем наличие group_id
    if group_id is None:
        # Пытаемся получить group_id из базы
        cursor.execute("SELECT group_id FROM plans WHERE id=?", (pid,))
        plan_data = cursor.fetchone()
        if plan_data:
            group_id = plan_data[0]
            logging.info(f"🔍 Got group_id from DB: {group_id}")
        else:
            group_id = get_default_group()
            logging.info(f"🔍 Using default group: {group_id}")
            
    if group_id is None:
        logging.error("🚫 No group_id available")
        bot.answer_callback_query(call.id, "❌ Нет доступных групп. Обратитесь к администратору.")
        return
    
    prices = [types.LabeledPrice(label=title, amount=price_cents)]
    
    # Создаем payload с информацией
    current_month, current_year = get_current_period()
    
    if payment_type == 'renewal' and renewal_end_ts:
        # Для продления добавляем специальную метку
        payload = f"renewal:{pid}:user:{user.id}:end_ts:{renewal_end_ts}:promo:{promo_id or 0}:{int(time.time())}"
        logging.info(f"🔍 Created renewal payload: {payload}")
    else:
        payload = f"plan:{pid}:user:{user.id}:type:{payment_type}:month:{current_month}:year:{current_year}:promo:{promo_id or 0}:{int(time.time())}"
        logging.info(f"🔍 Created regular payload: {payload}")
    
    # Сохраняем в базу
    cursor.execute("INSERT OR REPLACE INTO invoices (payload, user_id, plan_id, amount_cents, created_ts, payment_type, period_month, period_year, promo_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (payload, user.id, pid, price_cents, int(time.time()), payment_type, current_month, current_year, promo_id))
    conn.commit()
    
    try:
        if payment_type == 'renewal':
            description_text = f"Продление подписки '{title}' на следующий месяц"
            if renewal_end_ts:
                end_date = datetime.fromtimestamp(renewal_end_ts, LOCAL_TZ).strftime('%d.%m.%Y')
                description_text += f" до {end_date}"
        else:
            description_text = f"{description}\nТип оплаты: {get_payment_type_text(payment_type)}"
            
        if promo_id:
            description_text += f"\nПрименен промокод"
        
        logging.info(f"🔍 Sending invoice: title={title}, amount={price_cents}")
        bot.send_invoice(
            call.message.chat.id, 
            title=title, 
            description=description_text,
            invoice_payload=payload, 
            provider_token=PROVIDER_TOKEN,
            currency=CURRENCY, 
            prices=prices
        )
        bot.answer_callback_query(call.id, "💳 Счёт для оплаты:")
    except Exception as e:
        logging.exception(f"send_invoice failed: {e}")
        bot.answer_callback_query(call.id, f"❌ Ошибка создания счёта: {str(e)}")

def debug_plan_info(plan_id):
    """Отладочная информация о плане"""
    cursor.execute("SELECT id, title, price_cents, group_id FROM plans WHERE id=?", (plan_id,))
    plan = cursor.fetchone()
    if plan:
        logging.info(f"🔍 DEBUG Plan {plan_id}: id={plan[0]}, title={plan[1]}, price={plan[2]}, group_id={plan[3]}")
    else:
        logging.error(f"🚫 Plan {plan_id} not found")    

def process_manual_payment_start(call, pid, user, title, price_cents, description, details, payment_type, promo_id=None, renewal_end_ts=None):
    """Начало процесса ручной оплаты"""
    user_id = user.id
    user_states[user_id] = {
        "mode": "manual_payment",
        "plan_id": pid,
        "amount_cents": price_cents,
        "title": title,
        "step": "show_instructions",
        "payment_type": payment_type,
        "promo_id": promo_id,
        "renewal_end_ts": renewal_end_ts  # Добавляем для продления
    }
    
    payment_type_text = get_payment_type_text(payment_type)
    
    if payment_type == 'renewal':
        end_date_str = datetime.fromtimestamp(renewal_end_ts, LOCAL_TZ).strftime('%d.%m.%Y %H:%M')
        text = (f"💳 <b>Продление подписки на следующий месяц для группы '{title}'</b>\n\n"
                f"💰 Сумма к оплате: {price_str_from_cents(price_cents)}\n\n"
                f"📅 Будет активно до: {end_date_str}\n\n"
                f"📋 <b>Инструкция по оплате:</b>\n{details}\n\n"
                f"После оплаты нажмите кнопку '✅ Я оплатил(а)' и следуйте инструкциям.")
    else:
        text = (f"💳 <b>Оплата {payment_type_text} группы '{title}'</b>\n\n"
                f"💰 Сумма к оплате: {price_str_from_cents(price_cents)}\n\n"
                f"📋 <b>Инструкция по оплате:</b>\n{details}\n\n"
                f"После оплаты нажмите кнопку '✅ Я оплатил(а)' и следуйте инструкциям.")
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Я оплатил(а)", callback_data=f"confirm_paid_renewal:{pid}:{payment_type}:{renewal_end_ts or 0}"))
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_payment"))
    
    bot.answer_callback_query(call.id, "📋 Инструкция по оплате отправлена")
    bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("confirm_paid_renewal:"))
def callback_confirm_paid_renewal(call):
    """Подтверждение ручной оплаты для продления"""
    try:
        parts = call.data.split(":")
        pid = int(parts[1])
        payment_type = parts[2]
        renewal_end_ts = int(parts[3]) if parts[3] != '0' else None
        
        user_id = call.from_user.id
        
        # Сохраняем текущее состояние
        current_state = user_states.get(user_id, {})
        
        user_states[user_id] = {
            "mode": "manual_payment", 
            "plan_id": pid,
            "step": "waiting_receipt",
            "amount_cents": current_state.get("amount_cents", 0),
            "payment_type": payment_type,
            "promo_id": current_state.get("promo_id"),
            "renewal_end_ts": renewal_end_ts
        }
        
        bot.answer_callback_query(call.id, "📎 Отправьте фото чека об оплате")
        bot.send_message(call.message.chat.id, "📎 Пожалуйста, отправьте фото или скриншот чека об оплате:")
        
    except Exception as e:
        logging.exception("Error in callback_confirm_paid_renewal")
        bot.answer_callback_query(call.id, "❌ Ошибка")



def process_manual_payment_start_from_message(message, pid, title, price_cents, description, details, payment_type, promo_id=None):
    """Начало ручной оплаты из сообщения"""
    user_id = message.from_user.id
    user_states[user_id] = {
        "mode": "manual_payment",
        "plan_id": pid,
        "amount_cents": price_cents,
        "title": title,
        "step": "show_instructions",
        "payment_type": payment_type,
        "promo_id": promo_id
    }
    
    payment_type_text = get_payment_type_text(payment_type)
    
    text = (f"💳 <b>Оплата {payment_type_text} группы '{title}'</b>\n\n"
            f"💰 Сумма к оплате: {price_str_from_cents(price_cents)}\n\n"
            f"📋 <b>Инструкция по оплате:</b>\n{details}\n\n"
            f"После оплаты нажмите кнопку '✅ Я оплатил(а)' и следуйте инструкциям.")
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Я оплатил(а)", callback_data=f"confirm_paid:{pid}:{payment_type}"))
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_payment"))
    
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)

def get_payment_type_text(payment_type):
    """Возвращает текстовое описание типа оплаты"""
    if payment_type == 'full' or payment_type == 'full_anytime':
        return "полной"
    elif payment_type == 'partial':
        return "первой части"
    elif payment_type == 'second_part':
        return "второй части"
    elif payment_type == 'half_month':
        return "половины месяца"
    else:
        return ""
    
# Обработчики выбора способа оплаты
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("paymethod:"))
def callback_paymethod(call):
    """Обработка выбора способа оплаты"""
    try:
        parts = call.data.split(":")
        pid = int(parts[1])
        method_id = int(parts[2])
        payment_type = parts[3]
        
        user = call.from_user
        
        if user.id not in user_states:
            bot.answer_callback_query(call.id, "❌ Сессия устарела")
            return
            
        state = user_states[user.id]
        
        # Получаем информацию о тарифе
        cursor.execute("SELECT title, price_cents, description, group_id FROM plans WHERE id=?", (pid,))
        plan = cursor.fetchone()
        if not plan:
            bot.answer_callback_query(call.id, "❌ Тариф не найден.")
            return
            
        title, price_cents, description, group_id = plan
        
        method = get_payment_method_by_id(method_id)
        if not method:
            bot.answer_callback_query(call.id, "❌ Способ оплаты не найден.")
            return
            
        method_id, name, mtype, method_desc, details = method
        
        if mtype == "card":
            process_card_payment(call, pid, user, title, state['original_price'], description, group_id, payment_type)
        else:  # manual
            process_manual_payment_start(call, pid, user, title, state['original_price'], description, details, payment_type)
            
    except Exception as e:
        logging.exception("Error in callback_paymethod")
        bot.answer_callback_query(call.id, "❌ Ошибка при выборе способа оплаты")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("paymethod_promo:"))
def callback_paymethod_promo(call):
    """Обработка выбора способа оплаты с промокодом"""
    try:
        parts = call.data.split(":")
        pid = int(parts[1])
        method_id = int(parts[2])
        payment_type = parts[3]
        promo_id = int(parts[4])
        
        user = call.from_user
        
        logging.info(f"🔍 DEBUG paymethod_promo CALLBACK: user={user.id}, pid={pid}, method_id={method_id}, payment_type={payment_type}, promo_id={promo_id}")
        
        if user.id not in user_states:
            logging.error(f"🚫 User state missing for user {user.id}")
            bot.answer_callback_query(call.id, "❌ Сессия устарела")
            return
            
        state = user_states[user.id]
        logging.info(f"🔍 DEBUG user_state in paymethod_promo: {state}")
        
        # Проверяем наличие всех необходимых полей
        required_fields = ['plan_id', 'title', 'description', 'group_id', 'final_price']
        missing_fields = [field for field in required_fields if field not in state or state[field] is None]
        
        if missing_fields:
            logging.error(f"🚫 Missing fields in user state: {missing_fields}")
            bot.answer_callback_query(call.id, "❌ Ошибка данных. Попробуйте снова.")
            return
        
        method = get_payment_method_by_id(method_id)
        if not method:
            bot.answer_callback_query(call.id, "❌ Способ оплаты не найден.")
            return
            
        method_id, name, mtype, method_desc, details = method
        
        logging.info(f"🔍 DEBUG Calling process_card_payment with: group_id={state['group_id']}")
        
        if mtype == "card":
            process_card_payment(call, pid, user, state['title'], state['final_price'], state['description'], state['group_id'], payment_type, promo_id)
        else:  # manual
            process_manual_payment_start(call, pid, user, state['title'], state['final_price'], state['description'], details, payment_type, promo_id)
            
    except Exception as e:
        logging.exception("Error in callback_paymethod_promo")
        bot.answer_callback_query(call.id, "❌ Ошибка при выборе способа оплаты")

# Добавьте эту временную функцию для проверки групп
@bot.message_handler(commands=["debug_groups"])
def debug_groups(message):
    if message.from_user.id not in ADMIN_IDS:
        return
        
    cursor.execute("SELECT chat_id, title, is_default FROM managed_groups")
    groups = cursor.fetchall()
    
    text = "📋 Зарегистрированные группы:\n\n"
    for chat_id, title, is_default in groups:
        text += f"🏷️ {title}\nID: {chat_id}\nПо умолчанию: {'✅' if is_default else '❌'}\n\n"
    
    bot.send_message(message.chat.id, text)
    
    # Проверим конкретно группу -1002496898299
    cursor.execute("SELECT chat_id, title FROM managed_groups WHERE chat_id = ?", (-1002496898299,))
    group = cursor.fetchone()
    if group:
        bot.send_message(message.chat.id, f"✅ Группа -1002496898299 найдена: {group[1]}")
    else:
        bot.send_message(message.chat.id, "❌ Группа -1002496898299 НЕ найдена в базе!")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("pay_with_promo:"))
def callback_pay_with_promo(call):
    """Оплата картой с примененным промокодом"""
    user_id = call.from_user.id
    if user_id not in user_states or 'final_price' not in user_states[user_id]:
        bot.answer_callback_query(call.id, "❌ Сессия устарела")
        return
        
    state = user_states[user_id]
    parts = call.data.split(":")
    pid = int(parts[1])
    payment_type = parts[2]
    
    process_card_payment(call, pid, call.from_user, state['title'], state['final_price'], state['description'], state['group_id'], payment_type, state.get('promo_id'))

# Обработчик подтверждения ручной оплаты
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("confirm_paid:"))
def callback_confirm_paid(call):
    """Подтверждение оплаты для ручного метода"""
    try:
        parts = call.data.split(":")
        pid = int(parts[1])
        payment_type = parts[2] if len(parts) > 2 else 'full'
        
        user_id = call.from_user.id
        
        # Сохраняем текущее состояние
        current_state = user_states.get(user_id, {})
        
        user_states[user_id] = {
            "mode": "manual_payment", 
            "plan_id": pid,
            "step": "waiting_receipt",
            "amount_cents": current_state.get("amount_cents", 0),
            "payment_type": payment_type,
            "promo_id": current_state.get("promo_id")
        }
        
        bot.answer_callback_query(call.id, "📎 Отправьте фото чека об оплате")
        bot.send_message(call.message.chat.id, "📎 Пожалуйста, отправьте фото или скриншот чека об оплате:")
        
    except Exception as e:
        logging.exception("Error in callback_confirm_paid")
        bot.answer_callback_query(call.id, "❌ Ошибка")

@bot.callback_query_handler(func=lambda call: call.data == "cancel_payment")
def callback_cancel_payment(call):
    """Отмена оплаты"""
    user_id = call.from_user.id
    if user_id in user_states:
        user_states.pop(user_id)
    bot.answer_callback_query(call.id, "❌ Оплата отменена")
    try:
        bot.edit_message_text("❌ Оплата отменена", call.message.chat.id, call.message.message_id)
    except:
        pass

# Обработчик фото чека для ручной оплаты
@bot.message_handler(content_types=['photo'], func=lambda m: m.from_user.id in user_states and user_states[m.from_user.id].get("mode") == "manual_payment" and user_states[m.from_user.id].get("step") == "waiting_receipt")
def handle_receipt_photo(message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    
    if not state or state.get("step") != "waiting_receipt":
        return
        
    receipt_photo = message.photo[-1].file_id
    state["receipt_photo"] = receipt_photo
    state["step"] = "waiting_name"
    
    bot.send_message(message.chat.id, "✅ Чек принят! Теперь введите ваши Фамилию и Имя:")

# Обработчик ФИО для ручной оплаты
@bot.message_handler(func=lambda m: m.from_user.id in user_states and user_states[m.from_user.id].get("mode") == "manual_payment" and user_states[m.from_user.id].get("step") == "waiting_name" and m.text)
def handle_full_name(message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    
    if not state or state.get("step") != "waiting_name":
        return
        
    full_name = message.text.strip()
    if len(full_name) < 2:
        bot.send_message(message.chat.id, "❌ Пожалуйста, введите полные Фамилию и Имя:")
        return
        
    # Сохраняем заявку на ручную оплату
    cursor.execute("""
        INSERT INTO manual_payments (user_id, plan_id, amount_cents, receipt_photo, full_name, created_ts, payment_type, period_month, period_year, promo_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, state["plan_id"], state["amount_cents"], state["receipt_photo"], full_name, int(time.time()), state["payment_type"], *get_current_period(), state.get("promo_id")))
    payment_id = cursor.lastrowid
    conn.commit()
    
    # Уведомляем админов
    cursor.execute("SELECT title FROM plans WHERE id=?", (state["plan_id"],))
    plan_title = cursor.fetchone()[0]
    
    payment_type_text = get_payment_type_text(state["payment_type"])
    
    for admin_id in ADMIN_IDS:
        try:
            text = (f"📋 <b>Новая заявка на ручную оплату</b>\n\n"
                    f"👤 Пользователь: @{message.from_user.username or 'N/A'} (ID: {user_id})\n"
                    f"🏷️ Группа: {plan_title}\n"
                    f"💵 Сумма: {price_str_from_cents(state['amount_cents'])}\n"
                    f"💳 Тип оплаты: {payment_type_text}\n"
                    f"👤 ФИО: {full_name}")
                    
            markup = types.InlineKeyboardMarkup()
            markup.row(
                types.InlineKeyboardButton("✅ Одобрить", callback_data=f"approve_payment:{payment_id}"),
                types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_payment:{payment_id}")
            )
            
            bot.send_photo(admin_id, state["receipt_photo"], caption=text, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            logging.error(f"Error notifying admin {admin_id}: {e}")
    
    # Очищаем состояние пользователя
    user_states.pop(user_id, None)
    
    bot.send_message(message.chat.id, "✅ Заявка отправлена на проверку! Ожидайте подтверждения администратора.")

# Обработчик успешной оплаты картой
@bot.pre_checkout_query_handler(func=lambda q: True)
def handle_precheckout(q):
    bot.answer_pre_checkout_query(q.id, ok=True)

@bot.message_handler(content_types=['successful_payment'])
def got_payment(message):
    sp = message.successful_payment
    payload = sp.invoice_payload
    user_id = message.from_user.id
    
    # Проверяем, это продление или обычная оплата
    if payload.startswith("renewal:"):
        # Это продление
        parts = payload.split(":")
        plan_id = int(parts[1])
        renewal_end_ts = int(parts[5])
        promo_id = int(parts[7]) if len(parts) > 7 and parts[7] != '0' else None
        
        # Используем обновленную функцию с параметром renewal_end_ts
        success, result = activate_subscription(user_id, plan_id, 'renewal', renewal_end_ts=renewal_end_ts)
        
    else:
        # Обычная оплата
        parts = payload.split(":")
        plan_id = int(parts[1])
        payment_type = parts[5]
        period_month = int(parts[7])
        period_year = int(parts[9])
        promo_id = int(parts[11]) if len(parts) > 11 and parts[11] != '0' else None

        success, result = activate_subscription(user_id, plan_id, payment_type)
    
    if not success:
        bot.send_message(user_id, f"❌ Ошибка активации подписки: {result}")
        return
    
    # Если был применен промокод, отмечаем его использование
    if promo_id and promo_id > 0:
        cursor.execute("INSERT INTO promo_usage (promo_id, user_id, used_ts) VALUES (?, ?, ?)",
                      (promo_id, user_id, int(time.time())))
        cursor.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE id=?", (promo_id,))
        conn.commit()
    
    # Формируем текст сообщения
    cursor.execute("SELECT title FROM plans WHERE id=?", (plan_id,))
    found = cursor.fetchone()
    if found:
        plan_title = found[0]
        
        if payload.startswith("renewal:"):
            # Сообщение о продлении
            txt = (f"✅ <b>Спасибо за продление подписки на группу '{plan_title}'!</b>\n\n"
                   f"🔗 Ваша новая приватная ссылка для входа в чат (одноразовая):\n{result}\n\n"
                   f"⏰ Подписка будет активна с момента окончания текущей.")
        elif payment_type == 'half_month':
            txt = (f"✅ <b>Спасибо за оплату половины месяца в группе '{plan_title}'!</b>\n\n"
                   f"🔗 Ваша приватная ссылка для входа в чат (одноразовая):\n{result}\n\n"
                   f"⏰ Подписка активна до 5 числа следующего месяца")
        elif payment_type == 'partial':
            txt = (f"✅ <b>Спасибо за оплату первой части в группе '{plan_title}'!</b>\n\n"
                   f"🔗 Ваша приватная ссылка для входа в чат (одноразовая):\n{result}\n\n"
                   f"⏰ Подписка активна до 15 числа текущего месяца\n"
                   f"💳 <b>Вторая часть оплачивается 15-20 числа</b>")
        else:
            # Полная оплата
            txt = (f"✅ <b>Спасибо за оплату группы '{plan_title}'!</b>\n\n"
                   f"🔗 Ваша приватная ссылка для входа в чат (одноразовая):\n{result}\n\n"
                   f"⏰ Подписка активна до 5 числа следующего месяца")
        
        bot.send_message(user_id, txt, parse_mode="HTML")
    else:
        bot.send_message(user_id, f"✅ Платёж принят! 🔗 Ваша ссылка: {result}")
    
    # Очищаем состояние пользователя
    if user_id in user_states:
        user_states.pop(user_id)

# ----------------- Админ-панель -----------------
@bot.message_handler(func=lambda message: message.text == "⚙️ Админ меню")
@only_private
def admin_menu(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "🚫 Доступ запрещен.", reply_markup=main_menu(message.from_user.id))
        return
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row(types.KeyboardButton("➕ Новая группа"), types.KeyboardButton("📝 Редактировать группу"))
    markup.row(types.KeyboardButton("👥 Управление группами"), types.KeyboardButton("🔄 Авто-добавление групп"))
    markup.row(types.KeyboardButton("📊 Подписки"), types.KeyboardButton("👤 Пользователи"))
    markup.row(types.KeyboardButton("💳 Управление оплатой"), types.KeyboardButton("📋 Заявки на оплату"))
    markup.row(types.KeyboardButton("🎫 Промокоды"), types.KeyboardButton("🔙 Главное меню"))
    bot.send_message(message.chat.id, "⚙️ Админ меню:", reply_markup=markup)

# Продолжение админ-панели в следующем сообщении...
@bot.message_handler(func=lambda message: message.text == "🔙 Главное меню")
@only_private
def back_to_main(message):
    bot.send_message(message.chat.id, "📋 Главное меню:", reply_markup=main_menu(message.from_user.id))

# Создание новой группы
@bot.message_handler(func=lambda message: message.text == "➕ Новая группа")
@only_private
def cmd_newplan(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    uid = message.from_user.id
    admin_states[uid] = {"mode": "create", "step": "title", "media_files": [], "media_type": None, "chat_id": message.chat.id}
    bot.send_message(message.chat.id, "➕ Добавление новой группы обучения.\nШаг 1/6: Отправьте название группы:")

# Редактирование групп
@bot.message_handler(func=lambda message: message.text == "📝 Редактировать группу")
@only_private
def admin_list_plans(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    cursor.execute("""
        SELECT p.id, p.title, p.price_cents, p.duration_days, p.group_id, mg.title
        FROM plans p
        LEFT JOIN managed_groups mg ON p.group_id = mg.chat_id
        WHERE p.is_active=1
        ORDER BY p.id
    """)
    rows = cursor.fetchall()
    if not rows:
        bot.send_message(message.chat.id, "📭 Групп обучения нет.")
        return
    for pid, title, price_cents, days, group_id, group_title in rows:
        group_text = f"Группа: {group_title}" if group_title else "Группа: по умолчанию"
        text = f"<b>{title}</b>\nЦена в месяц: {price_str_from_cents(price_cents)}\n{group_text}"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✏️ Редактировать", callback_data=f"editplan:{pid}"))
        markup.add(types.InlineKeyboardButton("🗑 Удалить", callback_data=f"delplan:{pid}"))
        markup.add(types.InlineKeyboardButton("🔍 Просмотреть медиа", callback_data=f"viewmedia:{pid}"))
        bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)

# Управление группами
@bot.message_handler(func=lambda message: message.text == "👥 Управление группами")
@only_private
def cmd_groups(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    groups = get_all_groups_with_bot()
    if not groups:
        invite_link = get_bot_invite_link()
        text = ("📭 Нет зарегистрированных групп/каналов.\n\n"
                "💡 <b>Как добавить группу:</b>\n"
                "1. Нажмите кнопку ниже чтобы добавить бота в группу\n"
                "2. Назначьте боту права администратора\n"
                "3. Используйте команду /register_group в группе\n\n"
                "Или добавьте бота по ссылке:")
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔗 Добавить бота в группу", url=invite_link))
        markup.add(types.InlineKeyboardButton("🔄 Авто-добавление групп", callback_data="auto_add_groups"))
        
        bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)
        return
        
    text = "🏷️ Зарегистрированные группы/каналы:\n\n"
    for chat_id, title, chat_type in groups:
        bot_status = "✅ Админ" if is_bot_admin_in_chat(chat_id) else "❌ Не админ"
        cursor.execute("SELECT is_default FROM managed_groups WHERE chat_id=?", (chat_id,))
        r = cursor.fetchone()
        is_default = r[0] if r else 0
        default_text = "✅ По умолчанию" if is_default else "❌ Не по умолчанию"
        emoji = "📢" if chat_type == "channel" else "👥"
        text += f"{emoji} <b>{title}</b>\nID: {chat_id}\nТип: {chat_type}\n{default_text}\nСтатус: {bot_status}\n\n"
    
    markup = types.InlineKeyboardMarkup()
    for chat_id, title, chat_type in groups:
        cursor.execute("SELECT is_default FROM managed_groups WHERE chat_id=?", (chat_id,))
        r = cursor.fetchone()
        is_default = r[0] if r else 0
        if not is_default:
            markup.add(types.InlineKeyboardButton(f"⚡ Default: {title[:15]}", callback_data=f"set_default:{chat_id}"))
    
    invite_link = get_bot_invite_link()
    markup.add(types.InlineKeyboardButton("🔗 Добавить новую группу", url=invite_link))
    
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)

# Авто-добавление групп
@bot.message_handler(func=lambda message: message.text == "🔄 Авто-добавление групп")
@only_private
def auto_add_groups(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    invite_link = get_bot_invite_link()
    text = ("🔄 <b>Автоматическое добавление групп/каналов</b>\n\n"
            "1) Добавьте бота в группу по ссылке ниже\n"
            "2) Назначьте права администратора\n"
            "3) Используйте команду /register_group в группе\n\n"
            f"🔗 Ссылка: {invite_link}")
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔗 Добавить бота в группу", url=invite_link))
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)

# Просмотр подписок
@bot.message_handler(func=lambda message: message.text == "📊 Подписки")
@only_private
def cmd_sublist(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    cursor.execute("""
        SELECT s.id, s.user_id, s.plan_id, s.start_ts, s.end_ts, s.active, s.group_id, p.title, s.payment_type, s.part_paid, s.current_period_month, s.current_period_year
        FROM subscriptions s
        LEFT JOIN plans p ON s.plan_id = p.id
        ORDER BY s.id DESC LIMIT 50
    """)
    rows = cursor.fetchall()
    if not rows:
        bot.send_message(message.chat.id, "📭 Подписок нет.")
        return
    text = "📊 Последние подписки:\n\n"
    current_month, current_year = get_current_period()
    
    for sid, uid, pid, st, et, active, gid, ptitle, payment_type, part_paid, period_month, period_year in rows:
        status = "✅ Активна" if active else "❌ Неактивна"
        
        if period_month == current_month and period_year == current_year:
            if part_paid == 'full':
                payment_status = "💰 Оплачено полностью"
            elif part_paid == 'first':
                payment_status = "⏳ Ожидает вторую часть"
            else:
                payment_status = "❌ Не оплачено"
        else:
            payment_status = "📅 Требуется оплата за новый месяц"
            
        time_left = et - int(time.time())
        days_left = max(0, time_left // (24*3600))
        text += f"🎫 #{sid} | 👤 {uid} | 🏷️ {ptitle or pid}\n💳 {payment_type} | {payment_status}\n📊 {status} | ⏰ Осталось: {days_left}д\n🏠 Группа: {gid}\n\n"
    bot.send_message(message.chat.id, text)

# Просмотр пользователей
@bot.message_handler(func=lambda message: message.text == "👤 Пользователи")
@only_private
def cmd_users(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    cursor.execute("SELECT user_id, referred_by, cashback_cents, username, join_date FROM users ORDER BY user_id DESC LIMIT 50")
    rows = cursor.fetchall()
    if not rows:
        bot.send_message(message.chat.id, "📭 Нет пользователей.")
        return
    text = "👤 Последние пользователи:\n\n"
    for user_id, referred_by, cashback_cents, username, join_date in rows:
        ref_text = f"👥 Реферер: {referred_by}" if referred_by else "🚫 Без реферера"
        join_date_str = datetime.utcfromtimestamp(join_date).strftime('%Y-%m-%d') if join_date else "N/A"
        text += f"🆔 ID: {user_id}\n👤 Username: {username or 'N/A'}\n{ref_text}\n💰 Баланс: {price_str_from_cents(cashback_cents)}\n📅 Регистрация: {join_date_str}\n\n"
    bot.send_message(message.chat.id, text)

# Управление оплатой
@bot.message_handler(func=lambda message: message.text == "💳 Управление оплатой")
@only_private
def cmd_payment_management(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    methods = get_active_payment_methods()
    text = "💳 <b>Управление способами оплаты</b>\n\n"
    for method_id, name, mtype, description, details in methods:
        status = "✅ Включен"
        text += f"<b>{name}</b> ({mtype})\n{description}\nСтатус: {status}\nID: {method_id}\n\n"
    
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("🔧 Настроить карту", callback_data="config_payment:card"),
        types.InlineKeyboardButton("🔧 Настроить ручную", callback_data="config_payment:manual")
    )
    markup.row(
        types.InlineKeyboardButton("🔄 Переключить карту", callback_data="toggle_payment:card"),
        types.InlineKeyboardButton("🔄 Переключить ручную", callback_data="toggle_payment:manual")
    )
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)

# Заявки на оплату
@bot.message_handler(func=lambda message: message.text == "📋 Заявки на оплату")
@only_private
def cmd_pending_payments(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    cursor.execute("""
        SELECT mp.id, mp.user_id, mp.plan_id, mp.amount_cents, mp.receipt_photo, mp.full_name, mp.created_ts, p.title, u.username, mp.payment_type
        FROM manual_payments mp
        LEFT JOIN plans p ON mp.plan_id = p.id
        LEFT JOIN users u ON mp.user_id = u.user_id
        WHERE mp.status = 'pending'
        ORDER BY mp.created_ts
    """)
    rows = cursor.fetchall()
    if not rows:
        bot.send_message(message.chat.id, "📭 Нет ожидающих заявок на оплату.")
        return
    
    for row in rows:
        payment_id, user_id, plan_id, amount_cents, receipt_photo, full_name, created_ts, plan_title, username, payment_type = row
        payment_type_text = get_payment_type_text(payment_type)
        
        text = (f"📋 <b>Заявка на оплату #{payment_id}</b>\n\n"
                f"👤 Пользователь: {username or 'N/A'} (ID: {user_id})\n"
                f"🏷️ Группа: {plan_title}\n"
                f"💵 Сумма: {price_str_from_cents(amount_cents)}\n"
                f"💳 Тип оплаты: {payment_type_text}\n"
                f"👤 ФИО: {full_name}\n"
                f"⏰ Время заявки: {datetime.utcfromtimestamp(created_ts).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("✅ Одобрить", callback_data=f"approve_payment:{payment_id}"),
            types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_payment:{payment_id}")
        )
        
        if receipt_photo:
            try:
                bot.send_photo(message.chat.id, receipt_photo, caption=text, parse_mode="HTML", reply_markup=markup)
            except:
                bot.send_message(message.chat.id, text + f"\n\n📎 Чек: {receipt_photo}", parse_mode="HTML", reply_markup=markup)
        else:
            bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)

# Управление промокодами
@bot.message_handler(func=lambda message: message.text == "🎫 Промокоды")
@only_private
def cmd_promo_codes(message):
    if message.from_user.id not in ADMIN_IDS:
        return
        
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("➕ Создать промокод", callback_data="create_promo"),
        types.InlineKeyboardButton("📋 Список промокодов", callback_data="list_promos")
    )
    bot.send_message(message.chat.id, "🎫 Управление промокодами:", reply_markup=markup)

# ----------------- Admin creation flow -----------------
@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and admin_states.get(m.from_user.id, {}).get("mode") == "create" and m.chat.type == "private",
                     content_types=['text', 'photo', 'video'])
def admin_create_handler(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    if not state:
        return
    if state.get("chat_id") and state["chat_id"] != message.chat.id:
        return
    step = state.get("step")

    # TITLE
    if step == "title":
        if not message.text:
            bot.send_message(message.chat.id, "❌ Отправьте название текстом.")
            return
        state["title"] = message.text.strip()
        state["step"] = "price"
        bot.send_message(message.chat.id, "Шаг 2/6: Укажите цену за месяц (например: 14.99):")
        return

    # PRICE
    if step == "price":
        if not message.text:
            bot.send_message(message.chat.id, "❌ Укажите цену текстом, например: 14.99")
            return
        cents = cents_from_str(message.text)
        if cents is None:
            bot.send_message(message.chat.id, "❌ Неправильный формат цены. Пример: 14.99")
            return
        state["price_cents"] = cents
        state["step"] = "description"
        bot.send_message(message.chat.id, "Шаг 3/6: Отправьте описание группы обучения:")
        return

    # DESCRIPTION
    if step == "description":
        if not message.text:
            bot.send_message(message.chat.id, "❌ Отправьте описание текстом.")
            return
        state["description"] = message.text.strip()
        state["step"] = "group"
        
        groups = get_all_groups_with_bot()
        if not groups:
            bot.send_message(message.chat.id, "❌ Нет доступных групп. Сначала добавьте бота в группу.")
            admin_states.pop(uid, None)
            return
            
        markup = types.InlineKeyboardMarkup()
        for chat_id, title, chat_type in groups:
            markup.add(types.InlineKeyboardButton(f"{title} ({chat_type})", callback_data=f"select_group:{chat_id}"))
        markup.add(types.InlineKeyboardButton("⏩ Использовать группу по умолчанию", callback_data="select_group:default"))
        
        bot.send_message(message.chat.id, "Шаг 4/6: Выберите группу для этого тарифа:", reply_markup=markup)
        return

    # MEDIA
    if step == "media":
        if message.photo:
            file_id = message.photo[-1].file_id
            state.setdefault("media_files", []).append(file_id)
            state["media_type"] = "photo"
            bot.send_message(message.chat.id, f"✅ Фото добавлено! Всего: {len(state['media_files'])}")
            return
        if message.video:
            file_id = message.video.file_id
            state.setdefault("media_files", []).append(file_id)
            state["media_type"] = "video"
            bot.send_message(message.chat.id, f"✅ Видео добавлено! Всего: {len(state['media_files'])}")
            return
        if message.text:
            txt = message.text.strip()
            if txt == "⏩ Пропустить медиа":
                group_id = state.get("group_id")
                cursor.execute("INSERT INTO plans (title, price_cents, duration_days, description, group_id, created_ts, is_active) VALUES (?, ?, 30, ?, ?, ?, 1)",
                               (state["title"], state["price_cents"], state["description"], group_id, int(time.time())))
                conn.commit()
                admin_states.pop(uid, None)
                bot.send_message(message.chat.id, "✅ Группа обучения сохранена (без превью).", reply_markup=main_menu(uid))
                return
            if txt == "✅ Завершить добавление медиа":
                media_files = state.get("media_files", [])
                media_type = state.get("media_type")
                first_media = media_files[0] if media_files else None
                media_ids_str = ",".join(media_files) if media_files else None
                group_id = state.get("group_id")
                
                cursor.execute("""INSERT INTO plans (title, price_cents, duration_days, description,
                                  media_file_id, media_type, media_file_ids, group_id, created_ts, is_active)
                                  VALUES (?, ?, 30, ?, ?, ?, ?, ?, ?, 1)""",
                               (state["title"], state["price_cents"], state["description"],
                                first_media, media_type, media_ids_str, group_id, int(time.time())))
                plan_id = cursor.lastrowid
                if media_files:
                    for idx, fid in enumerate(media_files):
                        cursor.execute("INSERT INTO plan_media (plan_id, file_id, media_type, ord, added_ts) VALUES (?, ?, ?, ?, ?)",
                                       (plan_id, fid, media_type, idx, int(time.time())))
                conn.commit()
                cnt = len(media_files)
                admin_states.pop(uid, None)
                if cnt == 0:
                    bot.send_message(message.chat.id, "✅ Группа обучения сохранена (без превью).", reply_markup=main_menu(uid))
                elif cnt == 1:
                    bot.send_message(message.chat.id, "✅ Группа обучения сохранена с 1 превью.", reply_markup=main_menu(uid))
                else:
                    bot.send_message(message.chat.id, f"✅ Группа обучения сохранена! Использовано первое из {cnt} медиа как превью.", reply_markup=main_menu(uid))
                return
            bot.send_message(message.chat.id, "❌ Отправляйте фото/видео или используйте кнопки '⏩ Пропустить медиа' / '✅ Завершить добавление медиа'.")
        return

# Обработчики callback для админ-панели
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("select_group:"))
def callback_select_group(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    group_data = call.data.split(":")[1]
    uid = call.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("step") != "group":
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
    
    if group_data == "default":
        group_id = get_default_group()
        if not group_id:
            bot.answer_callback_query(call.id, "❌ Группа по умолчанию не установлена.")
            return
        state["group_id"] = group_id
        cursor.execute("SELECT title FROM managed_groups WHERE chat_id=?", (group_id,))
        group_title = cursor.fetchone()[0]
        bot.answer_callback_query(call.id, f"✅ Выбрана группа по умолчанию: {group_title}")
    else:
        group_id = int(group_data)
        state["group_id"] = group_id
        cursor.execute("SELECT title FROM managed_groups WHERE chat_id=?", (group_id,))
        group_title = cursor.fetchone()[0]
        bot.answer_callback_query(call.id, f"✅ Выбрана группа: {group_title}")
    
    state["step"] = "media"
    if "media_files" not in state:
        state["media_files"] = []
    state["media_type"] = None
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row(types.KeyboardButton("⏩ Пропустить медиа"), types.KeyboardButton("✅ Завершить добавление медиа"))
    
    bot.edit_message_text(
        f"Шаг 5/6: Прикрепите фото/видео превью для группы '{state['title']}' (можно несколько).\nГруппа: {group_title}\n\nКогда закончите - нажмите '✅ Завершить добавление медиа'.",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=None
    )
    bot.send_message(call.message.chat.id, "Отправляйте медиа или используйте кнопки ниже:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("set_default:"))
def callback_set_default(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
    chat_id = int(call.data.split(":")[1])
    set_default_group(chat_id)
    cursor.execute("SELECT title FROM managed_groups WHERE chat_id=?", (chat_id,))
    title = cursor.fetchone()[0]
    bot.answer_callback_query(call.id, f"✅ Группа '{title}' установлена по умолчанию!")
    try:
        bot.edit_message_text(f"✅ Группа '{title}' установлена по умолчанию!", call.message.chat.id, call.message.message_id)
    except:
        pass

@bot.callback_query_handler(func=lambda call: call.data == "auto_add_groups")
def callback_auto_add_groups(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    invite_link = get_bot_invite_link()
    text = ("🔄 <b>Автоматическое добавление групп/каналов</b>\n\n"
            "1) Добавьте бота в группу по ссылке ниже\n"
            "2) Назначьте права администратора\n"
            "3) Используйте команду /register_group в группе\n\n"
            "🔗 Ссылка для добавления бота:")
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔗 Добавить бота в группу", url=invite_link))
    
    bot.answer_callback_query(call.id, "ℹ️ Информация об авто-добавлении")
    bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("viewmedia:"))
def callback_viewmedia(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
    pid = int(call.data.split(":")[1])
    cursor.execute("SELECT file_id, media_type FROM plan_media WHERE plan_id=? ORDER BY ord", (pid,))
    rows = cursor.fetchall()
    if not rows:
        bot.answer_callback_query(call.id, "📭 Медиа у группы не найдены.")
        return
    try:
        for fid, mtype in rows:
            if mtype == "photo":
                bot.send_photo(call.message.chat.id, fid)
            else:
                bot.send_video(call.message.chat.id, fid)
    except:
        pass
    bot.answer_callback_query(call.id, "📦 Все медиа отправлены (если были).")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("delplan:"))
def callback_delplan(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
    pid = int(call.data.split(":")[1])
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_del:{pid}"))
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    bot.answer_callback_query(call.id, "⚠️ Подтвердите удаление группы.")
    bot.send_message(call.message.chat.id, f"Вы уверены, что хотите удалить группу обучения #{pid}?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("confirm_del:"))
def callback_confirm_del(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
    pid = int(call.data.split(":")[1])
    try:
        cursor.execute("DELETE FROM plan_media WHERE plan_id=?", (pid,))
        cursor.execute("UPDATE plans SET is_active=0 WHERE id=?", (pid,))
        conn.commit()
        bot.answer_callback_query(call.id, "✅ Группа обучения удалена.")
        try:
            bot.edit_message_text("Группа обучения удалена.", call.message.chat.id, call.message.message_id)
        except:
            pass
    except Exception:
        logging.exception("Error deleting plan")
        bot.answer_callback_query(call.id, "❌ Ошибка при удалении группы.")

# Обработка заявок на оплату
@bot.callback_query_handler(func=lambda call: call.data and (call.data.startswith("approve_payment:") or call.data.startswith("reject_payment:")))
def handle_payment_review(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    is_approve = call.data.startswith("approve_payment:")
    payment_id = int(call.data.split(":")[1])
    
    cursor.execute("""
        SELECT mp.user_id, mp.plan_id, mp.amount_cents, p.title, u.username, mp.payment_type
        FROM manual_payments mp
        LEFT JOIN plans p ON mp.plan_id = p.id
        LEFT JOIN users u ON mp.user_id = u.user_id
        WHERE mp.id = ? AND mp.status = 'pending'
    """, (payment_id,))
    
    payment = cursor.fetchone()
    if not payment:
        bot.answer_callback_query(call.id, "❌ Заявка не найдена или уже обработана.")
        return
        
    user_id, plan_id, amount_cents, plan_title, username, payment_type = payment
    
    if is_approve:
        # Одобряем заявку
        success, result = activate_subscription(user_id, plan_id, payment_type)
        if success:
            cursor.execute("UPDATE manual_payments SET status='approved', admin_id=?, reviewed_ts=? WHERE id=?", 
                          (call.from_user.id, int(time.time()), payment_id))
            conn.commit()
            
            # Уведомляем пользователя
            try:
                bot.send_message(user_id, f"✅ Ваша заявка на группу '{plan_title}' одобрена!\n\n🔗 Ваша пригласительная ссылка (одноразовая):\n{result}")
            except:
                pass
                
            bot.answer_callback_query(call.id, "✅ Заявка одобрена!")
            try:
                bot.edit_message_caption(f"✅ ЗАЯВКА ОДОБРЕНА\n\nПользователь: {username or user_id}\nГруппа: {plan_title}", call.message.chat.id, call.message.message_id)
            except:
                pass
        else:
            bot.answer_callback_query(call.id, f"❌ Ошибка: {result}")
    else:
        # Отклоняем заявку
        cursor.execute("UPDATE manual_payments SET status='rejected', admin_id=?, reviewed_ts=? WHERE id=?", 
                      (call.from_user.id, int(time.time()), payment_id))
        conn.commit()
        
        # Уведомляем пользователя
        try:
            bot.send_message(user_id, f"❌ Ваша заявка на группу '{plan_title}' отклонена. Если вы считаете это ошибкой, свяжитесь с администратором.")
        except:
            pass
            
        bot.answer_callback_query(call.id, "❌ Заявка отклонена!")
        try:
            bot.edit_message_caption(f"❌ ЗАЯВКА ОТКЛОНЕНA\n\nПользователь: {username or user_id}\nГруппа: {plan_title}", call.message.chat.id, call.message.message_id)
        except:
            pass

# Управление способами оплаты
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("config_payment:"))
def callback_config_payment(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    payment_type = call.data.split(":")[1]
    
    cursor.execute("SELECT id, name, description, details FROM payment_methods WHERE type=?", (payment_type,))
    method = cursor.fetchone()
    
    if not method:
        bot.answer_callback_query(call.id, "❌ Способ оплаты не найден.")
        return
        
    method_id, name, description, details = method
    
    text = (f"🔧 <b>Настройка способа оплаты: {name}</b>\n\n"
            f"📝 Текущее описание: {description}\n"
            f"💳 Текущие реквизиты: {details or 'Не указаны'}\n\n"
            f"Отправьте новое описание и реквизиты в формате:\n"
            f"Описание|Реквизиты\n\n"
            f"Пример:\nОплата картой|Реквизиты: 0000 0000 0000 0000")
    
    admin_states[call.from_user.id] = {
        "mode": "config_payment",
        "method_id": method_id,
        "chat_id": call.message.chat.id
    }
    
    bot.answer_callback_query(call.id, "✏️ Введите новые настройки")
    bot.send_message(call.message.chat.id, text, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("toggle_payment:"))
def callback_toggle_payment(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    payment_type = call.data.split(":")[1]
    
    cursor.execute("SELECT id, is_active FROM payment_methods WHERE type=?", (payment_type,))
    method = cursor.fetchone()
    
    if not method:
        bot.answer_callback_query(call.id, "❌ Способ оплаты не найден.")
        return
        
    method_id, is_active = method
    new_status = 0 if is_active else 1
    
    cursor.execute("UPDATE payment_methods SET is_active=? WHERE id=?", (new_status, method_id))
    conn.commit()
    
    status_text = "включен" if new_status else "выключен"
    bot.answer_callback_query(call.id, f"✅ Способ оплаты {status_text}!")
    
    # Обновляем сообщение
    methods = get_active_payment_methods()
    text = "💳 <b>Управление способами оплаты</b>\n\n"
    for method_id, name, mtype, description, details in methods:
        status = "✅ Включен" if cursor.execute("SELECT is_active FROM payment_methods WHERE id=?", (method_id,)).fetchone()[0] else "❌ Выключен"
        text += f"<b>{name}</b> ({mtype})\n{description}\nСтатус: {status}\nID: {method_id}\n\n"
    
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("🔧 Настроить карту", callback_data="config_payment:card"),
        types.InlineKeyboardButton("🔧 Настроить ручную", callback_data="config_payment:manual")
    )
    markup.row(
        types.InlineKeyboardButton("🔄 Переключить карту", callback_data="toggle_payment:card"),
        types.InlineKeyboardButton("🔄 Переключить ручную", callback_data="toggle_payment:manual")
    )
    
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
    except:
        pass

@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and admin_states.get(m.from_user.id, {}).get("mode") == "config_payment" and m.chat.type == "private")
def handle_payment_config(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("chat_id") != message.chat.id:
        return
        
    if not message.text or "|" not in message.text:
        bot.send_message(message.chat.id, "❌ Неправильный формат. Используйте: Описание|Реквизиты")
        return
        
    parts = message.text.split("|", 1)
    description = parts[0].strip()
    details = parts[1].strip()
    
    cursor.execute("UPDATE payment_methods SET description=?, details=? WHERE id=?", 
                  (description, details, state["method_id"]))
    conn.commit()
    
    admin_states.pop(uid, None)
    bot.send_message(message.chat.id, "✅ Настройки способа оплаты обновлены!")

# Управление промокодами
@bot.callback_query_handler(func=lambda call: call.data == "create_promo")
def callback_create_promo(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    admin_states[call.from_user.id] = {
        "mode": "create_promo",
        "step": "type",
        "chat_id": call.message.chat.id
    }
    
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("📊 Процентная скидка", callback_data="promo_type:percent"),
        types.InlineKeyboardButton("💵 Фиксированная скидка", callback_data="promo_type:fixed")
    )
    
    bot.answer_callback_query(call.id, "Создание промокода...")
    bot.send_message(call.message.chat.id, "🎫 Создание промокода\n\nВыберите тип скидки:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("promo_type:"))
def callback_promo_type(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    promo_type = call.data.split(":")[1]
    uid = call.from_user.id
    
    if uid not in admin_states or admin_states[uid].get("mode") != "create_promo":
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
        
    admin_states[uid]["promo_type"] = promo_type
    admin_states[uid]["step"] = "value"
    
    if promo_type == "percent":
        text = "Введите размер скидки в процентах (например: 10 для 10%):"
    else:
        text = "Введите размер фиксированной скидки (например: 5.00 для 5 рублей):"
        
    bot.answer_callback_query(call.id, "Введите значение скидки")
    bot.send_message(call.message.chat.id, text)

@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and admin_states.get(m.from_user.id, {}).get("mode") == "create_promo" and admin_states.get(m.from_user.id, {}).get("step") == "value" and m.chat.type == "private")
def handle_promo_value(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("chat_id") != message.chat.id:
        return
        
    promo_type = state.get("promo_type")
    value_text = message.text.strip()
    
    try:
        if promo_type == "percent":
            discount_percent = int(value_text)
            if discount_percent <= 0 or discount_percent > 100:
                raise ValueError
            state["discount_percent"] = discount_percent
            state["discount_fixed_cents"] = 0
        else:
            discount_cents = cents_from_str(value_text)
            if discount_cents <= 0:
                raise ValueError
            state["discount_percent"] = 0
            state["discount_fixed_cents"] = discount_cents
            
        state["step"] = "max_uses"
        bot.send_message(message.chat.id, "Введите максимальное количество использований (или 0 для безлимита):")
        
    except ValueError:
        bot.send_message(message.chat.id, "❌ Неверное значение. Попробуйте снова:")

@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and admin_states.get(m.from_user.id, {}).get("mode") == "create_promo" and admin_states.get(m.from_user.id, {}).get("step") == "max_uses" and m.chat.type == "private")
def handle_promo_max_uses(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("chat_id") != message.chat.id:
        return
        
    try:
        max_uses = int(message.text.strip())
        if max_uses < 0:
            raise ValueError
            
        state["max_uses"] = max_uses if max_uses > 0 else None
        state["step"] = "expires"
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.row(types.KeyboardButton("⏩ Без срока"), types.KeyboardButton("7 дней"))
        markup.row(types.KeyboardButton("30 дней"), types.KeyboardButton("90 дней"))
        
        bot.send_message(message.chat.id, "Выберите срок действия промокода:", reply_markup=markup)
        
    except ValueError:
        bot.send_message(message.chat.id, "❌ Неверное значение. Введите число:")

@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and admin_states.get(m.from_user.id, {}).get("mode") == "create_promo" and admin_states.get(m.from_user.id, {}).get("step") == "expires" and m.chat.type == "private")
def handle_promo_expires(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("chat_id") != message.chat.id:
        return
        
    text = message.text.strip()
    expires_ts = None
    
    if text == "⏩ Без срока":
        expires_ts = None
    elif text == "7 дней":
        expires_ts = int(time.time()) + 7 * 24 * 3600
    elif text == "30 дней":
        expires_ts = int(time.time()) + 30 * 24 * 3600
    elif text == "90 дней":
        expires_ts = int(time.time()) + 90 * 24 * 3600
    else:
        bot.send_message(message.chat.id, "❌ Выберите вариант из кнопок:")
        return
        
    # Генерируем промокод
    code = generate_promo_code()
    
    # Сохраняем в базу
    cursor.execute("""
        INSERT INTO promo_codes (code, discount_percent, discount_fixed_cents, max_uses, created_ts, expires_ts)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (code, state["discount_percent"], state["discount_fixed_cents"], state["max_uses"], int(time.time()), expires_ts))
    conn.commit()
    
    # Формируем информацию о промокоде
    promo_info = f"🎫 Промокод: {code}\n"
    if state["discount_percent"]:
        promo_info += f"📊 Скидка: {state['discount_percent']}%\n"
    else:
        promo_info += f"💵 Скидка: {price_str_from_cents(state['discount_fixed_cents'])}\n"
    
    promo_info += f"🔄 Макс. использований: {state['max_uses'] or 'безлимит'}\n"
    
    if expires_ts:
        expires_str = datetime.fromtimestamp(expires_ts, LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S UTC')
        promo_info += f"⏰ Действует до: {expires_str}\n"
    else:
        promo_info += "⏰ Срок действия: бессрочно\n"
    
    admin_states.pop(uid, None)
    
    # Возвращаем обычную клавиатуру
    bot.send_message(message.chat.id, f"✅ Промокод создан!\n\n{promo_info}", parse_mode="HTML", reply_markup=main_menu(uid))

@bot.callback_query_handler(func=lambda call: call.data == "list_promos")
def callback_list_promos(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    cursor.execute("SELECT code, discount_percent, discount_fixed_cents, is_active, used_count, max_uses, expires_ts FROM promo_codes ORDER BY created_ts DESC")
    promos = cursor.fetchall()
    
    if not promos:
        bot.answer_callback_query(call.id, "📭 Нет промокодов.")
        return
        
    text = "📋 Список промокодов:\n\n"
    
    for promo in promos:
        code, discount_percent, discount_fixed_cents, is_active, used_count, max_uses, expires_ts = promo
        
        text += f"🎫 {code}\n"
        if discount_percent:
            text += f"📊 Скидка: {discount_percent}%\n"
        else:
            text += f"💵 Скидка: {price_str_from_cents(discount_fixed_cents)}\n"
            
        status = "✅ Активен" if is_active else "❌ Неактивен"
        text += f"📊 Статус: {status}\n"
        text += f"🔄 Использован: {used_count} раз"
        if max_uses:
            text += f" из {max_uses}\n"
        else:
            text += " (безлимит)\n"
            
        if expires_ts:
            expires_str = datetime.utcfromtimestamp(expires_ts).strftime('%Y-%m-%d %H:%M:%S UTC')
            text += f"⏰ Действует до: {expires_str}\n"
        else:
            text += "⏰ Срок: бессрочно\n"
            
        text += "\n"
    
    bot.answer_callback_query(call.id, "📋 Список промокодов")
    bot.send_message(call.message.chat.id, text, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "cancel")
def callback_cancel(call):
    bot.answer_callback_query(call.id, "Отменено.")

# ----------------- Notification system -----------------
def send_payment_notifications():
    """Отправляет уведомления о необходимости оплаты"""
    try:
        now = now_local()
        current_hour = now.hour
        current_minute = now.minute
        
        # Проверяем условия ТОЛЬКО в определенное время (чтобы не выполнялось каждые 5 минут)
        
        # 1-го числа в 10:00 - уведомление о начале периода оплаты
        if now.day == 1 and current_hour == 10 and current_minute == 0:
            logging.info("📢 Отправка уведомлений: 1-е число, начало месяца")
            current_month, current_year = get_current_period()
            
            # Находим пользователей с активными подписками за предыдущий период
            cursor.execute("""
                SELECT DISTINCT s.user_id, u.username 
                FROM subscriptions s
                JOIN users u ON s.user_id = u.user_id
                WHERE s.active = 1 AND (s.current_period_month != ? OR s.current_period_year != ?)
            """, (current_month, current_year))
            
            users = cursor.fetchall()
            
            for user_id, username in users:
                try:
                    text = (
                        "📅 <b>Напоминание об оплате на новый месяц</b>\n\n"
                        "Наступил новый месяц! Для продолжения доступа к группе обучения необходимо продлить подписку.\n\n"
                        "💳 <b>Период оплаты:</b> 1-5 число\n"
                        "⏰ <b>До 5 числа</b> вы можете:\n"
                        "• Оплатить полную сумму за месяц\n"
                        "• Или оплатить первую часть (вторая часть оплачивается 15-20 числа)\n\n"
                        "Если оплата не поступит до 5 числа 23:59, доступ к группе будет приостановлен."
                    )
                    
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("🔄 Продлить подписку", callback_data="renew_subscription"))
                    
                    bot.send_message(user_id, text, parse_mode="HTML", reply_markup=markup)
                    
                    # Обновляем время последнего уведомления
                    cursor.execute("""
                        UPDATE subscriptions 
                        SET last_notification_ts = ? 
                        WHERE user_id = ? AND active = 1
                    """, (int(time.time()), user_id))
                    conn.commit()
                    
                    logging.info(f"📨 Отправлено уведомление пользователю {user_id}")
                    
                except Exception as e:
                    logging.error(f"Error sending notification to user {user_id}: {e}")
        
        # 15-го числа в 10:00 - уведомление о второй части оплаты
        elif now.day == 15 and current_hour == 10 and current_minute == 0:
            logging.info("📢 Отправка уведомлений: 15-е число, вторая часть")
            current_month, current_year = get_current_period()
            
            # Находим пользователей с оплаченной первой частью
            cursor.execute("""
                SELECT DISTINCT s.user_id, u.username, p.title 
                FROM subscriptions s
                JOIN users u ON s.user_id = u.user_id
                JOIN plans p ON s.plan_id = p.id
                WHERE s.active = 1 AND s.payment_type = 'partial' 
                AND s.part_paid = 'first' 
                AND s.current_period_month = ? AND s.current_period_year = ?
            """, (current_month, current_year))
            
            users = cursor.fetchall()
            
            for user_id, username, plan_title in users:
                try:
                    cursor.execute("SELECT price_cents FROM plans WHERE id = (SELECT plan_id FROM subscriptions WHERE user_id = ? LIMIT 1)", (user_id,))
                    price_cents = cursor.fetchone()[0]
                    second_part_price = price_cents // 2
                    
                    text = (
                        "📅 <b>Напоминание о второй части оплаты</b>\n\n"
                        f"Группа: <b>{plan_title}</b>\n"
                        f"💵 Сумма к оплате: {price_str_from_cents(second_part_price)}\n\n"
                        "💳 <b>Период оплаты:</b> 15-20 число\n"
                        "⏰ <b>До 20 числа 23:59</b> необходимо оплатить вторую часть.\n"
                        "Если оплата не поступит, доступ к группе будет приостановлен."
                    )
                    
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("💳 Оплатить вторую часть", callback_data="pay_second_part"))
                    
                    bot.send_message(user_id, text, parse_mode="HTML", reply_markup=markup)
                    
                    logging.info(f"📨 Отправлено уведомление о второй части пользователю {user_id}")
                    
                except Exception as e:
                    logging.error(f"Error sending second part notification to user {user_id}: {e}")
                    
        # 4-го числа в 18:00 - Напоминание о скором дедлайне первой части
        elif now.day == 4 and current_hour == 18 and current_minute == 0:
            logging.info("📢 Отправка уведомлений: 4-е число, дедлайн первой части")
            current_month, current_year = get_current_period()
            
            # Пользователи, которые еще не оплатили за этот месяц
            cursor.execute("""
                SELECT DISTINCT s.user_id, u.username 
                FROM subscriptions s
                JOIN users u ON s.user_id = u.user_id
                WHERE s.active = 1 
                AND (s.current_period_month != ? OR s.current_period_year != ? OR s.part_paid = 'none')
            """, (current_month, current_year))
            
            users = cursor.fetchall()
            
            for user_id, username in users:
                try:
                    text = (
                        "⏰ <b>Напоминание о дедлайне!</b>\n\n"
                        "Завтра заканчивается период оплаты подписки!\n\n"
                        "💳 <b>Успейте оплатить до 5 числа 23:59</b>\n"
                        "• Полная оплата - доступ до 5 числа следующего месяца\n"
                        "• Первая часть - доступ до 15 числа + вторая часть 15-20 числа\n\n"
                        "После 5 числа доступ к группе будет приостановлен."
                    )
                    
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("🔄 Продлить подписку", callback_data="renew_subscription"))
                    
                    bot.send_message(user_id, text, parse_mode="HTML", reply_markup=markup)
                    
                    logging.info(f"📨 Отправлено уведомление о дедлайне пользователю {user_id}")
                    
                except Exception as e:
                    logging.error(f"Error sending deadline notification to user {user_id}: {e}")
        
        # 19-го числа в 18:00 - Напоминание о скором дедлайне второй части
        elif now.day == 19 and current_hour == 18 and current_minute == 0:
            logging.info("📢 Отправка уведомлений: 19-е число, дедлайн второй части")
            current_month, current_year = get_current_period()
            
            # Пользователи с частичной оплатой
            cursor.execute("""
                SELECT DISTINCT s.user_id, u.username, p.title 
                FROM subscriptions s
                JOIN users u ON s.user_id = u.user_id
                JOIN plans p ON s.plan_id = p.id
                WHERE s.active = 1 AND s.payment_type = 'partial' 
                AND s.part_paid = 'first' 
                AND s.current_period_month = ? AND s.current_period_year = ?
            """, (current_month, current_year))
            
            users = cursor.fetchall()
            
            for user_id, username, plan_title in users:
                try:
                    cursor.execute("SELECT price_cents FROM plans WHERE id = (SELECT plan_id FROM subscriptions WHERE user_id = ? LIMIT 1)", (user_id,))
                    price_cents = cursor.fetchone()[0]
                    second_part_price = price_cents // 2
                    
                    text = (
                        "⏰ <b>Напоминание о дедлайне второй части!</b>\n\n"
                        f"Группа: <b>{plan_title}</b>\n"
                        f"💵 Сумма к оплате: {price_str_from_cents(second_part_price)}\n\n"
                        "Завтра заканчивается период оплаты второй части!\n\n"
                        "💳 <b>Успейте оплатить до 20 числа 23:59</b>\n"
                        "После 20 числа доступ к группе будет приостановлен."
                    )
                    
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("💳 Оплатить вторую часть", callback_data="pay_second_part"))
                    
                    bot.send_message(user_id, text, parse_mode="HTML", reply_markup=markup)
                    
                    logging.info(f"📨 Отправлено уведомление о дедлайне второй части пользователю {user_id}")
                    
                except Exception as e:
                    logging.error(f"Error sending second part deadline notification to user {user_id}: {e}")
                    
    except Exception as e:
        logging.error(f"Error in send_payment_notifications: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "pay_second_part")
def callback_pay_second_part(call):
    """Оплата второй части из уведомления"""
    user_id = call.from_user.id
    
    # Находим активные подписки пользователя, ожидающие вторую часть
    cursor.execute("""
        SELECT s.plan_id, p.title 
        FROM subscriptions s
        JOIN plans p ON s.plan_id = p.id
        WHERE s.user_id = ? AND s.active = 1 AND s.payment_type = 'partial' 
        AND s.part_paid = 'first'
        AND s.current_period_month = ? AND s.current_period_year = ?
        LIMIT 1
    """, (user_id, *get_current_period()))
    
    subscription = cursor.fetchone()
    
    if not subscription:
        bot.answer_callback_query(call.id, "❌ Нет подписок, ожидающих вторую часть оплаты")
        return
    
    plan_id, plan_title = subscription
    
    # Показываем варианты оплаты второй части
    show_plan_full_info(call.message.chat.id, user_id, plan_id, show_back_button=True)
    bot.answer_callback_query(call.id, "💳 Выберите способ оплаты второй части")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("get_link:"))
def callback_get_link(call):
    """Получение ссылки для активной подписки"""
    try:
        sub_id = int(call.data.split(":")[1])
        user_id = call.from_user.id
        
        cursor.execute("""
            SELECT s.invite_link, p.title, s.end_ts, s.active
            FROM subscriptions s
            LEFT JOIN plans p ON s.plan_id = p.id
            WHERE s.id = ? AND s.user_id = ?
        """, (sub_id, user_id))
        
        subscription = cursor.fetchone()
        
        if not subscription:
            bot.answer_callback_query(call.id, "❌ Подписка не найдена")
            return
            
        invite_link, plan_title, end_ts, active = subscription
        
        if not active or end_ts < int(time.time()):
            bot.answer_callback_query(call.id, "❌ Подписка неактивна или истекла")
            return
            
        if not invite_link:
            # Генерируем новую ссылку
            cursor.execute("SELECT group_id FROM subscriptions WHERE id = ?", (sub_id,))
            group_id = cursor.fetchone()[0]
            new_link = create_chat_invite_link_one_time(BOT_TOKEN, group_id, expire_seconds=7*24*3600, member_limit=1)
            
            if new_link:
                cursor.execute("UPDATE subscriptions SET invite_link = ? WHERE id = ?", (new_link, sub_id))
                conn.commit()
                invite_link = new_link
            else:
                bot.answer_callback_query(call.id, "❌ Не удалось создать ссылку")
                return
        
        text = (f"🔗 <b>Ссылка для группы '{plan_title}'</b>\n\n"
                f"{invite_link}\n\n"
                f"⚠️ Ссылка одноразовая, действует 7 дней")
        
        bot.answer_callback_query(call.id, "🔗 Ссылка отправлена")
        bot.send_message(call.message.chat.id, text, parse_mode="HTML")
        
    except Exception as e:
        logging.exception("Error in callback_get_link")
        bot.answer_callback_query(call.id, "❌ Ошибка получения ссылки")

@bot.callback_query_handler(func=lambda call: call.data == "show_plans_notification")
def callback_show_plans_notification(call):
    """Показывает группы обучения при нажатии на уведомление"""
    bot.answer_callback_query(call.id, "📋 Показываем группы обучения")
    show_plans(call.message)

# ----------------- Expiration and cleanup system -----------------
def check_expirations_loop():
    """Проверяет истечение сроков оплаты и удаляет неуплативших"""
    last_check_hour = -1  # Храним последний час проверки
    
    while True:
        try:
            now = now_local()
            current_hour = now.hour
            current_minute = now.minute
            
            # Проверяем уведомления ТОЛЬКО если час изменился (чтобы не проверять каждую минуту)
            if current_hour != last_check_hour:
                last_check_hour = current_hour
                
                # Проверяем условия для отправки уведомлений в определенные часы
                if (now.day == 1 and current_hour == 10 and current_minute == 0) or \
                   (now.day == 15 and current_hour == 10 and current_minute == 0) or \
                   (now.day == 4 and current_hour == 18 and current_minute == 0) or \
                   (now.day == 19 and current_hour == 18 and current_minute == 0):
                    logging.info(f"🕐 Проверка времени для уведомлений: {now.day}.{now.month} {current_hour}:{current_minute}")
                    send_payment_notifications()
            
            current_month, current_year = get_current_period()
            
            # 6-го числа в 00:00 - удаляем тех, кто вообще не оплатил
            if now.day == 6 and now.hour == 0 and now.minute == 0:
                logging.info("🔄 Проверка экспирации: удаление неплативших пользователей")
                
                cursor.execute("""
                    SELECT s.id, s.user_id, s.group_id, s.plan_id, p.title, u.username
                    FROM subscriptions s
                    JOIN plans p ON s.plan_id = p.id
                    JOIN users u ON s.user_id = u.user_id
                    WHERE s.active = 1 
                    AND (s.current_period_month != ? OR s.current_period_year != ?)
                    AND s.part_paid = 'none'  # Удаляем только тех, у кого ВООБЩЕ ничего не оплачено
                """, (current_month, current_year))
                
                expired_subs = cursor.fetchall()
                logging.info(f"📊 Найдено {len(expired_subs)} подписок для удаления (не оплатили вовсе)")
                
                for sub_id, user_id, group_id, plan_id, plan_title, username in expired_subs:
                    try:
                        # Пытаемся удалить из группы
                        if group_id:
                            try:
                                bot.ban_chat_member(group_id, user_id, until_date=int(time.time()) + 30)
                                logging.info(f"👤 Удален пользователь {username or user_id} из группы {group_id}")
                                time.sleep(0.1)
                            except Exception as e:
                                logging.warning(f"❌ Не удалось удалить пользователя {user_id} из группы {group_id}: {e}")
                        
                        # Деактивируем подписку
                        cursor.execute("UPDATE subscriptions SET active = 0, removed = 1 WHERE id = ?", (sub_id,))
                        conn.commit()
                        
                        # Уведомляем пользователя
                        try:
                            bot.send_message(user_id, 
                                           f"❌ Доступ к группе '{plan_title}' приостановлен.\n\n"
                                           "Вы не оплатили подписку за текущий месяц. "
                                           "Для восстановления доступа оплатите подписку в разделе '📋 Группы обучения'.")
                            logging.info(f"📢 Отправлено уведомление пользователю {username or user_id}")
                        except Exception as e:
                            logging.warning(f"❌ Не удалось отправить уведомление пользователю {user_id}: {e}")
                            
                    except Exception as e:
                        logging.error(f"❌ Ошибка обработки expired подписки {sub_id}: {e}")
            
            # 21-го числа в 00:00 - удаляем тех, кто оплатил только первую часть
            elif now.day == 21 and now.hour == 0 and now.minute == 0:
                logging.info("🔄 Проверка экспирации: удаление пользователей с частичной оплатой")
                
                cursor.execute("""
                    SELECT s.id, s.user_id, s.group_id, s.plan_id, p.title, u.username
                    FROM subscriptions s
                    JOIN plans p ON s.plan_id = p.id
                    JOIN users u ON s.user_id = u.user_id
                    WHERE s.active = 1 
                    AND s.payment_type = 'partial' 
                    AND s.part_paid = 'first'  # Только первую часть оплатили
                    AND s.current_period_month = ? AND s.current_period_year = ?
                """, (current_month, current_year))
                
                expired_partial_subs = cursor.fetchall()
                logging.info(f"📊 Найдено {len(expired_partial_subs)} подписок с частичной оплатой для удаления")
                
                for sub_id, user_id, group_id, plan_id, plan_title, username in expired_partial_subs:
                    try:
                        # Пытаемся удалить из группы
                        if group_id:
                            try:
                                bot.ban_chat_member(group_id, user_id, until_date=int(time.time()) + 30)
                                logging.info(f"👤 Удален пользователь {username or user_id} из группы {group_id} (частичная оплата)")
                                time.sleep(0.1)
                            except Exception as e:
                                logging.warning(f"❌ Не удалось удалить пользователя {user_id} из группы {group_id}: {e}")
                        
                        # Деактивируем подписку
                        cursor.execute("UPDATE subscriptions SET active = 0, removed = 1 WHERE id = ?", (sub_id,))
                        conn.commit()
                        
                        # Уведомляем пользователя
                        try:
                            bot.send_message(user_id, 
                                           f"❌ Доступ к группе '{plan_title}' приостановлен.\n\n"
                                           "Вы не оплатили вторую часть подписки. "
                                           "Для восстановления доступа оплатите вторую часть в разделе '📋 Группы обучения'.")
                            logging.info(f"📢 Отправлено уведомление пользователю {username or user_id} о частичной оплате")
                        except Exception as e:
                            logging.warning(f"❌ Не удалось отправить уведомление пользователю {user_id}: {e}")
                            
                    except Exception as e:
                        logging.error(f"❌ Ошибка обработки expired частичной подписки {sub_id}: {e}")
            
            # Ежедневно проверяем истекшие подписки (на всякий случай)
            elif now.hour == 1 and now.minute == 0:  # Каждый день в 01:00
                logging.info("🔄 Ежедневная проверка истекших подписок")
                
                cursor.execute("""
                    SELECT s.id, s.user_id, s.group_id, s.plan_id, p.title, u.username
                    FROM subscriptions s
                    JOIN plans p ON s.plan_id = p.id
                    JOIN users u ON s.user_id = u.user_id
                    WHERE s.active = 1 AND s.end_ts < ?
                """, (int(time.time()),))
                
                expired_subs = cursor.fetchall()
                
                if expired_subs:
                    logging.info(f"📊 Найдено {len(expired_subs)} подписок с истекшим сроком")
                    
                    for sub_id, user_id, group_id, plan_id, plan_title, username in expired_subs:
                        try:
                            # Пытаемся удалить из группы
                            if group_id:
                                try:
                                    bot.ban_chat_member(group_id, user_id, until_date=int(time.time()) + 30)
                                    logging.info(f"👤 Удален пользователь {username or user_id} из группы {group_id} (истек срок)")
                                    time.sleep(0.1)
                                except Exception as e:
                                    logging.warning(f"❌ Не удалось удалить пользователя {user_id} из группы {group_id}: {e}")
                            
                            # Деактивируем подписку
                            cursor.execute("UPDATE subscriptions SET active = 0, removed = 1 WHERE id = ?", (sub_id,))
                            conn.commit()
                            
                        except Exception as e:
                            logging.error(f"❌ Ошибка обработки daily expired подписки {sub_id}: {e}")
            
            time.sleep(60)  # Проверяем каждую минуту
            
        except Exception as e:
            logging.exception("❌ Критическая ошибка в check_expirations_loop")
            time.sleep(60)  # Ждем минуту перед повторной попыткой

# Запускаем фоновые процессы
threading.Thread(target=check_expirations_loop, daemon=True).start()

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("editplan:"))
def callback_edit_plan(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    pid = int(call.data.split(":")[1])
    
    # Получаем информацию о группе
    cursor.execute("""
        SELECT p.id, p.title, p.price_cents, p.description, p.group_id, p.media_file_ids, p.media_type
        FROM plans p
        WHERE p.id=?
    """, (pid,))
    
    plan = cursor.fetchone()
    if not plan:
        bot.answer_callback_query(call.id, "❌ Группа не найдена.")
        return
        
    plan_id, title, price_cents, description, group_id, media_file_ids, media_type = plan
    
    # Инициализируем состояние редактирования
    uid = call.from_user.id
    admin_states[uid] = {
        "mode": "edit",
        "step": "edit_choice",
        "plan_id": plan_id,
        "current_title": title,
        "current_price": price_cents,
        "current_description": description,
        "current_group_id": group_id,
        "media_files": media_file_ids.split(",") if media_file_ids else [],
        "media_type": media_type,
        "chat_id": call.message.chat.id
    }
    
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("📝 Ред. название", callback_data=f"edit_field:title:{plan_id}"),
        types.InlineKeyboardButton("💰 Ред. цену", callback_data=f"edit_field:price:{plan_id}")
    )
    markup.row(
        types.InlineKeyboardButton("📋 Ред. описание", callback_data=f"edit_field:description:{plan_id}"),
        types.InlineKeyboardButton("👥 Изменить группу", callback_data=f"edit_field:group:{plan_id}")
    )
    markup.row(
        types.InlineKeyboardButton("✏️🖼️ медиа", callback_data=f"edit_field:media:{plan_id}"),
        types.InlineKeyboardButton("✅ Завершить редактирование", callback_data=f"edit_finish:{plan_id}")
    )
    
    text = f"✏️ <b>Редактирование группы:</b> {title}\n\nВыберите что хотите изменить:"
    
    bot.answer_callback_query(call.id, "✏️ Режим редактирования")
    bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "renew_subscription")
def callback_renew_subscription(call):
    """Продление существующей подписки"""
    user_id = call.from_user.id
    
    # Находим активные подписки пользователя
    cursor.execute("""
        SELECT s.id, s.plan_id, p.title, s.part_paid, s.current_period_month, s.current_period_year
        FROM subscriptions s
        JOIN plans p ON s.plan_id = p.id
        WHERE s.user_id = ? AND s.active = 1
        ORDER BY s.end_ts DESC
        LIMIT 1
    """, (user_id,))
    
    subscription = cursor.fetchone()
    
    if not subscription:
        bot.answer_callback_query(call.id, "❌ У вас нет активных подписок для продления")
        return
    
    sub_id, plan_id, plan_title, part_paid, period_month, period_year = subscription
    current_month, current_year = get_current_period()
    
    # Проверяем, нужно ли продлевать
    if period_month == current_month and period_year == current_year and part_paid == 'full':
        bot.answer_callback_query(call.id, "✅ Ваша подписка уже оплачена за этот месяц")
        return
    
    # Показываем варианты оплаты для продления
    show_plan_full_info(call.message.chat.id, user_id, plan_id, show_back_button=True)
    bot.answer_callback_query(call.id, "💳 Выберите вариант оплаты")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_field:"))
def callback_edit_field(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    parts = call.data.split(":")
    field = parts[1]
    plan_id = int(parts[2])
    uid = call.from_user.id
    
    state = admin_states.get(uid)
    if not state or state.get("mode") != "edit" or state.get("plan_id") != plan_id:
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
    
    state["step"] = f"editing_{field}"
    
    if field == "title":
        bot.send_message(call.message.chat.id, f"✏️ Текущее название: {state['current_title']}\nВведите новое название:")
    elif field == "price":
        bot.send_message(call.message.chat.id, f"✏️ Текущая цена: {price_str_from_cents(state['current_price'])}\nВведите новую цену (например: 14.99):")
    elif field == "description":
        bot.send_message(call.message.chat.id, f"✏️ Текущее описание: {state['current_description']}\nВведите новое описание:")
    elif field == "group":
        groups = get_all_groups_with_bot()
        markup = types.InlineKeyboardMarkup()
        for chat_id, title, chat_type in groups:
            markup.add(types.InlineKeyboardButton(f"{title} ({chat_type})", callback_data=f"select_edit_group:{chat_id}:{plan_id}"))
        
        cursor.execute("SELECT title FROM managed_groups WHERE chat_id=?", (state['current_group_id'],))
        current_group = cursor.fetchone()
        current_group_title = current_group[0] if current_group else "Неизвестно"
        
        bot.send_message(call.message.chat.id, 
                        f"👥 Текущая группа: {current_group_title}\nВыберите новую группу:",
                        reply_markup=markup)
    elif field == "media":
        # Показываем меню управления медиа вместо прямого перехода к добавлению
        show_media_management_menu(call.message.chat.id, state)
    
    bot.answer_callback_query(call.id, f"Редактирование {field}")

def show_media_management_menu(chat_id, state):
    """Показывает меню управления медиа"""
    plan_id = state["plan_id"]
    media_count = len(state.get("media_files", []))
    
    text = f"🖼️ <b>Управление медиа для группы '{state['current_title']}'</b>\n\n"
    text += f"📊 Текущее количество медиа: {media_count}\n\n"
    
    if media_count > 0:
        text += "✅ Медиа загружены. Вы можете:\n• Добавить новые медиа\n• Удалить все текущие медиа\n• Просмотреть текущие медиа"
    else:
        text += "📭 Медиа отсутствуют. Вы можете добавить новые медиа."
    
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("➕ Добавить медиа", callback_data=f"add_media:{plan_id}"),
        types.InlineKeyboardButton("🗑️ Удалить все медиа", callback_data=f"clear_media:{plan_id}")
    )
    
    if media_count > 0:
        markup.row(types.InlineKeyboardButton("👀 Просмотреть текущие медиа", callback_data=f"view_current_media:{plan_id}"))
    
    markup.row(types.InlineKeyboardButton("🔙 Назад к редактированию", callback_data=f"back_to_edit:{plan_id}"))
    
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("add_media:"))
def callback_add_media(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    plan_id = int(call.data.split(":")[1])
    uid = call.from_user.id
    
    state = admin_states.get(uid)
    if not state or state.get("mode") != "edit" or state.get("plan_id") != plan_id:
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
    
    state["step"] = "adding_media"
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row(types.KeyboardButton("✅ Завершить добавление медиа"))
    markup.row(types.KeyboardButton("🔙 Назад к управлению медиа"))
    
    bot.send_message(call.message.chat.id, 
                    "📎 Отправляйте фото или видео для добавления.\n\n"
                    "💡 <b>Примечание:</b> Новые медиа заменят существующие.\n"
                    "Когда закончите - нажмите '✅ Завершить добавление медиа'.",
                    parse_mode="HTML", reply_markup=markup)
    bot.answer_callback_query(call.id, "Добавление медиа...")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("clear_media:"))
def callback_clear_media(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    plan_id = int(call.data.split(":")[1])
    uid = call.from_user.id
    
    state = admin_states.get(uid)
    if not state or state.get("mode") != "edit" or state.get("plan_id") != plan_id:
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
    
    # Удаляем все медиа из базы
    cursor.execute("DELETE FROM plan_media WHERE plan_id=?", (plan_id,))
    cursor.execute("UPDATE plans SET media_file_id=NULL, media_file_ids=NULL, media_type=NULL WHERE id=?", (plan_id,))
    conn.commit()
    
    # Обновляем состояние
    state["media_files"] = []
    state["media_type"] = None
    
    bot.answer_callback_query(call.id, "✅ Все медиа удалены!")
    
    # Показываем меню управления медиа снова
    show_media_management_menu(call.message.chat.id, state)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("view_current_media:"))
def callback_view_current_media(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    plan_id = int(call.data.split(":")[1])
    uid = call.from_user.id
    
    state = admin_states.get(uid)
    if not state or state.get("mode") != "edit" or state.get("plan_id") != plan_id:
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
    
    # Отправляем текущие медиа
    media_files = state.get("media_files", [])
    media_type = state.get("media_type")
    
    if not media_files:
        bot.answer_callback_query(call.id, "📭 Нет медиа для просмотра")
        return
    
    bot.answer_callback_query(call.id, "📦 Отправляем текущие медиа...")
    
    try:
        # Отправляем первое медиа с описанием
        if media_type == "photo":
            bot.send_photo(call.message.chat.id, media_files[0], 
                          caption=f"🖼️ Текущие медиа ({len(media_files)} шт.)\nПервый элемент из {len(media_files)}")
        elif media_type == "video":
            bot.send_video(call.message.chat.id, media_files[0],
                          caption=f"🎥 Текущие медиа ({len(media_files)} шт.)\nПервый элемент из {len(media_files)}")
        
        # Если есть еще медиа, отправляем остальные (ограничим 5)
        if len(media_files) > 1:
            remaining_media = media_files[1:5]  # Ограничиваем 5 медиа
            media_group = []
            
            for file_id in remaining_media:
                if media_type == "photo":
                    media_group.append(types.InputMediaPhoto(file_id))
                elif media_type == "video":
                    media_group.append(types.InputMediaVideo(file_id))
            
            if media_group:
                bot.send_media_group(call.message.chat.id, media_group)
                
            if len(media_files) > 5:
                bot.send_message(call.message.chat.id, f"📁 ... и еще {len(media_files) - 5} медиа")
                
    except Exception as e:
        logging.error(f"Error sending media: {e}")
        bot.send_message(call.message.chat.id, "❌ Ошибка при отправке медиа")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("back_to_edit:"))
def callback_back_to_edit(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    plan_id = int(call.data.split(":")[1])
    uid = call.from_user.id
    
    state = admin_states.get(uid)
    if not state or state.get("mode") != "edit" or state.get("plan_id") != plan_id:
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
    
    # Возвращаемся к меню редактирования
    show_edit_menu(call.message.chat.id, state)
    bot.answer_callback_query(call.id, "🔙 Назад к редактированию")

# Обработчик медиа в режиме добавления
@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and 
                    admin_states.get(m.from_user.id, {}).get("mode") == "edit" and 
                    admin_states.get(m.from_user.id, {}).get("step") == "adding_media" and
                    m.chat.type == "private")
def handle_adding_media(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("chat_id") != message.chat.id:
        return

    if message.photo:
        file_id = message.photo[-1].file_id
        state.setdefault("media_files", []).append(file_id)
        state["media_type"] = "photo"
        bot.send_message(message.chat.id, f"✅ Фото добавлено! Всего: {len(state['media_files'])}")
        return
        
    if message.video:
        file_id = message.video.file_id
        state.setdefault("media_files", []).append(file_id)
        state["media_type"] = "video"
        bot.send_message(message.chat.id, f"✅ Видео добавлено! Всего: {len(state['media_files'])}")
        return
        
    if message.text:
        txt = message.text.strip()
        if txt == "✅ Завершить добавление медиа":
            # Сохраняем новые медиа
            media_files = state.get("media_files", [])
            media_type = state.get("media_type")
            
            if media_files:
                first_media = media_files[0]
                media_ids_str = ",".join(media_files)
                
                # Обновляем медиа в базе
                cursor.execute("UPDATE plans SET media_file_id=?, media_file_ids=?, media_type=? WHERE id=?", 
                              (first_media, media_ids_str, media_type, state["plan_id"]))
                
                # Очищаем старые медиа и добавляем новые
                cursor.execute("DELETE FROM plan_media WHERE plan_id=?", (state["plan_id"],))
                for idx, fid in enumerate(media_files):
                    cursor.execute("INSERT INTO plan_media (plan_id, file_id, media_type, ord, added_ts) VALUES (?, ?, ?, ?, ?)",
                                  (state["plan_id"], fid, media_type, idx, int(time.time())))
                
                conn.commit()
                
                cnt = len(media_files)
                bot.send_message(message.chat.id, 
                               f"✅ Медиа обновлены!\n📊 Загружено {cnt} медиа", 
                               reply_markup=types.ReplyKeyboardRemove())
            else:
                bot.send_message(message.chat.id, 
                               "✅ Медиа не изменены", 
                               reply_markup=types.ReplyKeyboardRemove())
            
            state["step"] = "edit_choice"
            # Показываем меню управления медиа снова
            show_media_management_menu(message.chat.id, state)
            return
            
        elif txt == "🔙 Назад к управлению медиа":
            # Возвращаемся к управлению медиа без сохранения
            state["step"] = "edit_choice"
            show_media_management_menu(message.chat.id, state)
            return
            
        bot.send_message(message.chat.id, "❌ Отправляйте фото или видео, или используйте кнопки")

@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and 
                    admin_states.get(m.from_user.id, {}).get("mode") == "edit" and 
                    admin_states.get(m.from_user.id, {}).get("step") == "adding_media" and
                    m.chat.type == "private",
                    content_types=['photo', 'video'])
def handle_edit_media_adding(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("chat_id") != message.chat.id:
        return

    if message.photo:
        file_id = message.photo[-1].file_id
        state.setdefault("media_files", []).append(file_id)
        state["media_type"] = "photo"
        bot.send_message(message.chat.id, f"✅ Фото добавлено! Всего: {len(state['media_files'])}")
        return
        
    if message.video:
        file_id = message.video.file_id
        state.setdefault("media_files", []).append(file_id)
        state["media_type"] = "video"
        bot.send_message(message.chat.id, f"✅ Видео добавлено! Всего: {len(state['media_files'])}")
        return

# Обработчик медиа в режиме редактирования (используем ту же логику что и при создании)
@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and 
                    admin_states.get(m.from_user.id, {}).get("mode") == "edit" and 
                    admin_states.get(m.from_user.id, {}).get("step") == "media" and
                    m.chat.type == "private",
                    content_types=['text', 'photo', 'video'])
def handle_edit_media(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("chat_id") != message.chat.id:
        return

    if message.photo:
        file_id = message.photo[-1].file_id
        state.setdefault("media_files", []).append(file_id)
        state["media_type"] = "photo"
        bot.send_message(message.chat.id, f"✅ Фото добавлено! Всего: {len(state['media_files'])}")
        return
        
    if message.video:
        file_id = message.video.file_id
        state.setdefault("media_files", []).append(file_id)
        state["media_type"] = "video"
        bot.send_message(message.chat.id, f"✅ Видео добавлено! Всего: {len(state['media_files'])}")
        return
        
    if message.text:
        txt = message.text.strip()
        if txt == "⏩ Пропустить медиа":
            # Сохраняем группу без изменений медиа
            state["step"] = "edit_choice"
            bot.send_message(message.chat.id, "✅ Медиа не изменены.", reply_markup=types.ReplyKeyboardRemove())
            # Показываем меню редактирования снова
            show_edit_menu(message.chat.id, state)
            return
            
        if txt == "✅ Завершить добавление медиа":
            # Сохраняем новые медиа
            media_files = state.get("media_files", [])
            media_type = state.get("media_type")
            
            if media_files:
                first_media = media_files[0]
                media_ids_str = ",".join(media_files)
                
                # Обновляем медиа в базе
                cursor.execute("UPDATE plans SET media_file_id=?, media_file_ids=?, media_type=? WHERE id=?", 
                              (first_media, media_ids_str, media_type, state["plan_id"]))
                
                # Очищаем старые медиа и добавляем новые
                cursor.execute("DELETE FROM plan_media WHERE plan_id=?", (state["plan_id"],))
                for idx, fid in enumerate(media_files):
                    cursor.execute("INSERT INTO plan_media (plan_id, file_id, media_type, ord, added_ts) VALUES (?, ?, ?, ?, ?)",
                                  (state["plan_id"], fid, media_type, idx, int(time.time())))
                
                conn.commit()
                
                cnt = len(media_files)
                if cnt == 1:
                    bot.send_message(message.chat.id, f"✅ Медиа обновлены! Использовано 1 превью.", reply_markup=types.ReplyKeyboardRemove())
                else:
                    bot.send_message(message.chat.id, f"✅ Медиа обновлены! Использовано первое из {cnt} медиа как превью.", reply_markup=types.ReplyKeyboardRemove())
            else:
                bot.send_message(message.chat.id, "✅ Медиа не изменены (оставлены предыдущие).", reply_markup=types.ReplyKeyboardRemove())
            
            state["step"] = "edit_choice"
            # Показываем меню редактирования снова
            show_edit_menu(message.chat.id, state)
            return
            
        bot.send_message(message.chat.id, "❌ Отправляйте фото/видео или используйте кнопки '⏩ Пропустить медиа' / '✅ Завершить добавление медиа'.")

def show_edit_menu(chat_id, state):
    """Показывает меню редактирования"""
    plan_id = state["plan_id"]
    
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("📝 Ред. название", callback_data=f"edit_field:title:{plan_id}"),
        types.InlineKeyboardButton("💰 Ред. цену", callback_data=f"edit_field:price:{plan_id}")
    )
    markup.row(
        types.InlineKeyboardButton("📋 Ред. описание", callback_data=f"edit_field:description:{plan_id}"),
        types.InlineKeyboardButton("👥 Изменить группу", callback_data=f"edit_field:group:{plan_id}")
    )
    markup.row(
        types.InlineKeyboardButton("🖼️ Управление медиа", callback_data=f"edit_field:media:{plan_id}"),
        types.InlineKeyboardButton("✅ Завершить редактирование", callback_data=f"edit_finish:{plan_id}")
    )
    
    text = f"✏️ <b>Редактирование группы:</b> {state['current_title']}\n\nВыберите что хотите изменить:"
    
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

# Обработчик выбора группы при редактировании
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("select_edit_group:"))
def callback_select_edit_group(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    parts = call.data.split(":")
    group_id = int(parts[1])
    plan_id = int(parts[2])
    uid = call.from_user.id
    
    state = admin_states.get(uid)
    if not state or state.get("mode") != "edit" or state.get("plan_id") != plan_id:
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
    
    cursor.execute("UPDATE plans SET group_id=? WHERE id=?", (group_id, plan_id))
    state["current_group_id"] = group_id
    conn.commit()
    
    cursor.execute("SELECT title FROM managed_groups WHERE chat_id=?", (group_id,))
    group_title = cursor.fetchone()[0]
    
    bot.answer_callback_query(call.id, f"✅ Группа изменена: {group_title}")
    
    # Просто отправляем сообщение и показываем меню снова
    bot.send_message(call.message.chat.id, f"✅ Группа изменена на: {group_title}")
    show_edit_menu(call.message.chat.id, state)

# Обработчик завершения редактирования
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_finish:"))
def callback_edit_finish(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "🚫 Доступ запрещен.")
        return
        
    plan_id = int(call.data.split(":")[1])
    uid = call.from_user.id
    
    state = admin_states.get(uid)
    if not state or state.get("mode") != "edit" or state.get("plan_id") != plan_id:
        bot.answer_callback_query(call.id, "❌ Сессия устарела.")
        return
    
    # Очищаем состояние
    admin_states.pop(uid, None)
    
    bot.answer_callback_query(call.id, "✅ Редактирование завершено!")
    bot.send_message(call.message.chat.id, "✅ Редактирование группы завершено!", reply_markup=main_menu(uid))

# Обработчик ввода текстовых данных при редактировании
@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states and 
                    admin_states.get(m.from_user.id, {}).get("mode") == "edit" and 
                    admin_states.get(m.from_user.id, {}).get("step", "").startswith("editing_") and
                    m.chat.type == "private" and m.text)
def handle_edit_text_input(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    
    if not state or state.get("chat_id") != message.chat.id:
        return
        
    step = state.get("step", "")
    field = step.replace("editing_", "")
    
    if field == "title":
        new_title = message.text.strip()
        cursor.execute("UPDATE plans SET title=? WHERE id=?", (new_title, state["plan_id"]))
        state["current_title"] = new_title
        conn.commit()
        bot.send_message(message.chat.id, f"✅ Название обновлено: {new_title}")
        
    elif field == "price":
        cents = cents_from_str(message.text)
        if cents is None:
            bot.send_message(message.chat.id, "❌ Неправильный формат цены. Пример: 14.99")
            return
        cursor.execute("UPDATE plans SET price_cents=? WHERE id=?", (cents, state["plan_id"]))
        state["current_price"] = cents
        conn.commit()
        bot.send_message(message.chat.id, f"✅ Цена обновлена: {price_str_from_cents(cents)}")
        
    elif field == "description":
        new_description = message.text.strip()
        cursor.execute("UPDATE plans SET description=? WHERE id=?", (new_description, state["plan_id"]))
        state["current_description"] = new_description
        conn.commit()
        bot.send_message(message.chat.id, f"✅ Описание обновлено")
    
    # Возвращаемся к меню редактирования
    state["step"] = "edit_choice"
    show_edit_menu(message.chat.id, state)

# ----------------- Manual registration command for groups -----------------
@bot.message_handler(commands=["register_group"])
def cmd_register_group(message):
    chat = message.chat
    if chat.type not in ("group", "supergroup"):
        bot.send_message(message.chat.id, "Эта команда должна быть вызвана в группе/супергруппе.")
        return
    try:
        member = bot.get_chat_member(chat.id, BOT_ID)
        if member.status not in ("administrator", "creator"):
            bot.send_message(chat.id, "Назначьте бота администратором, затем повторите /register_group.")
            return
    except Exception:
        bot.send_message(chat.id, "Не могу проверить статус. Убедитесь, что бот добавлен.")
        return
    add_group_to_db(chat.id, chat.title or chat.username or str(chat.id), chat.type)
    bot.send_message(chat.id, "✅ Группа зарегистрирована — бот видит группу и сохранит её в базе.")
    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, f"✅ Группа зарегистрирована: {chat.title} (ID: {chat.id})")
        except:
            pass

# ----------------- Graceful shutdown -----------------
def shutdown():
    try:
        logging.info("Stopping bot...")
        bot.stop_polling()
    except:
        pass

# ----------------- Run polling -----------------
if __name__ == "__main__":
    logging.info("Starting student control bot...")
    try:
        bot.infinity_polling(timeout=60, long_polling_timeout=60,
                             allowed_updates=['message', 'edited_message', 'callback_query', 'my_chat_member', 'chat_member', 'inline_query', 'pre_checkout_query', 'shipping_query'])
    except KeyboardInterrupt:
        shutdown()
    except Exception:
        logging.exception("Bot crashed; shutting down")