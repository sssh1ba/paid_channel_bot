import asyncio
import logging
import sqlite3
import time
import uuid
import os
from datetime import datetime

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice, PreCheckoutQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ============================== КОНФИГУРАЦИЯ ==============================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8639398874:AAGPpWq9Ebo-a7gzHWhASvxjom3KX3ZGphE")
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "557620:AAy5SsfiXy0qSpCE6VtOa7TbXLRxLenUhU3")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003890843942"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "7234593897"))   # твой ID

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================== БАЗА ДАННЫХ ==============================
DB_NAME = "subscriptions.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            subscription_end INTEGER NOT NULL DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_crypto (
            invoice_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            days INTEGER NOT NULL,
            amount_usd TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_crypto (
            invoice_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            processed_at INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stars_payments (
            payment_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            days INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_stars (
            payment_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            days INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    logger.info("Database initialized")

def update_subscription(user_id: int, days_to_add: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    now = int(time.time())
    if days_to_add == 0:
        cursor.execute("""
            INSERT INTO users (user_id, subscription_end)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET subscription_end = 0
        """, (user_id, 0))
    else:
        cursor.execute("""
            INSERT INTO users (user_id, subscription_end)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET subscription_end = 
                CASE 
                    WHEN subscription_end > ? THEN subscription_end + ?
                    ELSE ? + ?
                END
        """, (user_id, now + days_to_add * 86400, now, days_to_add * 86400, now, days_to_add * 86400))
    conn.commit()
    conn.close()
    logger.info(f"User {user_id} subscription updated +{days_to_add} days")

def get_user_subscription_end(user_id: int) -> int:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT subscription_end FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else 0

# ----- CRYPTO -----
def add_pending_crypto(invoice_id: int, user_id: int, days: int, amount_usd: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO pending_crypto (invoice_id, user_id, days, amount_usd, created_at) VALUES (?, ?, ?, ?, ?)",
                   (invoice_id, user_id, days, amount_usd, int(time.time())))
    conn.commit()
    conn.close()

def get_pending_crypto(user_id: int = None, invoice_id: int = None):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    if invoice_id is not None:
        cursor.execute("SELECT invoice_id, user_id, days, amount_usd, created_at FROM pending_crypto WHERE invoice_id = ?", (invoice_id,))
        row = cursor.fetchone()
        conn.close()
        return {"invoice_id": row[0], "user_id": row[1], "days": row[2], "amount_usd": row[3], "created_at": row[4]} if row else None
    if user_id is not None:
        cursor.execute("SELECT invoice_id, user_id, days, amount_usd, created_at FROM pending_crypto WHERE user_id = ?", (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return [{"invoice_id": r[0], "user_id": r[1], "days": r[2], "amount_usd": r[3], "created_at": r[4]} for r in rows] if rows else []
    cursor.execute("SELECT invoice_id, user_id, days, amount_usd, created_at FROM pending_crypto")
    rows = cursor.fetchall()
    conn.close()
    return [{"invoice_id": r[0], "user_id": r[1], "days": r[2], "amount_usd": r[3], "created_at": r[4]} for r in rows] if rows else []

def delete_pending_crypto(invoice_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM pending_crypto WHERE invoice_id = ?", (invoice_id,))
    conn.commit()
    conn.close()

def mark_crypto_processed(invoice_id: int, user_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO processed_crypto (invoice_id, user_id, processed_at) VALUES (?, ?, ?)",
                   (invoice_id, user_id, int(time.time())))
    conn.commit()
    conn.close()

def is_crypto_processed(invoice_id: int) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_crypto WHERE invoice_id = ?", (invoice_id,))
    row = cursor.fetchone()
    conn.close()
    return row is not None

# ----- STARS -----
def add_stars_payment(payment_id: str, user_id: int, days: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO stars_payments (payment_id, user_id, days, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
                   (payment_id, user_id, days, int(time.time())))
    conn.commit()
    conn.close()

def get_stars_payment_status(payment_id: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT status, days FROM stars_payments WHERE payment_id = ?", (payment_id,))
    row = cursor.fetchone()
    conn.close()
    return row

def complete_stars_payment(payment_id: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("UPDATE stars_payments SET status = 'completed' WHERE payment_id = ?", (payment_id,))
    conn.commit()
    conn.close()

def add_pending_stars(payment_id: str, user_id: int, days: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO pending_stars (payment_id, user_id, days, created_at) VALUES (?, ?, ?, ?)",
                   (payment_id, user_id, days, int(time.time())))
    conn.commit()
    conn.close()

def get_pending_stars(user_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT payment_id, days, created_at FROM pending_stars WHERE user_id = ?", (user_id,))
    rows = cursor.fetchall()
    conn.close()
    return [{"payment_id": r[0], "days": r[1], "created_at": r[2]} for r in rows]

def delete_pending_stars(payment_id: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM pending_stars WHERE payment_id = ?", (payment_id,))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, subscription_end FROM users")
    rows = cursor.fetchall()
    conn.close()
    return rows

# ============================== КЛАВИАТУРЫ ==============================
def start_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔥 ОТКРЫТЬ ДОСТУП", callback_data="open_access")
    builder.button(text="📸 Что внутри?", callback_data="what_inside")
    builder.button(text="❓ Вопросы", callback_data="faq")
    builder.adjust(1)
    return builder.as_markup()

def tariffs_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="7 дней — $5", callback_data="tariff_7")
    builder.button(text="🔥 30 дней — $15 (лучший выбор)", callback_data="tariff_30")
    builder.button(text="♾ НАВСЕГДА — $30", callback_data="tariff_forever")
    builder.button(text="◀️ Назад", callback_data="back_to_start")
    builder.adjust(1)
    return builder.as_markup()

def payment_methods_keyboard(days: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💸 Крипта (быстро)", callback_data=f"crypto_{days}")
    builder.button(text="⭐ Telegram Stars", callback_data=f"stars_{days}")
    builder.button(text="◀️ Назад", callback_data="open_access")
    builder.adjust(1)
    return builder.as_markup()

def crypto_payment_keyboard(pay_url: str, invoice_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💸 Оплатить", url=pay_url)
    builder.button(text="🔄 Проверить оплату", callback_data=f"check_crypto_{invoice_id}")
    builder.button(text="❌ Отменить", callback_data=f"cancel_crypto_{invoice_id}")
    builder.button(text="◀️ Назад", callback_data="open_access")
    builder.adjust(1)
    return builder.as_markup()

def stars_payment_keyboard(payment_id: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💸 Отправить счёт повторно", callback_data=f"new_stars_{payment_id}")
    builder.button(text="🔄 Проверить оплату", callback_data=f"check_stars_{payment_id}")
    builder.button(text="❌ Отменить", callback_data=f"cancel_stars_{payment_id}")
    builder.button(text="◀️ Назад", callback_data="open_access")
    builder.adjust(1)
    return builder.as_markup()

def back_to_start_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data="back_to_start")
    return builder.as_markup()

# ============================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==============================
async def notify_admin(bot: Bot, text: str):
    if ADMIN_ID and ADMIN_ID != 0:
        try:
            await bot.send_message(ADMIN_ID, text)
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")

async def backup_db(bot: Bot):
    while True:
        await asyncio.sleep(86400)
        if ADMIN_ID and ADMIN_ID != 0:
            try:
                with open(DB_NAME, "rb") as f:
                    await bot.send_document(
                        chat_id=ADMIN_ID,
                        document=f,
                        caption=f"📦 Бэкап базы данных {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                    )
                logger.info("Backup sent to admin")
            except Exception as e:
                logger.exception("Failed to send backup")
                await notify_admin(bot, f"❌ Ошибка при отправке бэкапа: {str(e)}")

# ============================== ВЫДАЧА ДОСТУПА ==============================
async def grant_access(bot: Bot, user_id: int, days: int, amount_usd: str = None):
    update_subscription(user_id, days)
    end_timestamp = get_user_subscription_end(user_id)

    expire_date = end_timestamp if end_timestamp > 0 else None
    try:
        link = await bot.create_chat_invite_link(
            chat_id=CHANNEL_ID,
            member_limit=1,
            expire_date=expire_date
        )
        invite_link = link.invite_link
        text = f"✅ Оплата получена! Подписка активна до: {datetime.fromtimestamp(end_timestamp).strftime('%d.%m.%Y %H:%M') if end_timestamp > 0 else 'навсегда'}.\n\nСсылка для вступления (действует один раз):\n{invite_link}"
    except Exception as e:
        logger.exception(f"Failed to create invite link for user {user_id}: {e}")
        text = f"✅ Оплата получена! Подписка активна до: {datetime.fromtimestamp(end_timestamp).strftime('%d.%m.%Y %H:%M') if end_timestamp > 0 else 'навсегда'}.\n\n⚠️ Не удалось создать ссылку. Пожалуйста, свяжитесь с администратором."

    await bot.send_message(user_id, text)

    if amount_usd:
        tariff_str = "навсегда" if days == 0 else f"{days} дней"
        await notify_admin(bot, f"💰 Новая оплата\n\n👤 Пользователь: {user_id}\n💵 Сумма: ${amount_usd}\n📆 Тариф: {tariff_str}")

# ============================== ФОНОВАЯ ПРОВЕРКА ПЛАТЕЖЕЙ ==============================
async def check_crypto_invoice(session: aiohttp.ClientSession, invoice_id: int) -> tuple[bool, str, str]:
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN}
    try:
        async with session.get("https://pay.crypt.bot/api/getInvoices",
                               params={"invoice_ids": invoice_id},
                               headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("ok") and data["result"]["items"]:
                    invoice = data["result"]["items"][0]
                    return invoice["status"] == "paid", invoice.get("amount", ""), invoice.get("asset", "")
                else:
                    logger.error(f"CryptoBot getInvoices error: {data}")
            else:
                logger.error(f"CryptoBot getInvoices HTTP {resp.status}: {await resp.text()}")
    except Exception as e:
        logger.exception("CryptoBot check failed")
    return False, "", ""

async def cancel_crypto_invoice(invoice_id: int) -> bool:
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post("https://pay.crypt.bot/api/deleteInvoice",
                                    json={"invoice_id": invoice_id},
                                    headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("ok", False)
                else:
                    logger.error(f"CryptoBot deleteInvoice error: {resp.status} {await resp.text()}")
        except Exception as e:
            logger.exception("CryptoBot cancel failed")
    return False

async def monitor_pending_invoice(bot: Bot, invoice_id: int, user_id: int, days: int, amount_usd: str):
    async with aiohttp.ClientSession() as session:
        for attempt in range(2880):  # 24 часа
            await asyncio.sleep(30)
            paid, paid_amount, paid_asset = await check_crypto_invoice(session, invoice_id)
            if paid:
                if is_crypto_processed(invoice_id):
                    logger.info(f"Invoice {invoice_id} already processed, skipping")
                    delete_pending_crypto(invoice_id)
                    return
                if paid_amount != amount_usd or paid_asset != "USDT":
                    logger.error(f"Invoice {invoice_id} paid with wrong amount/asset: {paid_amount} {paid_asset}, expected {amount_usd} USDT")
                    await bot.send_message(user_id, "⚠️ Ошибка: сумма оплаты не совпадает. Свяжитесь с администратором.")
                    delete_pending_crypto(invoice_id)
                    return
                await grant_access(bot, user_id, days, amount_usd)
                mark_crypto_processed(invoice_id, user_id)
                delete_pending_crypto(invoice_id)
                logger.info(f"Payment confirmed for invoice {invoice_id}, user {user_id}")
                return
        delete_pending_crypto(invoice_id)
        logger.warning(f"Invoice {invoice_id} not paid within 24 hours, removed")

# ============================== ПРОВЕРКА ПОДПИСОК ==============================
async def check_subscriptions(bot: Bot):
    while True:
        await asyncio.sleep(3600)
        now = int(time.time())
        users = get_all_users()
        for user_id, end_ts in users:
            if end_ts != 0 and end_ts < now:
                logger.info(f"User {user_id} subscription expired. Removing from channel.")
                try:
                    await bot.ban_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
                    await bot.unban_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
                except Exception as e:
                    if "user not found" in str(e).lower():
                        logger.info(f"User {user_id} not in channel, skipping")
                    else:
                        logger.exception(f"Failed to kick user {user_id} from channel")
                        await notify_admin(bot, f"❌ Ошибка при удалении пользователя {user_id} из канала: {str(e)}")

# ============================== ОБРАБОТЧИКИ БОТА ==============================
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🔥 Приватный доступ к контенту Виолины\n\n"
        "— новые фото каждый день\n"
        "— доступ только через бота\n"
        "— не весь контент остается, часть удаляется\n\n"
        "👇 выбери доступ",
        reply_markup=start_keyboard()
    )

@dp.callback_query(lambda c: c.data == "open_access")
async def open_access(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Выбери вариант доступа:\n\n"
        "💡 Большинство берут на 30 дней (выгоднее)\n"
        "⏳ Осталось 12 мест по текущей цене",
        reply_markup=tariffs_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "what_inside")
async def what_inside(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "📸 Что внутри?\n\n"
        "Закрытый канал с эксклюзивным контентом:\n"
        "🎯 ежедневные свежие фото\n"
        "🎯 редкие видео\n"
        "🎯 эксклюзивный временный контент\n\n"
        "🔥 Часть материалов удаляется через 24 часа.",
        reply_markup=back_to_start_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "faq")
async def faq(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "❓ Часто задаваемые вопросы\n\n"
        "📌 Как получить доступ?\n"
        "→ выбери тариф и оплати удобным способом\n\n"
        "📌 Что за крипта?\n"
        "→ принимаем USDT через @CryptoBot (быстро и безопасно)\n\n"
        "📌 Как попасть в канал?\n"
        "→ после оплаты бот пришлёт одноразовую ссылку\n\n"
        "📌 Поддержка?\n"
        "→ пиши @Ev1LLyy",
        reply_markup=back_to_start_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_start")
async def back_to_start(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔥 Приватный доступ к контенту Виолины\n\n"
        "— новые фото каждый день\n"
        "— доступ только через бота\n"
        "— не весь контент остается, часть удаляется\n\n"
        "👇 выбери доступ",
        reply_markup=start_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("tariff_"))
async def process_tariff(callback: types.CallbackQuery):
    try:
        if callback.data == "tariff_7":
            days, amount_usd = 7, 5
        elif callback.data == "tariff_30":
            days, amount_usd = 30, 15
        elif callback.data == "tariff_forever":
            days, amount_usd = 0, 30
        else:
            await callback.answer("Неизвестный тариф", show_alert=True)
            return
    except Exception as e:
        logger.exception("Error parsing tariff")
        await callback.answer("Ошибка", show_alert=True)
        await notify_admin(callback.bot, f"❌ Ошибка в process_tariff: {str(e)}")
        return

    await callback.message.edit_text(
        f"Остался последний шаг 👇\n\nВыбери удобный способ оплаты:",
        reply_markup=payment_methods_keyboard(days)
    )
    await callback.answer()

# ---------- ОПЛАТА CRYPTOBOT ----------
@dp.callback_query(lambda c: c.data.startswith("crypto_"))
async def process_crypto(callback: types.CallbackQuery):
    try:
        days = int(callback.data.split("_")[1])
    except (IndexError, ValueError):
        await callback.answer("Неверный формат", show_alert=True)
        return

    amount_usd = "5" if days == 7 else "15" if days == 30 else "30"
    user_id = callback.from_user.id

    pending_list = get_pending_crypto(user_id=user_id)
    for pending in pending_list:
        if time.time() - pending["created_at"] < 7200:
            pay_url = f"https://t.me/CryptoBot?start=pay_{pending['invoice_id']}"
            await callback.message.edit_text(
                f"У вас уже есть активный неоплаченный счёт на сумму {amount_usd} USDT.\n"
                f"Пожалуйста, оплатите его или подождите.",
                reply_markup=crypto_payment_keyboard(pay_url, pending['invoice_id'])
            )
            await callback.answer()
            return

    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN}
    payload = {
        "asset": "USDT",
        "amount": amount_usd,
        "description": f"Подписка на {'навсегда' if days == 0 else f'{days} дней'}",
        "paid_btn_name": "openChannel",
        "paid_btn_url": f"https://t.me/c/{abs(CHANNEL_ID)}",
        "allow_comments": False,
        "allow_anonymous": False
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post("https://pay.crypt.bot/api/createInvoice", json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ok"):
                        invoice = data["result"]
                        invoice_id = invoice["invoice_id"]
                        pay_url = invoice["pay_url"]

                        add_pending_crypto(invoice_id, user_id, days, amount_usd)
                        asyncio.create_task(monitor_pending_invoice(callback.bot, invoice_id, user_id, days, amount_usd))

                        await callback.message.edit_text(
                            "⚡ Оплата почти завершена\n\nПосле оплаты ты сразу получишь доступ",
                            reply_markup=crypto_payment_keyboard(pay_url, invoice_id)
                        )
                        return
                    else:
                        raise Exception(f"CryptoBot error: {data}")
                else:
                    raise Exception(f"HTTP {resp.status}")
        except Exception as e:
            logger.exception("CryptoBot createInvoice failed")
            await callback.message.edit_text(f"❌ Ошибка при создании счёта. Попробуйте позже.\n{e}", reply_markup=back_to_start_keyboard())
            await notify_admin(callback.bot, f"❌ Ошибка CryptoBot: {str(e)}")
            await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("check_crypto_"))
async def check_crypto_payment(callback: types.CallbackQuery):
    invoice_id = int(callback.data.split("_")[2])
    pending = get_pending_crypto(invoice_id=invoice_id)
    if not pending:
        await callback.answer("Платёж уже обработан или не найден.", show_alert=True)
        return

    async with aiohttp.ClientSession() as session:
        paid, paid_amount, paid_asset = await check_crypto_invoice(session, invoice_id)
        if not paid:
            await callback.answer("Платёж пока не получен. Подождите или оплатите.", show_alert=True)
            return

        if is_crypto_processed(invoice_id):
            await callback.answer("Платёж уже был обработан.", show_alert=True)
            delete_pending_crypto(invoice_id)
            return

        days = pending["days"]
        user_id = pending["user_id"]
        amount_usd = pending["amount_usd"]

        if paid_amount != amount_usd or paid_asset != "USDT":
            logger.error(f"Invoice {invoice_id} paid with wrong amount/asset: {paid_amount} {paid_asset}, expected {amount_usd} USDT")
            await callback.bot.send_message(user_id, "⚠️ Ошибка: сумма оплаты не совпадает. Свяжитесь с администратором.")
            delete_pending_crypto(invoice_id)
            await callback.answer("Ошибка суммы оплаты", show_alert=True)
            await notify_admin(callback.bot, f"⚠️ Несовпадение суммы оплаты: ожидалось {amount_usd} USDT, получено {paid_amount} {paid_asset}")
            return

        await grant_access(callback.bot, user_id, days, amount_usd)
        mark_crypto_processed(invoice_id, user_id)
        delete_pending_crypto(invoice_id)
        await callback.message.edit_text("✅ Оплата успешно получена! Доступ активирован.")
        await callback.answer("Оплата подтверждена!", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("cancel_crypto_"))
async def cancel_crypto_payment(callback: types.CallbackQuery):
    invoice_id = int(callback.data.split("_")[2])
    pending = get_pending_crypto(invoice_id=invoice_id)
    if not pending:
        await callback.answer("Платёж уже обработан или не найден.", show_alert=True)
        return

    cancelled = await cancel_crypto_invoice(invoice_id)
    if cancelled:
        logger.info(f"User {callback.from_user.id} cancelled invoice {invoice_id}")
    else:
        logger.warning(f"Failed to cancel invoice {invoice_id} via API, but still deleting locally.")

    delete_pending_crypto(invoice_id)
    await callback.message.edit_text(
        "❌ Платёж отменён. Вы можете выбрать другой тариф.",
        reply_markup=tariffs_keyboard()
    )
    await callback.answer("Платёж отменён", show_alert=True)

# ---------- ОПЛАТА TELEGRAM STARS ----------
async def create_stars_invoice(bot: Bot, user_id: int, days: int) -> str:
    stars = 500 if days == 7 else 1500 if days == 30 else 3000
    prices = [LabeledPrice(label="Подписка", amount=stars)]
    payment_id = f"stars_{days}_{uuid.uuid4().hex}"

    await bot.send_invoice(
        chat_id=user_id,
        title="Подписка на канал",
        description=f"Доступ к приватному каналу на {'навсегда' if days == 0 else f'{days} дней'}",
        payload=payment_id,
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter="subscription",
        need_name=False,
        need_phone_number=False,
        need_email=False,
        need_shipping_address=False,
        is_flexible=False
    )
    add_pending_stars(payment_id, user_id, days)
    return payment_id

@dp.callback_query(lambda c: c.data.startswith("stars_"))
async def process_stars(callback: types.CallbackQuery):
    try:
        days = int(callback.data.split("_")[1])
    except (IndexError, ValueError):
        await callback.answer("Неверный формат", show_alert=True)
        return

    user_id = callback.from_user.id

    old_pending = get_pending_stars(user_id)
    for p in old_pending:
        delete_pending_stars(p["payment_id"])

    try:
        payment_id = await create_stars_invoice(callback.bot, user_id, days)
        await callback.message.delete()
        await callback.bot.send_message(
            user_id,
            "⚡ Оплата почти завершена\n\nПосле оплаты ты сразу получишь доступ",
            reply_markup=stars_payment_keyboard(payment_id)
        )
    except Exception as e:
        logger.exception("Failed to send Stars invoice")
        await callback.message.edit_text(
            "❌ Не удалось отправить счёт. Попробуйте позже.",
            reply_markup=back_to_start_keyboard()
        )
        await notify_admin(callback.bot, f"❌ Ошибка отправки Stars инвойса: {str(e)}")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("new_stars_"))
async def create_new_stars_invoice(callback: types.CallbackQuery):
    old_payment_id = callback.data.split("_")[2]
    user_id = callback.from_user.id

    pending = get_pending_stars(user_id)
    old = next((p for p in pending if p["payment_id"] == old_payment_id), None)
    if not old:
        await callback.answer("Платёж уже обработан или не найден.", show_alert=True)
        return

    days = old["days"]
    delete_pending_stars(old_payment_id)

    try:
        new_payment_id = await create_stars_invoice(callback.bot, user_id, days)
        await callback.message.edit_text(
            "✅ Создан новый счёт.",
            reply_markup=stars_payment_keyboard(new_payment_id)
        )
    except Exception as e:
        logger.exception("Failed to create new Stars invoice")
        await callback.message.edit_text(
            "❌ Не удалось создать новый счёт. Попробуйте позже.",
            reply_markup=back_to_start_keyboard()
        )
        await notify_admin(callback.bot, f"❌ Ошибка создания нового Stars инвойса: {str(e)}")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("check_stars_"))
async def check_stars_payment(callback: types.CallbackQuery):
    payment_id = callback.data.split("_")[2]
    user_id = callback.from_user.id

    row = get_stars_payment_status(payment_id)
    if not row:
        await callback.answer("Платёж ещё не начинался. Нажмите «Отправить счёт повторно».", show_alert=True)
        return

    status, days = row
    if status == "completed":
        amount_usd = "5" if days == 7 else "15" if days == 30 else "30"
        await grant_access(callback.bot, user_id, days, amount_usd)
        delete_pending_stars(payment_id)
        await callback.message.edit_text("✅ Оплата успешно получена! Доступ активирован.")
        await callback.answer("Оплата подтверждена!", show_alert=True)
    else:
        await callback.answer("Платёж пока не получен. Подождите или оплатите.", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("cancel_stars_"))
async def cancel_stars_payment(callback: types.CallbackQuery):
    payment_id = callback.data.split("_")[2]
    pending = get_pending_stars(callback.from_user.id)
    if not any(p["payment_id"] == payment_id for p in pending):
        await callback.answer("Платёж уже обработан или не найден.", show_alert=True)
        return

    delete_pending_stars(payment_id)
    await callback.message.edit_text(
        "❌ Платёж отменён. Вы можете выбрать другой тариф.",
        reply_markup=tariffs_keyboard()
    )
    await callback.answer("Платёж отменён", show_alert=True)

@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    try:
        payload = query.invoice_payload
        if payload.startswith("stars_"):
            parts = payload.split("_")
            days = int(parts[1])
            payment_id = parts[2] if len(parts) > 2 else payload
            add_stars_payment(payment_id, query.from_user.id, days)
    except Exception as e:
        logger.exception("Failed to save pending Stars payment")
        await notify_admin(query.bot, f"❌ Ошибка в pre_checkout: {str(e)}")
    await query.answer(ok=True)

@dp.message(lambda message: message.successful_payment is not None)
async def successful_payment(message: Message):
    payment = message.successful_payment
    payload = payment.invoice_payload
    if not payload.startswith("stars_"):
        return
    try:
        parts = payload.split("_")
        days = int(parts[1])
        payment_id = parts[2] if len(parts) > 2 else payload
    except (IndexError, ValueError):
        logger.error(f"Invalid stars payload: {payload}")
        return

    user_id = message.from_user.id
    amount_usd = "5" if days == 7 else "15" if days == 30 else "30"

    await grant_access(message.bot, user_id, days, amount_usd)
    complete_stars_payment(payment_id)
    delete_pending_stars(payment_id)
    await message.answer("✅ Оплата получена! Доступ к каналу активирован.")
    try:
        await message.bot.edit_message_reply_markup(
            chat_id=user_id,
            message_id=message.message_id - 1,
            reply_markup=None
        )
    except:
        pass

# ============================== ВОССТАНОВЛЕНИЕ НЕЗАВЕРШЁННЫХ ПЛАТЕЖЕЙ ==============================
async def restore_stars_payments(bot: Bot):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT payment_id, user_id, days FROM stars_payments WHERE status = 'pending'")
    rows = cursor.fetchall()
    conn.close()
    for payment_id, user_id, days in rows:
        logger.info(f"Restoring pending Stars payment {payment_id} for user {user_id}")
        amount_usd = "5" if days == 7 else "15" if days == 30 else "30"
        await grant_access(bot, user_id, days, amount_usd)
        complete_stars_payment(payment_id)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    now = int(time.time())
    cursor.execute("DELETE FROM pending_stars WHERE created_at < ?", (now - 3600,))
    conn.commit()
    conn.close()

# ============================== ЗАПУСК ==============================
async def main():
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN":
        logger.error("BOT_TOKEN не задан")
        return
    if not CRYPTOBOT_TOKEN or CRYPTOBOT_TOKEN == "YOUR_CRYPTOBOT_TOKEN":
        logger.error("CRYPTOBOT_TOKEN не задан")
        return
    if CHANNEL_ID is None:
        logger.error("CHANNEL_ID не задан")
        return

    bot = Bot(token=BOT_TOKEN)

    try:
        me = await bot.get_me()
        chat_member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=me.id)
        if chat_member.status not in ('creator', 'administrator'):
            logger.error("Bot is not an admin of the channel")
            print("❌ Бот не является администратором канала. Добавьте бота как администратора.")
            await bot.session.close()
            return
        if hasattr(chat_member, 'can_invite_users') and not chat_member.can_invite_users:
            logger.error("Bot does not have invite users permission")
            print("❌ У бота нет права 'Пригласительные ссылки'.")
            await bot.session.close()
            return
        if hasattr(chat_member, 'can_restrict_members') and not chat_member.can_restrict_members:
            logger.error("Bot does not have restrict members permission")
            print("❌ У бота нет права 'Блокировка пользователей'.")
            await bot.session.close()
            return
        test_link = await bot.create_chat_invite_link(chat_id=CHANNEL_ID, member_limit=1)
        logger.info("Bot is admin of the channel, invite link works.")
    except Exception as e:
        logger.error(f"Bot is NOT admin of the channel or channel ID is wrong: {e}")
        print("❌ Бот не является администратором канала или ID канала неверный. Исправьте и перезапустите.")
        await bot.session.close()
        return

    init_db()

    all_pending = get_pending_crypto()
    for pending in all_pending:
        if time.time() - pending["created_at"] < 86400:
            asyncio.create_task(monitor_pending_invoice(
                bot, pending["invoice_id"], pending["user_id"],
                pending["days"], pending["amount_usd"]
            ))
            logger.info(f"Restored monitoring for invoice {pending['invoice_id']}")

    await restore_stars_payments(bot)

    asyncio.create_task(check_subscriptions(bot))
    asyncio.create_task(backup_db(bot))

    # Сброс вебхука и небольшая пауза для устранения конфликтов
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(1)

    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        logger.info("Bot stopped")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
