# bot.py
import os
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
import re

from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# DB
from database.db import (
    init_db,
    save_user_hashed,
    get_user_by_chat,
    get_users_by_subgroup,
    mark_notified,
    was_notified,
)

import requests
from bs4 import BeautifulSoup

# ------------------------
# Конфігурація
# ------------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Сторінка з інтервалами відключень (ГРАФІКИ, не перелік адрес!)
ZOE_LIST_URL = os.getenv(
    "ZOE_LIST_URL",
    "https://www.zoe.com.ua/%D0%B3%D1%80%D0%B0%D1%84%D1%96%D0%BA%D0%B8-%D0%BF%D0%BE%D0%B3%D0%BE%D0%B4%D0%B8%D0%BD%D0%BD%D0%B8%D1%85-%D1%81%D1%82%D0%B0%D0%B1%D1%96%D0%BB%D1%96%D0%B7%D0%B0%D1%86%D1%96%D0%B9%D0%BD%D0%B8%D1%85/"
)

NOTIFY_MINUTES_BEFORE = int(os.getenv("NOTIFY_MINUTES_BEFORE", "30"))
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))

# Посилання, де користувач може сам знайти свою чергу
QUEUE_INFO_URL = (
    "https://script.google.com/macros/s/AKfycbyjNJSWjEU8Tgdeav_gb7VfHUDPeGPQywtS0Csu2RkI14o4ARmA6Tp0AHsLtLYg5Zj5/exec"
)

TZ = pytz.timezone("Europe/Kyiv")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Якщо раптом в .env вказано сторінку з переліком адрес — попереджаємо в логах
if "перелік-%D0%B0%D0%B4%D1%80%D0%B5%D1%81" in ZOE_LIST_URL.lower() or "перелік-адрес" in ZOE_LIST_URL.lower():
    logger.warning(
        "ZOE_LIST_URL зараз вказує на сторінку з переліком адрес, "
        "а не на сторінку з графіками погодинних відключень. "
        "Для коректної роботи бота встановіть ZOE_LIST_URL на сторінку з графіками."
    )

# Regex інтервалів часу на сторінці ZOE: "1.2 07:00–09:00" або "1.2: 07:00 - 09:00"
_interval_re = re.compile(
    r"(\d+\.\d+)\s*[:\-–—]?\s*(\d{1,2}:\d{2})\s*[–\-—]\s*(\d{1,2}:\d{2})"
)

# Regex перевірки формату підчерги (наприклад "1.1", "  2 . 3 ")
_subgroup_re = re.compile(r"^\s*(\d+)\s*\.\s*(\d+)\s*$")


# ------------------------
# Helpers
# ------------------------
def main_menu_keyboard():
    kb = [
        [InlineKeyboardButton("🔔 Зареєструватися", callback_data="menu_register")],
        [
            InlineKeyboardButton("ℹ️ Моя підчерга", callback_data="menu_getgroup"),
            InlineKeyboardButton("➡️ Наступне", callback_data="menu_next"),
        ],
    ]
    return InlineKeyboardMarkup(kb)


def format_subgroup(raw: str) -> str | None:
    """
    Приводить введёну строку до вигляду 'X.Y', якщо формат валідний.
    Інакше повертає None.
    """
    m = _subgroup_re.match(raw)
    if not m:
        return None
    g = m.group(1)
    s = m.group(2)
    return f"{g}.{s}"


async def _register_or_ask_confirm(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    username: str,
    canonical: str,
):
    """
    Загальна логіка реєстрації:
    - якщо користувач ще не мав підчерги — просто зберігаємо;
    - якщо вже був зареєстрований — питаємо підтвердження
      «чи впевнені ви що хочете знову зареєструватися».
    """
    user = get_user_by_chat(chat_id)
    group_id = canonical.split(".")[0]

    # Якщо користувач вже має підчергу — питаємо підтвердження
    if user and user.get("subgroup"):
        old = (user.get("subgroup") or "").strip()

        # зберігаємо нове значення в user_data, застосуємо після натискання "Так"
        context.user_data["pending_subgroup"] = canonical
        context.user_data["pending_group_id"] = group_id

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Так, змінити підчергу",
                        callback_data="confirm_rereg_yes",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "❌ Ні, залишити як є",
                        callback_data="confirm_rereg_no",
                    )
                ],
            ]
        )

        msg = (
            f"Ви вже зареєстровані з підчергою <b>{old}</b>.\n\n"
            f"Нова підчерга: <b>{canonical}</b>.\n\n"
            "Чи впевнені ви що хочете знову зареєструватися "
            "і змінити свою підчергу?"
        )
        if update.effective_message:
            await update.effective_message.reply_text(
                msg,
                reply_markup=kb,
                parse_mode="HTML",
            )
        return

    # Інакше — новий користувач або без підчерги, просто зберігаємо
    save_user_hashed(
        chat_id,
        username,
        hashed_address=None,
        raw_address=None,
        group_id=group_id,
        subgroup=canonical,
        verified=1,
    )

    if update.effective_message:
        await update.effective_message.reply_text(
            f"Готово — вас призначено у підчергу <b>{canonical}</b>.",
            parse_mode="HTML",
        )
        await update.effective_message.reply_text(
            "Повертаємось у головне меню.",
            reply_markup=main_menu_keyboard(),
        )


# ------------------------
# Команди
# ------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привіт! Я надсилатиму повідомлення про заплановані відключення.\n\n"
        "1️⃣ Дізнайтесь свою чергу та підчергу тут:\n"
        f"{QUEUE_INFO_URL}\n\n"
        "2️⃣ Потім поверніться сюди та введіть свою підчергу у форматі <b>1.1</b>, <b>2.3</b> тощо.",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )


async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Головне меню — оберіть дію:", reply_markup=main_menu_keyboard())


# ------------------------
# Callback меню + підтвердження повторної реєстрації
# ------------------------
async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""

    # ---------- реєстрація ----------
    if data == "menu_register":
        context.user_data["awaiting_subgroup"] = True
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="menu_back")]])
        await q.message.reply_text(
            "Щоб отримувати сповіщення, введіть свою <b>підчергу</b> у форматі <b>1.1</b>, <b>2.3</b> тощо.\n\n"
            "Як дізнатись свою чергу?\n"
            f"➡️ Скористайтесь сервісом: {QUEUE_INFO_URL}",
            reply_markup=kb,
            parse_mode="HTML",
        )
        return

    # ---------- повернутись у меню ----------
    if data == "menu_back":
        context.user_data["awaiting_subgroup"] = False
        await q.message.reply_text("Повернулись у головне меню.", reply_markup=main_menu_keyboard())
        return

    # ---------- показати свою підчергу ----------
    if data == "menu_getgroup":
        chat_id = q.message.chat.id
        user = get_user_by_chat(chat_id)
        if not user:
            await q.message.reply_text("Ви не зареєстровані. Натисніть '🔔 Зареєструватися'.")
        elif user.get("subgroup"):
            await q.message.reply_text(f"Ваша підчерга: {user.get('subgroup')}")
        else:
            await q.message.reply_text("Ваша підчерга не встановлена.")
        return

    # ---------- кнопка «Наступне» ----------
    if data == "menu_next":
        dummy_update = Update(update.update_id, callback_query=q)
        await next_cmd(dummy_update, context)
        return

    # ---------- підтвердження повторної реєстрації ----------
    if data == "confirm_rereg_yes":
        chat_id = q.message.chat.id
        username = q.from_user.username or q.from_user.full_name or str(chat_id)

        new_subgroup = context.user_data.get("pending_subgroup")
        new_group_id = context.user_data.get("pending_group_id")

        if not new_subgroup or not new_group_id:
            await q.message.reply_text(
                "Немає нової підчерги для збереження. Спробуйте зареєструватися знову.",
                reply_markup=main_menu_keyboard(),
            )
            return

        save_user_hashed(
            chat_id,
            username,
            hashed_address=None,
            raw_address=None,
            group_id=new_group_id,
            subgroup=new_subgroup,
            verified=1,
        )

        # очищаємо pending
        context.user_data.pop("pending_subgroup", None)
        context.user_data.pop("pending_group_id", None)

        await q.message.reply_text(
            f"Підчергу змінено. Нова підчерга: <b>{new_subgroup}</b>.",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )
        return

    if data == "confirm_rereg_no":
        # просто скасовуємо pending і повертаємось у меню
        context.user_data.pop("pending_subgroup", None)
        context.user_data.pop("pending_group_id", None)
        await q.message.reply_text(
            "Підчерга залишилась без змін.",
            reply_markup=main_menu_keyboard(),
        )
        return

    await q.message.reply_text("Невідома дія.")


# ------------------------
# Обробка довільних текстових повідомлень (router)
# ------------------------
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Якщо очікуємо підчергу — обробляємо як реєстрацію
    if context.user_data.get("awaiting_subgroup"):
        context.user_data["awaiting_subgroup"] = False
        await subgroup_message(update, context)
        return

    # Якщо користувач просто щось пише — показуємо меню
    await update.message.reply_text("Скористайтесь меню нижче:", reply_markup=main_menu_keyboard())


# ------------------------
# Обробка введеної підчерги (реєстрація через меню)
# ------------------------
async def subgroup_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username or update.effective_user.full_name or str(chat_id)
    text = (update.message.text or "").strip()

    if not text:
        await update.message.reply_text("Порожнє повідомлення. Введіть підчергу у форматі 1.1 або натисніть /menu.")
        return

    canonical = format_subgroup(text)
    if not canonical:
        await update.message.reply_text(
            "Невірний формат. Введіть підчергу у вигляді <b>1.1</b>, <b>2.3</b> тощо.",
            parse_mode="HTML",
        )
        return

    await _register_or_ask_confirm(update, context, chat_id, username, canonical)


# ------------------------
# Командна реєстрація /register (альтернатива кнопці)
# ------------------------
async def register_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username or update.effective_user.full_name or str(chat_id)

    if not context.args:
        await update.message.reply_text(
            "Щоб зареєструватись, введіть <b>підчергу</b> у форматі <b>1.1</b>.\n\n"
            "Як дізнатись свою чергу?\n"
            f"➡️ {QUEUE_INFO_URL}",
            parse_mode="HTML",
        )
        return

    user_input = " ".join(context.args).strip()
    canonical = format_subgroup(user_input)
    if not canonical:
        await update.message.reply_text(
            "Невірний формат. Приклад використання:\n"
            "<code>/register 1.1</code>",
            parse_mode="HTML",
        )
        return

    await _register_or_ask_confirm(update, context, chat_id, username, canonical)


# ------------------------
# Інформаційні команди
# ------------------------
async def getgroup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = get_user_by_chat(chat_id)
    if not user:
        await update.message.reply_text("Ви не зареєстровані. Використайте /register або кнопку 'Зареєструватися'.")
        return
    if user.get("subgroup"):
        await update.message.reply_text(f"Ваша підчерга: {user.get('subgroup')}")
    else:
        await update.message.reply_text("Ваша підчерга не встановлена.")


async def next_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати найближчий інтервал для підчерги користувача зі сторінки ZOE."""
    chat_id = (
        update.effective_chat.id
        if update.effective_chat
        else (update.callback_query.message.chat_id if update.callback_query else None)
    )
    if chat_id is None:
        return

    user = get_user_by_chat(chat_id)
    if not user or not user.get("subgroup"):
        if update.effective_message:
            await update.effective_message.reply_text(
                "Підчерга не встановлена. Використайте /register або кнопку 'Зареєструватися'."
            )
        return

    user_subgroup = (user.get("subgroup") or "").strip()
    user_group_id = (user.get("group_id") or "").strip() or user_subgroup.split(".")[0]

    if not ZOE_LIST_URL:
        if update.effective_message:
            await update.effective_message.reply_text("Не налаштовано ZOE_LIST_URL.")
        return

    try:
        logger.info("Fetching ZOE_LIST_URL: %s", ZOE_LIST_URL)
        resp = requests.get(ZOE_LIST_URL, timeout=15, headers={"User-Agent": "zap-bot/1.0"})
        logger.info("ZOE response: status=%s url=%s", resp.status_code, resp.url)
        resp.raise_for_status()

        html = resp.text
        logger.info("ZOE html head: %r", html[:300])

        text = BeautifulSoup(html, "html.parser").get_text("\n")

        intervals = []
        for m in _interval_re.finditer(text):
            sg = m.group(1).strip()
            start_s = m.group(2)
            end_s = m.group(3)
            intervals.append((sg, start_s, end_s))

        # Для отладки: які підчерги є на сторінці
        subgroups_on_page = sorted(set(sg for (sg, _, _) in intervals))
        logger.info("ZOE subgroups on page: %s", ", ".join(subgroups_on_page))

        # 1) Точне співпадіння по підчерзі, наприклад '1.2'
        exact = [(s, e) for (sg, s, e) in intervals if sg == user_subgroup]

        # 2) Якщо нічого не знайшли — шукаємо по черзі (всі підчерги, що починаються з '1.')
        by_group = []
        if not exact and user_group_id:
            prefix = user_group_id + "."
            by_group = [(sg, s, e) for (sg, s, e) in intervals if sg == user_group_id or sg.startswith(prefix)]

        if exact:
            s, e = exact[0]
            msg = f"Наступне (приблизно) відключення для підчерги {user_subgroup}: {s} — {e}"
            if update.effective_message:
                await update.effective_message.reply_text(msg)
            return

        if by_group:
            sg0, s, e = by_group[0]
            msg = (
                f"Не знайдено окремого запису саме для підчерги {user_subgroup}, "
                f"але для черги {user_group_id} є інтервал ({sg0}): {s} — {e}"
            )
            if update.effective_message:
                await update.effective_message.reply_text(msg)
            return

        if update.effective_message:
            await update.effective_message.reply_text(
                "Не знайдено записів для вашої підчерги на сторінці.\n"
                f"Ваша підчерга: {user_subgroup}\n"
                f"Підчерги на сторінці: {', '.join(subgroups_on_page) or 'немає розпізнаних підчерг'}"
            )

    except Exception as ex:
        logger.exception("Помилка next_cmd: %s", ex)
        if update.effective_message:
            await update.effective_message.reply_text("Помилка отримання розкладу. Спробуйте пізніше.")


# ------------------------
# Перевірка й нотифікація (періодично)
# ------------------------
async def check_and_notify(application):
    """Періодично перевіряє сторінку ZOE і сповіщає за N хвилин до початку."""
    if not ZOE_LIST_URL:
        return
    try:
        resp = requests.get(ZOE_LIST_URL, timeout=15, headers={"User-Agent": "zap-bot/1.0"})
        resp.raise_for_status()
        text = BeautifulSoup(resp.text, "html.parser").get_text("\n")

        now = datetime.now(TZ)
        threshold = now + timedelta(minutes=NOTIFY_MINUTES_BEFORE)

        intervals = []
        for m in _interval_re.finditer(text):
            sg = m.group(1).strip()
            start_s = m.group(2)
            end_s = m.group(3)
            try:
                start_dt = TZ.localize(
                    datetime.combine(now.date(), datetime.strptime(start_s, "%H:%M").time())
                )
            except Exception:
                continue
            try:
                end_dt = TZ.localize(
                    datetime.combine(now.date(), datetime.strptime(end_s, "%H:%M").time())
                )
            except Exception:
                end_dt = start_dt + timedelta(hours=2)
            intervals.append((sg, start_dt, end_dt))

        subgroups = set([i[0] for i in intervals])
        for sg in subgroups:
            users_chat_ids = get_users_by_subgroup(sg)
            if not users_chat_ids:
                continue

            for (_sg, start_dt, end_dt) in [it for it in intervals if it[0] == sg]:
                key = f"{start_dt.date()}_{sg}_{start_dt.strftime('%H%M')}"
                if start_dt <= threshold and start_dt >= now and not was_notified(key):
                    text_msg = (
                        f"⚡️ <b>Увага!</b>\n"
                        f"Наближається відключення для підчерги <b>{sg}</b>\n"
                        f"Дата: {start_dt.strftime('%d.%m.%Y')}\n"
                        f"Час: {start_dt.strftime('%H:%M')} — {end_dt.strftime('%H:%M')}\n\n"
                        f"Джерело: {ZOE_LIST_URL}"
                    )
                    for cid in users_chat_ids:
                        try:
                            await application.bot.send_message(
                                chat_id=cid,
                                text=text_msg,
                                parse_mode="HTML",
                            )
                        except Exception as e:
                            logger.warning("Не вдалося відправити повідомлення %s: %s", cid, e)
                    mark_notified(key, datetime.now().timestamp())
    except Exception as e:
        logger.exception("Помилка в check_and_notify: %s", e)


# ------------------------
# Наш фоновий цикл (без JobQueue/APS)
# ------------------------
async def notifier_loop(application):
    """Періодично запускає check_and_notify()."""
    await asyncio.sleep(5)  # невелика затримка перед першим запуском
    while True:
        try:
            await check_and_notify(application)
        except Exception as e:
            logger.exception("notifier_loop error: %s", e)
        await asyncio.sleep(max(5, CHECK_INTERVAL_MINUTES * 60))


# ------------------------
# post_init — старт фонового циклу в уже запущеному loop
# ------------------------
async def _post_init(app):
    app.create_task(notifier_loop(app))


# ------------------------
# Запуск бота
# ------------------------
def main():
    if not BOT_TOKEN:
        print("Помилка: вкажіть BOT_TOKEN у .env або в перемінних оточення.")
        return

    # Ініціалізація БД
    init_db()

    # Створюємо додаток (прикріпляємо post_init для фонового цикла)
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_post_init).build()

    # Команди
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CommandHandler("register", register_cmd))
    app.add_handler(CommandHandler("getgroup", getgroup_cmd))
    app.add_handler(CommandHandler("next", next_cmd))
    # /cancel просто вертає меню
    app.add_handler(CommandHandler("cancel", menu_cmd))

    # Callback меню (реєстрація / отримати підчергу / next / back / підтвердження)
    app.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^(menu_|confirm_rereg_)"))

    # Один універсальний обробник тексту
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    print("Бот запущено. Натисни Ctrl+C для зупинки.")
    app.run_polling()


if __name__ == "__main__":
    main()
