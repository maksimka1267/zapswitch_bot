# create_db.py
import sqlite3
from pathlib import Path
import sys

# Шлях до файлу бази у тій же папці, що й цей скрипт
DB_PATH = Path(__file__).resolve().parent / "zap_bot.db"


def get_conn():
    """Повертає з'єднання з sqlite файлом (створить файл, якщо його немає)."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    # Щоб отримувати рядки у вигляді dict (зручніше працювати)
    conn.row_factory = sqlite3.Row
    return conn


def _create_base_schema(cur):
    # Таблиця користувачів — зберігає тільки підчергу + групу (hashed_address можна не використовувати)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users(
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            address TEXT,           -- залишаємо для зворотної сумісності (НЕ використовуємо)
            hashed_address TEXT,    -- основне поле для адреси (хеш), зараз не використовується
            group_id TEXT,
            subgroup TEXT,
            verified INTEGER DEFAULT 0
        );
        """
    )

    # Таблиця для зіставлення адрес із зовнішніх джерел (зараз не використовується)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS addr_map(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_address TEXT,
            norm_address TEXT,
            group_id TEXT,
            subgroup TEXT,
            source_url TEXT
        );
        """
    )

    # Налаштування чатів (опційно)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chats(
            chat_id INTEGER PRIMARY KEY,
            subgroup TEXT
        );
        """
    )

    # Щоб не дублювати розсилки
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS notified(
            id TEXT PRIMARY KEY,   -- наприклад: '2025-11-02_1.1_0730'
            ts INTEGER             -- unix timestamp коли відправлено
        );
        """
    )

    # Індекси
    cur.execute("CREATE INDEX IF NOT EXISTS idx_addr_norm ON addr_map(norm_address);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_addr_subgroup ON addr_map(subgroup);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_subgroup ON users(subgroup);")


def _ensure_columns(cur):
    """М'які міграції існуючих таблиць (додаємо відсутні колонки)."""
    # users.hashed_address
    cur.execute("PRAGMA table_info(users);")
    cols = [r["name"] for r in cur.fetchall()]
    if "hashed_address" not in cols:
        try:
            cur.execute("ALTER TABLE users ADD COLUMN hashed_address TEXT;")
        except Exception:
            # Наприклад, дуже старий SQLite/заблоковано — ігноруємо, але логіка в коді працює з NULL ок
            pass


def init_db():
    """Створює/мігрує таблиці, якщо вони ще не існують / не повні."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        _create_base_schema(cur)
        _ensure_columns(cur)
        conn.commit()
    finally:
        conn.close()


def show_tables():
    """Повертає список таблиць у БД."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    rows = cur.fetchall()
    conn.close()
    return [r["name"] for r in rows]


def demo_insert_sample():
    """Опціонально: вставляє кілька тестових записів для перевірки."""
    conn = get_conn()
    cur = conn.cursor()
    # Приклад запису в addr_map
    cur.execute(
        """
        INSERT INTO addr_map(raw_address, norm_address, group_id, subgroup, source_url)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("вул. Перемоги, 12", "вулиця перемоги 12", "1", "1.1", "https://zoe.com.ua/example.pdf"),
    )
    # Приклад тестового юзера
    cur.execute(
        """
        INSERT OR REPLACE INTO users(chat_id, username, address, hashed_address, group_id, subgroup, verified)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (123456789, "testuser", None, "BASE64_HASH_EXAMPLE", "1", "1.1", 1),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    print(f"Створюємо / підключаємось до БД: {DB_PATH}")
    init_db()
    tables = show_tables()
    print("Таблиці в базі:", tables)

    # За бажання — вставити демонстраційні записи, якщо передано аргумент 'demo'
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        demo_insert_sample()
        print("Вставлені демонстраційні записи в базу (addr_map, users).")

    print("\nГотово. Файл бази знаходиться за вказаним шляхом.")
    print("Інтеграція: імпортуй init_db() і DB_PATH у свій основний бот, щоб використовувати ту ж базу.")
