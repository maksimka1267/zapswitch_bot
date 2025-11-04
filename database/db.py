# db.py
from pathlib import Path
import sqlite3
import time

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "zap_bot.db"


def get_conn():
    """Повертає з'єднання з sqlite файлом (створить файл, якщо його не існує)."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Створює базові таблиці та виконує м'які міграції (безпечний багаторазовий виклик)."""
    conn = get_conn()
    cur = conn.cursor()

    # ---- Базова схема
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users(
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            address TEXT,           -- залишено для зворотної сумісності (НЕ використовуємо)
            hashed_address TEXT,    -- основне поле для адреси (хеш), зараз не використовується
            group_id TEXT,
            subgroup TEXT,
            verified INTEGER DEFAULT 0
        );
        """
    )
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chats(
            chat_id INTEGER PRIMARY KEY,
            subgroup TEXT
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS notified(
            id TEXT PRIMARY KEY,
            ts INTEGER
        );
        """
    )

    # ---- Індекси
    cur.execute("CREATE INDEX IF NOT EXISTS idx_addr_norm ON addr_map(norm_address);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_addr_subgroup ON addr_map(subgroup);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_subgroup ON users(subgroup);")

    # ---- М'які міграції
    ensure_hashed_column(cur)

    conn.commit()
    conn.close()


def ensure_hashed_column(cur_or_none=None):
    """
    Якщо в таблиці users немає колонки hashed_address — додає її.
    Може бути викликано в контексті існуючого курсора або окремо.
    """
    if cur_or_none is None:
        conn = get_conn()
        cur = conn.cursor()
        own_conn = True
    else:
        cur = cur_or_none
        own_conn = False

    try:
        cur.execute("PRAGMA table_info(users);")
        cols = [r["name"] for r in cur.fetchall()]
        if "hashed_address" not in cols:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN hashed_address TEXT;")
            except Exception:
                # Ігноруємо помилку, якщо ALTER недоступний — код працюватиме з NULL у полі
                pass
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


# =========================
# Функції для addr_map (зараз не використовуються)
# =========================
def insert_addr_map_record(raw_address, norm_address, group_id, subgroup, source_url):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO addr_map(raw_address, norm_address, group_id, subgroup, source_url) VALUES (?, ?, ?, ?, ?)",
        (raw_address, norm_address, group_id, subgroup, source_url),
    )
    conn.commit()
    conn.close()


def clear_addr_map_by_source(source_url):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM addr_map WHERE source_url=?", (source_url,))
    conn.commit()
    conn.close()


def load_all_addr_map_records():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, raw_address, norm_address, group_id, subgroup, source_url FROM addr_map")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def load_addr_map_by_id(rec_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, raw_address, norm_address, group_id, subgroup, source_url FROM addr_map WHERE id=?",
        (rec_id,),
    )
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else None


# =========================
# Функції для users
# =========================
def save_user_hashed(chat_id, username, hashed_address, raw_address=None, group_id=None, subgroup=None, verified=0):
    """
    Зберігає або оновлює користувача.
    raw_address за замовчуванням не зберігаємо (приватність) — залишено параметр для сумісності.
    hashed_address зараз може бути None, якщо не працюємо з адресами.
    """
    conn = get_conn()
    cur = conn.cursor()
    # На випадок, якщо БД дуже стара і без міграцій — спробуємо додати колонку на льоту.
    ensure_hashed_column(cur)

    cur.execute(
        """
        INSERT OR REPLACE INTO users(chat_id, username, address, hashed_address, group_id, subgroup, verified)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (chat_id, username, None, hashed_address, group_id, subgroup, verified),
    )
    conn.commit()
    conn.close()


def get_user_by_chat(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT chat_id, username, address, hashed_address, group_id, subgroup, verified FROM users WHERE chat_id=?",
        (chat_id,),
    )
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else None


def get_users_by_subgroup(subgroup):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM users WHERE subgroup=? AND verified=1", (subgroup,))

    rows = [r["chat_id"] for r in cur.fetchall()]
    conn.close()
    return rows


def list_all_users(limit=100):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT chat_id, username, group_id, subgroup, verified FROM users ORDER BY chat_id LIMIT ?",
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# =========================
# Функції для notified
# =========================
def mark_notified(key, ts=None):
    if ts is None:
        ts = int(time.time())
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO notified(id, ts) VALUES (?, ?)", (key, int(ts)))
    conn.commit()
    conn.close()


def was_notified(key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM notified WHERE id=?", (key,))
    r = cur.fetchone()
    conn.close()
    return bool(r)
