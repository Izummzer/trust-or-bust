# bot/db.py
import os
import ssl
import asyncpg
import certifi
from typing import List, Dict, Optional

_DB_POOL = None

def _make_ssl_ctx() -> ssl.SSLContext:
    """
    SSL-контекст: сперва пробуем CA Supabase (если переменная SUPABASE_CA указывает на файл),
    иначе используем certifi bundle.
    """
    ca = os.getenv("SUPABASE_CA")
    if ca and os.path.exists(ca):
        return ssl.create_default_context(cafile=ca)
    return ssl.create_default_context(cafile=certifi.where())

async def get_pool():
    """
    Пул соединений к Supabase.
    ВАЖНО: statement_cache_size=0 из-за PgBouncer (transaction pooler).
    """
    global _DB_POOL
    if _DB_POOL is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")
        _DB_POOL = await asyncpg.create_pool(
            dsn,
            ssl=_make_ssl_ctx(),
            min_size=1,
            max_size=5,
            timeout=10,
            statement_cache_size=0
        )
    return _DB_POOL

# -------------------- USERS --------------------

async def ensure_user(telegram_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as c:
        await c.execute("""
            insert into users(telegram_id) values ($1)
            on conflict (telegram_id) do nothing
        """, telegram_id)
        row = await c.fetchrow("select id from users where telegram_id=$1", telegram_id)
        return row["id"]

# -------------------- SESSIONS / DECK --------------------

NEXT_LEVEL = {"A1": "A2", "A2": "B1", "B1": "B2", "B2": "B2"}

async def upsert_session(user_id: int, level: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as c:
        sid = await c.fetchval("""
          insert into sessions(user_id, level, status)
          values($1,$2,'morning')
          returning id
        """, user_id, level)
        return sid

async def set_session_status(session_id: int, status: str):
    pool = await get_pool()
    async with pool.acquire() as c:
        await c.execute("update sessions set status=$2 where id=$1", session_id, status)

async def pick_words_for_level(level: str, pos: str, n: int = 5) -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire() as c:
        rows = await c.fetch("""
          select id, word, translation
          from words
          where level=$1 and pos=$2
          order by random() limit $3
        """, NEXT_LEVEL.get(level, "B1"), pos, n)
        return [dict(r) for r in rows]

async def save_deck(session_id: int, word_ids: List[int]):
    pool = await get_pool()
    async with pool.acquire() as c:
        for i, wid in enumerate(word_ids, start=1):
            await c.execute("""
              insert into session_deck(session_id, word_id, position)
              values ($1,$2,$3)
              on conflict (session_id, position) do update set word_id=excluded.word_id
            """, session_id, wid, i)

async def load_deck(session_id: int) -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire() as c:
        rows = await c.fetch("""
          select sd.position, w.id as word_id, w.word, w.translation
          from session_deck sd
          join words w on w.id=sd.word_id
          where sd.session_id=$1
          order by sd.position
        """, session_id)
        return [dict(r) for r in rows]

# -------------------- EXAMPLES --------------------

async def fetch_ok_example(word_id: int, exclude_en: Optional[List[str]] = None) -> Optional[Dict]:
    """
    Берём корректный пример для слова, можно исключить тексты, которые показали утром.
    """
    exclude_en = exclude_en or []
    pool = await get_pool()
    async with pool.acquire() as c:
        if exclude_en:
            rows = await c.fetch("""
              select id, en, ru from examples
              where word_id=$1 and kind='ok' and not (en = any($2::text[]))
              order by random() limit 1
            """, word_id, exclude_en)
        else:
            rows = await c.fetch("""
              select id, en, ru from examples
              where word_id=$1 and kind='ok'
              order by random() limit 1
            """, word_id)
        return dict(rows[0]) if rows else None

# -------------------- ATTEMPTS / WALLET / EXPORT --------------------

async def record_attempt(session_id:int, word_id:int, example_id:int|None,
                        shown_en:str, shown_ru:str, truth:bool,
                        user_choice:bool, employee_card:bool, delta:int):
    pool = await get_pool()
    async with pool.acquire() as c:
        await c.execute("""
          insert into attempts(session_id, word_id, example_id,
                               shown_en, shown_ru, truth, user_choice, employee_card, delta, resolved)
          values($1,$2,$3,$4,$5,$6,$7,$8,$9,true)
        """, session_id, word_id, example_id, shown_en, shown_ru, truth, user_choice, employee_card, delta)

async def add_balance(user_id:int, delta:int):
    pool = await get_pool()
    async with pool.acquire() as c:
        await c.execute("""
          insert into wallets(user_id, balance) values ($1,$2)
          on conflict (user_id) do update set balance=wallets.balance+excluded.balance
        """, user_id, delta)

# async def fetch_export(session_id:int) -> List[Dict]:
#     pool = await get_pool()
#     async with pool.acquire() as c:
#         rows = await c.fetch("""
#           select a.created_at, w.word, w.translation, a.shown_en, a.shown_ru,
#                  a.truth, a.user_choice, a.employee_card, a.delta
#           from attempts a
#           join words w on w.id=a.word_id
#           where a.session_id=$1
#           order by a.id
#         """, session_id)
#         return [dict(r) for r in rows]

async def fetch_export(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                a.created_at::text                             AS created_at,
                COALESCE(w.word, '')                           AS word,
                COALESCE(w.translation, '')                    AS translation,
                a.shown_en,
                a.shown_ru,
                a.truth,
                a.user_choice,
                a.employee_card,
                a.delta
            FROM attempts a
            LEFT JOIN words w ON w.id = a.word_id
            WHERE a.user_id = $1
            ORDER BY a.created_at
        """, user_id)
        # Вернём список dict для удобства
        return [dict(r) for r in rows]

