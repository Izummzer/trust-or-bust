# bot/db.py
import os, ssl, certifi, asyncpg
from typing import Optional
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

_DB_POOL: Optional[asyncpg.Pool] = None

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

def _ensure_sslmode_verify_full(dsn: str) -> str:
    parts = urlparse(dsn)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    # важно: verify-full (а не require), чтобы hostname проверялся корректно
    q["sslmode"] = "verify-full"
    parts = parts._replace(query=urlencode(q))
    return urlunparse(parts)

def _make_ssl_ctx() -> ssl.SSLContext:
    """
    Приоритетно Supabase CA (prod-ca-2021.crt), иначе — certifi bundle.
    """
    ca_path = Path("bot/certs/prod-ca-2021.crt")
    if ca_path.exists():
        ctx = ssl.create_default_context(cafile=str(ca_path))
    else:
        ctx = ssl.create_default_context(cafile=certifi.where())
    # на всякий случай явно задаём строгую проверку
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

_SSL_CTX = _make_ssl_ctx()
_DSN_VERIFIED = _ensure_sslmode_verify_full(DATABASE_URL)

async def get_pool() -> asyncpg.Pool:
    global _DB_POOL
    if _DB_POOL is None:
        _DB_POOL = await asyncpg.create_pool(
            _DSN_VERIFIED,
            min_size=1, max_size=5,
            ssl=_SSL_CTX,
            statement_cache_size=0,  # важно из-за PgBouncer
        )
    return _DB_POOL

# --- USERS ---
async def ensure_user(tg_id: int) -> int:
    """
    Возвращает users.id. Работает и с колонкой 'telegram_id', и с 'tg_id'.
    Не требует уникального индекса: сначала SELECT, потом INSERT.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Сначала пытаемся со схемой telegram_id
        try:
            uid = await conn.fetchval("select id from users where telegram_id=$1", tg_id)
            if uid:
                return uid
            uid = await conn.fetchval(
                "insert into users(telegram_id) values($1) returning id",
                tg_id
            )
            return uid
        except asyncpg.UndefinedColumnError:
            # Фолбэк на старую схему tg_id
            uid = await conn.fetchval("select id from users where tg_id=$1", tg_id)
            if uid:
                return uid
            uid = await conn.fetchval(
                "insert into users(tg_id) values($1) returning id",
                tg_id
            )
            return uid

# --- SESSIONS ---
async def start_session(user_id: int, level: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        sid = await conn.fetchval(
            "insert into sessions(user_id, level) values($1,$2) returning id",
            user_id, level
        )
        return sid

async def finish_session(session_id: int, final_balance: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "update sessions set finished_at=now(), balance=$2 where id=$1",
            session_id, final_balance
        )

# --- RESULTS ---
async def append_result(
    session_id: int,
    item_index: int,
    sentence_en: str,
    sentence_ru: str,
    truth: bool,
    user_choice: bool,
    employee_card: bool,
    outcome: str,
    delta: Optional[int],
    balance_after: Optional[int]
) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """insert into results
               (session_id, item_index, sentence_en, sentence_ru, truth, user_choice, employee_card, outcome, delta, balance_after)
               values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
            session_id, item_index, sentence_en, sentence_ru, truth, user_choice, employee_card, outcome, delta, balance_after
        )
