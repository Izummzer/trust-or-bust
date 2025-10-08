# bot/db.py
import os, ssl, asyncpg, certifi

_DB_POOL = None

def make_ssl_ctx():
    ca_path = os.getenv("SUPABASE_CA")
    if ca_path and os.path.exists(ca_path):
        # Жёстко используем CA Supabase
        return ssl.create_default_context(cafile=ca_path)
    # Фолбэк: certifi bundle
    return ssl.create_default_context(cafile=certifi.where())

async def get_pool():
    global _DB_POOL
    if _DB_POOL is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")
        _DB_POOL = await asyncpg.create_pool(
            dsn,
            ssl=make_ssl_ctx(),
            min_size=1,
            max_size=5,
            timeout=10,
            statement_cache_size=0   # <-- ключевая строка для PgBouncer (transaction)
        )
    return _DB_POOL


async def ensure_user(tg_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (tg_id) VALUES ($1)
            ON CONFLICT (tg_id) DO NOTHING
        """, tg_id)
        row = await conn.fetchrow("SELECT id FROM users WHERE tg_id=$1", tg_id)
        return row["id"]
