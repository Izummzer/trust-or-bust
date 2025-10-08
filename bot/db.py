import os, asyncpg, ssl
from urllib.parse import urlparse

_DB_POOL = None

async def get_pool():
    global _DB_POOL
    if _DB_POOL is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")

        parsed = urlparse(dsn)
        print(f"[DB] host={parsed.hostname} port={parsed.port} db={parsed.path}")

        ssl_ctx = ssl.create_default_context()
        _DB_POOL = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=5,
            ssl=ssl_ctx,
            timeout=10
        )
    return _DB_POOL


async def ensure_user(tg_id: int):
    pool = await get_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow("select id from users where tg_id=$1", tg_id)
        if row:
            return row["id"]
        row = await con.fetchrow(
            "insert into users (tg_id) values ($1) returning id", tg_id
        )
        return row["id"]
