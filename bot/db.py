import os, asyncpg, ssl

_DB_POOL = None

async def get_pool():
    global _DB_POOL
    if _DB_POOL is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")

        ssl_ctx = ssl.create_default_context()  # проверяет сертификат
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
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (telegram_id)
            VALUES ($1)
            ON CONFLICT (telegram_id) DO NOTHING
            """,
            tg_id
        )
        row = await conn.fetchrow("SELECT id FROM users WHERE telegram_id=$1", tg_id)
        return row["id"] if row else None
