import os, asyncpg, ssl, certifi

_DB_POOL = None

async def get_pool():
    global _DB_POOL
    if _DB_POOL is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")

        # Используем свежий корневой стор от certifi
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        # ТОЛЬКО для теста:
        # ssl_ctx = ssl._create_unverified_context()


        _DB_POOL = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=5,
            ssl=ssl_ctx,
            timeout=10,
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
