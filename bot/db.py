# bot/db.py
import os, asyncpg, ssl, socket
from urllib.parse import urlparse, unquote

_DB_POOL = None

async def get_pool():
    global _DB_POOL
    if _DB_POOL is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")

        parsed = urlparse(dsn)
        host = parsed.hostname
        port = parsed.port or 5432
        user = parsed.username
        password = unquote(parsed.password or "")
        database = (parsed.path or "/postgres").lstrip("/")

        # Форсируем IPv4 (AF_INET) — берём первый A-запись
        ipv4 = socket.getaddrinfo(host, port, socket.AF_INET)[0][4][0]
        print(f"[DB] ipv4={ipv4} db={database}")

        ssl_ctx = ssl.create_default_context()

        _DB_POOL = await asyncpg.create_pool(
            user=user,
            password=password,
            database=database,
            host=ipv4,    # ключевой момент — используем IPv4-адрес
            port=port,
            ssl=ssl_ctx,
            min_size=1,
            max_size=5,
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
