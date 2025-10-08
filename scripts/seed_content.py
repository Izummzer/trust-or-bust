# scripts/seed_content.py
import os, csv, asyncio, asyncpg, ssl, certifi
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

load_dotenv()  # читает .env, если есть в корне

DATABASE_URL = os.getenv("DATABASE_URL")

def ensure_sslmode(dsn: str, mode: str = "verify-full") -> str:
    # добавить/заменить sslmode=verify-full в строке подключения
    parts = urlparse(dsn)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q["sslmode"] = mode
    parts = parts._replace(query=urlencode(q))
    return urlunparse(parts)

def make_ssl_ctx() -> ssl.SSLContext:
    # приоритетно используем Supabase CA, если файл есть
    ca_path = Path("bot/certs/prod-ca-2021.crt")
    if ca_path.exists():
        ctx = ssl.create_default_context(cafile=str(ca_path))
        return ctx
    # иначе — certifi bundle
    return ssl.create_default_context(cafile=certifi.where())

async def main():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set (env or .env)")

    dsn = ensure_sslmode(DATABASE_URL, "verify-full")  # именно verify-full
    ssl_ctx = make_ssl_ctx()

    # Если используешь Transaction Pooler (порт 6543) — выключаем prepared statements
    conn = await asyncpg.connect(dsn, ssl=ssl_ctx, statement_cache_size=0)

    ids = {}
    with open('content/words.csv', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            w_id = await conn.fetchval("""
                insert into words(level,pos,word,translation)
                values($1,$2,$3,$4)
                on conflict (level,pos,word) do update
                  set translation = excluded.translation
                returning id
            """, row['level'], row['pos'], row['word'], row['translation'])
            if not w_id:
                w_id = await conn.fetchval("select id from words where word=$1", row['word'])
            ids[row['word']] = w_id

    with open('content/examples.csv', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            w_id = ids[row['word']]
            await conn.execute("""
                insert into examples(word_id, kind, en, ru)
                values($1,$2,$3,$4)
                on conflict do nothing
            """, w_id, row['kind'], row['en'], row['ru'])

    await conn.close()
    print("Seed OK")

if __name__ == "__main__":
    asyncio.run(main())
