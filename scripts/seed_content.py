# scripts/seed_content.py
import csv, os, asyncio, asyncpg, ssl, certifi

DATABASE_URL = os.getenv("DATABASE_URL")  # возьми из Railway Variables при локальном запуске

async def main():
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    conn = await asyncpg.connect(DATABASE_URL, ssl=ssl_ctx)
    # words
    ids = {}
    with open('content/words.csv') as f:
        r = csv.DictReader(f)
        for row in r:
            w_id = await conn.fetchval("""
                insert into words(level,pos,word,translation)
                values($1,$2,$3,$4)
                on conflict do nothing
                returning id
            """, row['level'], row['pos'], row['word'], row['translation'])
            if not w_id:
                w_id = await conn.fetchval("select id from words where word=$1", row['word'])
            ids[row['word']] = w_id
    # examples
    with open('content/examples.csv') as f:
        r = csv.DictReader(f)
        for row in r:
            w_id = ids[row['word']]
            await conn.execute("""
              insert into examples(word_id, kind, en, ru)
              values($1,$2,$3,$4)
              on conflict do nothing
            """, w_id, row['kind'], row['en'], row['ru'])
    await conn.close()

asyncio.run(main())
