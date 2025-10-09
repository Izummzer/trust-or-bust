# bot/db.py
import os, ssl, certifi, asyncpg
from typing import Optional
import csv
from io import StringIO

_DB_POOL: Optional[asyncpg.Pool] = None

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

# В Railway + Supabase используем verify-full и корневой CA
_SSL_CTX = ssl.create_default_context(cafile=certifi.where())

async def get_pool() -> asyncpg.Pool:
    global _DB_POOL
    if _DB_POOL is None:
        _DB_POOL = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1, max_size=5,
            ssl=_SSL_CTX,
            statement_cache_size=0  # важно из-за pgbouncer
        )
    return _DB_POOL

# --- USERS ---
async def ensure_user(tg_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        uid = await conn.fetchval(
            """insert into users(tg_id)
               values($1)
               on conflict (tg_id) do update set tg_id=excluded.tg_id
               returning id""",
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

async def get_user_stats(user_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select
                count(*) as total,
                count(*) filter (where r.truth = r.user_choice) as correct,
                coalesce(sum(coalesce(r.delta,0)), 0) as sum_delta
            from results r
            join sessions s on s.id = r.session_id
            where s.user_id = $1
            """,
            user_id
        )
        total = row["total"] or 0
        correct = row["correct"] or 0
        accuracy = round((correct/total)*100, 1) if total else 0.0
        return {"total": total, "correct": correct, "accuracy": accuracy, "sum_delta": row["sum_delta"]}

async def export_results_csv(user_id: int) -> bytes:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select
                r.id, s.user_id, s.level, r.item_index,
                r.sentence_en, r.sentence_ru,
                r.truth, r.user_choice, r.employee_card, r.outcome,
                r.delta, r.balance_after, r.created_at
            from results r
            join sessions s on s.id = r.session_id
            where s.user_id = $1
            order by r.created_at
            """,
            user_id
        )
        header = ["id","user_id","level","item_index","sentence_en","sentence_ru",
                  "truth","user_choice","employee_card","outcome","delta",
                  "balance_after","created_at"]
        s = StringIO()
        w = csv.writer(s)
        w.writerow(header)
        for r in rows:
            w.writerow([r[h] for h in header])
        return s.getvalue().encode("utf-8")
