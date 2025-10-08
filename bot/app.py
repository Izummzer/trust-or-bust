import os
import io, csv
import random
import re
from typing import Dict, List, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

from db import (
    ensure_user, upsert_session, set_session_status,
    pick_words_for_level, save_deck, load_deck,
    fetch_ok_example, record_attempt, add_balance, fetch_export
)

# ---------- CONFIG ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is not set")

# ---------- ASCII-SAFE ICONS ----------
CHECK = "✅"
CROSS = "❌"
EMP   = "🧑‍💼"
NOTE  = "📝"
START_EMOJI  = "🚀"
EXPORT_EMOJI = "📤"
MAG   = "🔎"
GREEN = "🟢"
ARROW = "➡️"
DOC   = "📄"
FLAG  = "🏁"

# ---------- RUNTIME STATE (минимум в памяти для UX) ----------
class UserState:
    def __init__(self):
        self.stage: str = "idle"          # idle | process | morning | evening | done
        self.level: str = "A2"
        self.session_id: Optional[int] = None
        self.deck: List[Dict] = []         # [{position, word_id, word, translation}]
        self.morning_idx: int = 0
        self.evening_idx: int = 0
        self.morning_shown: Dict[int, str] = {}  # word_id -> en (что показали утром)
        self.pending: Dict = {}            # контекст активного спора

USERS: Dict[int, UserState] = {}

# ---------- KEYBOARDS ----------
def kb_intro():
    kb = InlineKeyboardBuilder()
    kb.button(text="Какой процесс?", callback_data="show_process")
    kb.adjust(1)
    return kb.as_markup()

def kb_process_menu():
    # на этом шаге даём только "Выбрать уровень"
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{MAG} Выбрать уровень", callback_data="choose_level")
    kb.adjust(1)
    return kb.as_markup()

def kb_levels():
    kb = InlineKeyboardBuilder()
    for lvl in ["A1", "A2", "B1", "B2"]:
        kb.button(text=lvl, callback_data=f"set_level:{lvl}")
    kb.adjust(4)
    return kb.as_markup()

def kb_main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{START_EMOJI} К игре", callback_data="start_day")
    kb.button(text=f"{MAG} Выбрать уровень", callback_data="choose_level")
    kb.button(text=f"{EXPORT_EMOJI} Экспорт CSV", callback_data="export_csv")
    kb.adjust(1)
    return kb.as_markup()

def kb_next(label: Optional[str] = None, data: str = "morning_next"):
    if label is None:
        label = f"{ARROW} К следующему слову"
    kb = InlineKeyboardBuilder()
    kb.button(text=label, callback_data=data)
    kb.adjust(1)
    return kb.as_markup()

def kb_believe():
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{CHECK} Верю", callback_data="believe:True")
    kb.button(text=f"{CROSS} Не верю", callback_data="believe:False")
    kb.adjust(2)
    return kb.as_markup()

def kb_after_employee():
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{GREEN} Ты прав (–€50)", callback_data="dispute:concede")
    kb.button(text=f"{MAG} Проверим в словаре", callback_data="dispute:check")
    kb.adjust(1)
    return kb.as_markup()

# ---------- HELPERS ----------
def format_with_highlights(text: str, highlights: List[str]) -> str:
    out = text
    # более длинные фрагменты подменяем первыми
    for frag in sorted(highlights, key=len, reverse=True):
        out = out.replace(frag, f"**_{frag}_**")
    return out

def _preserve_case(src: str, repl: str) -> str:
    if src.isupper():
        return repl.upper()
    if src[:1].isupper():
        return repl.capitalize()
    return repl

def swap_studied(text: str, target: str, pool_words: List[str]) -> (str, Optional[str]):
    """
    Подмена 'target' на случайное другое из pool_words (1 раз, целым словом).
    """
    candidates = [w for w in pool_words if w.lower() != target.lower()]
    if not candidates:
        return text, None
    replacement = random.choice(candidates)

    pattern = re.compile(rf"\b{re.escape(target)}\b", flags=re.IGNORECASE)

    def repl(m: re.Match) -> str:
        return _preserve_case(m.group(0), replacement)

    swapped = pattern.sub(repl, text, count=1)
    if swapped == text:
        return text, None
    return swapped, replacement

# ---------- BOT ----------
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def on_start(m: Message):
    USERS[m.from_user.id] = UserState()
    # создаём/находим пользователя в БД сразу
    uid = await ensure_user(m.from_user.id)

    intro_text = (
        "👔 Welcome to *Trust or Bust: English Game*!\n\n"
        "Вы владелец маленькой консалтинговой фирмы. Компания выходит на международный рынок и вы с сотрудниками "
        "решили улучшить знание английского — каждый день учить по 5 новых слов. И подкрепить это начинание "
        "материальной составляющей :)\n\n"
        "Нажми «Какой процесс?».")
    await m.answer(intro_text, parse_mode="Markdown", reply_markup=kb_intro())

@dp.callback_query(F.data == "show_process")
async def show_process(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())
    s.stage = "process"
    process_text = (
        "Сначала вы договариваетесь с коллегами о словах которые учите, затем проверяете друг друга.\n\n"
        "Сотрудник принесёт предложения с этими словами. Вы оцениваете, корректно ли составлено предложение.\n"
        "Иногда сотрудник может специально включать ошибку, чтобы подловить вас.\n\n"
        "Вы голосуете «Верно/Не верно». Сотрудник тоже показывает карточку (как он задумывал предложение).\n"
        "Если мнения совпали — ок. Если нет — можно «Признать» (–€50) или «Проверить в словаре» (+€50/-€100).\n\n"
        "Выберите уровень, а затем начните игру."
    )
    await cb.message.answer(process_text, reply_markup=kb_process_menu())
    await cb.answer()

@dp.callback_query(F.data == "choose_level")
async def choose_level(cb: CallbackQuery):
    await cb.message.answer("Выберите уровень: A1 / A2 / B1 / B2", reply_markup=kb_levels())
    await cb.answer()

@dp.callback_query(F.data.startswith("set_level:"))
async def set_level(cb: CallbackQuery):
    lvl = cb.data.split(":", 1)[1]
    s = USERS.setdefault(cb.from_user.id, UserState())
    s.level = lvl
    await cb.message.answer(
        f"✅ Уровень установлен: *{lvl}*\n\nТеперь можно перейти к игре:",
        parse_mode="Markdown",
        reply_markup=kb_main_menu()
    )
    await cb.answer()

@dp.callback_query(F.data == "start_day")
async def start_day(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())
    s.stage = "morning"
    s.morning_idx = 0
    s.evening_idx = 0
    s.morning_shown.clear()
    s.pending.clear()

    user_id = await ensure_user(cb.from_user.id)
    # создаём сессию
    s.session_id = await upsert_session(user_id, s.level)
    # набираем 5 слов следующего уровня, сохраняем в deck
    words = await pick_words_for_level(s.level, pos="adjectives", n=5)
    await save_deck(s.session_id, [w["id"] for w in words])
    s.deck = await load_deck(s.session_id)

    await cb.message.answer("📘 Этап 1: Определимся со словами")
    await send_next_morning(cb.message, s)
    await cb.answer()

async def send_next_morning(msg: Message, s: UserState):
    # переход к вечеру
    if s.morning_idx >= len(s.deck):
        await set_session_status(s.session_id, "evening")
        s.stage = "evening"
        s.evening_idx = 0
        await msg.answer("📗 Этап 2: Проверим предложения сотрудников")
        await send_next_evening(msg, s)
        return

    item = s.deck[s.morning_idx]
    ok = await fetch_ok_example(item["word_id"])
    # запомним показанный утром EN, чтоб избежать повтора вечером
    if ok:
        s.morning_shown[item["word_id"]] = ok["en"]
        text = (
            f"Слово {s.morning_idx+1}/5\n\n"
            f"*{item['word']}* — {item['translation']}\n\n"
            f"Пример:\n“{ok['en']}”\n_{ok['ru']}_"
        )
    else:
        text = (
            f"Слово {s.morning_idx+1}/5\n\n"
            f"*{item['word']}* — {item['translation']}\n\n"
            f"_Пример пока не добавлен._"
        )
    # на последней карточке меняем подпись
    last = (s.morning_idx + 1 == len(s.deck))
    await msg.answer(text, parse_mode="Markdown",
                     reply_markup=kb_next("➡️ Перейти к проверке" if last else None, "morning_next"))

@dp.callback_query(F.data == "morning_next")
async def on_morning_next(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())
    if s.stage != "morning":
        await cb.answer("Сейчас не этап слов.", show_alert=True); return
    s.morning_idx += 1
    await send_next_morning(cb.message, s)
    await cb.answer()

async def send_next_evening(msg: Message, s: UserState):
    if s.evening_idx >= len(s.deck):
        await set_session_status(s.session_id, "done")
        s.stage = "done"
        await msg.answer("🏁 День завершён. Можешь экспортировать результаты.", reply_markup=kb_main_menu())
        return

    deck_words = [d["word"] for d in s.deck]
    item = s.deck[s.evening_idx]

    # взять корректный пример НЕ равный утреннему
    exclude = [s.morning_shown.get(item["word_id"])] if s.morning_shown.get(item["word_id"]) else []
    ok = await fetch_ok_example(item["word_id"], exclude_en=exclude)
    if ok:
        base_en, base_ru, base_id = ok["en"], ok["ru"], ok["id"]
    else:
        base_en, base_ru, base_id = f"This is {item['word']}.", f"Это {item['translation']}.", None

    # построить неверный вариант — подмена ключевого слова на другое из пятёрки
    swapped_en, replaced = swap_studied(base_en, item["word"], deck_words)
    bad_available = (replaced is not None and swapped_en != base_en)

    # выбрать, что показать игроку
    if bad_available and random.random() < 0.5:
        shown_en, shown_ru, truth, example_id = swapped_en, base_ru, False, base_id
    else:
        shown_en, shown_ru, truth, example_id = base_en, base_ru, True, base_id

    # сохранить контекст задания для этапа спора
    s.pending = {
        "session_id": s.session_id,
        "word_id": item["word_id"],
        "shown_en": shown_en,
        "shown_ru": shown_ru,
        "truth": truth,
        "example_id": example_id,
        "correct_en": base_en,
        "correct_ru": base_ru,
    }

    body = (
        f"Предложение {s.evening_idx+1}/5:\n\n"
        f"“{shown_en}”\n_{shown_ru}_\n\n"
        f"Веришь, что корректно?"
    )
    await msg.answer(body, parse_mode="Markdown", reply_markup=kb_believe())

@dp.callback_query(F.data.startswith("believe:"))
async def on_believe(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())
    if s.stage != "evening" or not s.pending:
        await cb.answer("Сейчас не проверка.", show_alert=True); return

    user_choice = (cb.data.split(":")[1] == "True")
    p = s.pending
    truth = p["truth"]

    # карточка сотрудника: если игрок ошибся → сотрудник показывает истину; если прав → 30% ошибается
    if user_choice != truth:
        employee_card = truth
    else:
        employee_card = truth if random.random() < 0.7 else (not truth)

    await cb.message.answer("🧑‍💼 Карточка сотрудника: " + ("✅ Верно" if employee_card else "❌ Не верно"))

    if user_choice == employee_card:
        # мнения совпали — фиксируем попытку без денег и идём дальше
        await record_attempt(
            s.session_id, p["word_id"], p["example_id"],
            p["shown_en"], p["shown_ru"], truth,
            user_choice, employee_card, 0
        )
        await cb.message.answer("👍 Совпало. Идём дальше.")
        s.evening_idx += 1
        s.pending = {}
        await send_next_evening(cb.message, s)
        await cb.answer(); return

    # разногласие: если сотрудник сказал «❌», он показывает "как правильно"
    if not employee_card:
        await cb.message.answer(
            "📝 Сотрудник предлагает вариант:\n"
            f"“{p['correct_en']}”\n_{p['correct_ru']}_",
            parse_mode="Markdown"
        )
    else:
        await cb.message.answer("🧑‍💼 Сотрудник настаивает на своём варианте.")

    # сохраним параметры спора
    p["user_choice"] = user_choice
    p["employee_card"] = employee_card
    s.pending = p

    await cb.message.answer("Твой ход:", reply_markup=kb_after_employee())
    await cb.answer()

@dp.callback_query(F.data.startswith("dispute:"))
async def on_dispute(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())
    if s.stage != "evening" or not s.pending:
        await cb.answer("Нет активного спора.", show_alert=True); return

    action = cb.data.split(":")[1]  # concede | check
    p = s.pending
    truth = p["truth"]
    user_choice = p["user_choice"]
    employee_card = p["employee_card"]

    # подсчёт дельты
    if action == "concede":
        delta = -50
        note  = "Признали сотрудника: −€50."
    else:
        if user_choice == truth:
            delta = +50
            note  = "Проверили: вы были правы. +€50."
        else:
            delta = -100
            note  = "Проверили: вы были неправы. −€100."

    # запись в БД
    user_id = await ensure_user(cb.from_user.id)
    await add_balance(user_id, delta)
    await record_attempt(
        s.session_id, p["word_id"], p["example_id"],
        p["shown_en"], p["shown_ru"], truth,
        user_choice, employee_card, delta
    )

    await cb.message.answer(note)
    s.pending = {}
    s.evening_idx += 1
    await send_next_evening(cb.message, s)
    await cb.answer()

# @dp.callback_query(F.data == "export_csv")
# async def export_csv(cb: CallbackQuery):
#     s = USERS.setdefault(cb.from_user.id, UserState())
#     if not s.session_id:
#         await cb.answer("Нет активной сессии.", show_alert=True); return

#     rows = await fetch_export(s.session_id)
#     if not rows:
#         await cb.message.answer("Пока нет результатов для экспорта."); return

#     # формируем CSV в памяти
#     from io import BytesIO
#     buf = BytesIO()
#     w = csv.writer(buf)
#     w.writerow(["created_at","word","translation","shown_en","shown_ru","truth","user_choice","employee_card","delta"])
#     for r in rows:
#         w.writerow([
#             r["created_at"], r["word"], r["translation"], r["shown_en"], r["shown_ru"],
#             r["truth"], r["user_choice"], r["employee_card"], r["delta"]
#         ])
#     buf.seek(0)
#     await cb.message.answer_document(document=("results.csv", buf))

@dp.callback_query(F.data == "export_csv")
async def export_csv(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())

    # Пишем в текстовый буфер (StringIO) → затем кодируем в bytes
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["created_at","word","translation","shown_en","shown_ru","truth","user_choice","employee_card","delta","balance_after_row"])

    bal = 0
    # если у тебя есть сохранение даты — подставь, иначе оставим пусто
    for r in s.results:
        if isinstance(r.get("delta"), int):
            bal += r["delta"]
        w.writerow([
            r.get("created_at",""),
            r.get("word",""),
            r.get("translation",""),
            r.get("text",""),
            r.get("text_ru",""),
            r.get("truth",""),
            r.get("your_choice",""),
            r.get("employee_card",""),
            r.get("delta",""),
            bal
        ])

    # конвертируем в bytes (можно с BOM, чтобы Excel открыл корректно)
    data = buf.getvalue().encode("utf-8-sig")
    buf.close()

    filename = f"results_{cb.from_user.id}.csv"
    file = BufferedInputFile(data=data, filename=filename)

    await cb.message.answer_document(
        document=file,
        caption="📄 Экспорт результатов (CSV)"
    )

async def main():
    print("Bot is running…")
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
