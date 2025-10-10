# Trust or Bust — English Game (app.py)
# aiogram v3, утро/вечер, выбор уровня, «замена ключевого слова»,
# экспорт CSV. DB используется только для ensure_user.

import os, csv, random, re
from io import StringIO
from dataclasses import dataclass, field
from typing import List, Dict, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, FSInputFile, BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

try:
    from db import (
        ensure_user,
        start_session,
        finish_session,
        append_result,
        get_pool,
    )
except Exception:
    # fallback на случай локального запуска без БД
    async def ensure_user(tg_id: int) -> int: return 0
    async def start_session(user_id: int, level: str) -> int: return 0
    async def finish_session(session_id: int, final_balance: int): pass
    async def append_result(*args, **kwargs): pass
    async def get_pool():  # чтобы код не падал, если где-то вызовется
        raise RuntimeError("DB pool is unavailable in fallback mode")


# ---------- CONFIG ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

# ---------- ICONS ----------
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

# ---------- DATA ----------
@dataclass
class Example:
    text: str
    text_ru: str
    uses: List[str]
    is_correct: bool
    employee_proposal: Optional[str] = None
    employee_proposal_ru: Optional[str] = None
    error_type: Optional[str] = None
    error_highlight: List[str] = field(default_factory=list)
    explanation: Optional[str] = None
    correct_note: Optional[str] = None

@dataclass
class WordCard:
    word: str
    translation: str

@dataclass
class EveningItem:
    example: Example
    employee_card: bool  # True=считает верным, False=считает неверным

@dataclass
class UserState:
    stage: str = "idle"
    level: str = "A2"
    pos: str = "adjectives"
    balance: int = 0
    session_id: int = 0
    deck: List[WordCard] = field(default_factory=list)
    word2id: Dict[str, int] = field(default_factory=dict)
    morning_idx: int = 0
    evening_idx: int = 0
    evening_queue: List[EveningItem] = field(default_factory=list)
    results: List[Dict] = field(default_factory=list)
    study_bank: Dict[str, List[tuple]] = field(default_factory=dict)


USERS: Dict[int, UserState] = {}

# ---------- BANKS ----------
A2_ADJ = [WordCard("big","большой"), WordCard("small","маленький"),
          WordCard("easy","лёгкий"), WordCard("hard","трудный"),
          WordCard("busy","занятой")]

B1_ADJ = [
    WordCard("reliable","надёжный"),
    WordCard("efficient","эффективный"),
    WordCard("flexible","гибкий"),
    WordCard("confident","уверенный"),
    WordCard("accurate","точный"),
    WordCard("productive","продуктивный"),
    WordCard("creative","креативный"),
]

B2_ADJ = [
    WordCard("meticulous","дотошный"),
    WordCard("versatile","разносторонний"),
    WordCard("robust","надёжный/устойчивый"),
    WordCard("scalable","масштабируемый"),
    WordCard("redundant","избыточный"),
]

WORD_BANK: Dict[str, List[WordCard]] = {
    "A1": A2_ADJ,   # следующий уровень
    "A2": B1_ADJ,
    "B1": B2_ADJ,
    "B2": B2_ADJ,
}

# Утренние (один корректный пример на слово)
STAGE1_EXAMPLES: Dict[str, Example] = {
    "reliable":  Example("Our team is reliable and finishes tasks on time.",
                         "Наша команда надёжная и завершает задачи вовремя.", ["reliable"], True),
    "efficient": Example("This tool is efficient for our project.",
                         "Этот инструмент эффективен для нашего проекта.", ["efficient"], True),
    "flexible":  Example("We need a flexible plan for the week.",
                         "Нам нужен гибкий план на неделю.", ["flexible"], True),
    "confident": Example("She is confident about the interview.",
                         "Она уверена насчёт собеседования.", ["confident"], True),
    "accurate":  Example("We need accurate data for the report.",
                         "Нам нужны точные данные для отчёта.", ["accurate"], True),
    "productive":Example("A short break can make you more productive.",
                         "Короткий перерыв может сделать вас более продуктивным.", ["productive"], True),
    "creative":  Example("We need a creative idea for this ad.",
                         "Нам нужна креативная идея для этой рекламы.", ["creative"], True),

    "meticulous": Example("She is meticulous and checks every detail.",
                          "Она дотошная и проверяет каждую деталь.", ["meticulous"], True),
    "versatile":  Example("A versatile employee can do many different tasks.",
                          "Разносторонний сотрудник может выполнять много разных задач.", ["versatile"], True),
    "robust":     Example("The system is robust and works under heavy load.",
                          "Система надёжная и работает под высокой нагрузкой.", ["robust"], True),
    "scalable":   Example("Our product is scalable and can handle more users.",
                          "Наш продукт масштабируемый и может выдерживать больше пользователей.", ["scalable"], True),
    "redundant":  Example("This step is redundant in our process.",
                          "Этот шаг избыточен в нашем процессе.", ["redundant"], True),
}

# Вечерние альтернативы (чтобы не повторять утренний пример)
ALT_OK: Dict[str, List[Example]] = {
    "reliable":  [Example("A reliable colleague keeps promises.", "Надёжный коллега держит обещания.", ["reliable"], True)],
    "efficient": [Example("An efficient team saves time and budget.", "Эффективная команда экономит время и бюджет.", ["efficient"], True)],
    "flexible":  [Example("Flexible policies help employees.", "Гибкие правила помогают сотрудникам.", ["flexible"], True)],
    "confident": [Example("I feel confident after preparation.", "Я чувствую уверенность после подготовки.", ["confident"], True)],
    "accurate":  [Example("Accurate numbers are important for decisions.", "Точные цифры важны для принятия решений.", ["accurate"], True)],
    "productive":[Example("I had a productive day at work.", "У меня был продуктивный день на работе.", ["productive"], True)],
    "creative":  [Example("She came up with a creative solution.", "Она придумала креативное решение.", ["creative"], True)],
    "meticulous":[Example("He is meticulous and checks every line.", "Он дотошный и проверяет каждую строчку.", ["meticulous"], True)],
    "versatile": [Example("A versatile tool is useful in many situations.", "Разносторонний инструмент полезен во многих ситуациях.", ["versatile"], True)],
    "robust":    [Example("This app is robust and rarely crashes.", "Это приложение надёжно и редко падает.", ["robust"], True)],
    "scalable":  [Example("The platform is scalable for future growth.", "Платформа масштабируема для будущего роста.", ["scalable"], True)],
    "redundant": [Example("We removed redundant details from the report.", "Мы убрали избыточные детали из отчёта.", ["redundant"], True)],
}

def kb_intro():
    kb = InlineKeyboardBuilder()
    kb.button(text="Какой процесс?", callback_data="show_process")
    kb.adjust(1)
    return kb.as_markup()

def kb_process_only_choose_level():
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{MAG} Выбрать уровень", callback_data="choose_level")
    kb.adjust(1)
    return kb.as_markup()

def kb_main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{START_EMOJI} К игре", callback_data="start_day")
    kb.button(text=f"{MAG} Выбрать уровень", callback_data="choose_level")
    kb.button(text=f"{EXPORT_EMOJI} Экспорт CSV", callback_data="export_csv")
    kb.adjust(1)
    return kb.as_markup()

def kb_levels():
    kb = InlineKeyboardBuilder()
    for lvl in ["A1","A2","B1","B2"]:
        kb.button(text=lvl, callback_data=f"set_level:{lvl}")
    kb.adjust(4)
    return kb.as_markup()

def kb_next(label=None, data="morning_next"):
    if label is None:
        label = f"{ARROW} К следующему слову"
    kb = InlineKeyboardBuilder()
    kb.button(text=label, callback_data=data)
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

def collect_examples_for_word(word: str) -> List[tuple]:
    pairs: List[tuple] = []
    if word in STAGE1_EXAMPLES:
        e = STAGE1_EXAMPLES[word]
        pairs.append((e.text, e.text_ru))
    for alt in ALT_OK.get(word, []):
        pairs.append((alt.text, alt.text_ru))
    # уникализируем
    seen, uniq = set(), []
    for t, r in pairs:
        key = (t.strip(), r.strip())
        if key not in seen:
            seen.add(key); uniq.append(key)
    return uniq

def _preserve_case(src: str, repl: str) -> str:
    if src.isupper():
        return repl.upper()
    if src and src[0].isupper():
        return repl.capitalize()
    return repl

def swap_word_everywhere(text: str, target: str, replacement: str) -> str:
    pattern = re.compile(rf"\b{re.escape(target)}\b", re.IGNORECASE)
    def _f(m: re.Match) -> str:
        return _preserve_case(m.group(0), replacement)
    return pattern.sub(_f, text)

def make_wrong_swapped_from_bank(base_word: str, deck_words: List[str], study_bank: Dict[str, List[tuple]]) -> Optional[Example]:
    pairs = study_bank.get(base_word) or []
    if not pairs:
        return None
    base_text, base_ru = random.choice(pairs)
    candidates = [w for w in deck_words if w.lower() != base_word.lower()]
    if not candidates:
        return None
    replacement = random.choice(candidates)
    swapped = swap_word_everywhere(base_text, base_word, replacement)
    if swapped == base_text:
        return None
    return Example(
        text=swapped,
        text_ru=base_ru,         # перевод оставляем оригинальный
        uses=[base_word],
        is_correct=False,
        employee_proposal=base_text,
        employee_proposal_ru=base_ru,
        error_type='semantic',
        error_highlight=[replacement],
        explanation="Ключевое слово подменено на другое изучаемое слово."
    )

async def build_evening_queue(deck: List[WordCard], study_bank: Dict[str, List[tuple]], word2id: Dict[str, int]) -> List[EveningItem]:
     deck_words = [c.word for c in deck]
     queue: List[EveningItem] = []
     for card in deck:
         ok_pool, bad_pool = await db_pick_evening_pools(word2id[card.word])
         base_ok = random.choice(ok_pool) if ok_pool else Example(f"This is {card.word}.", f"Это {card.word}.", [card.word], True)
         bad_ex = random.choice(bad_pool) if bad_pool else make_wrong_swapped_from_bank(card.word, deck_words, study_bank)
         candidates = [base_ok] + ([bad_ex] if bad_ex else [])
         ex = random.choice(candidates)
         
         queue.append(EveningItem(example=ex, employee_card=True))
     random.shuffle(queue)
     return queue


# --- DB helpers for content (NEW) ---

async def db_pick_deck(level: str, pos: str, k: int = 5):
    """Выбрать k слов для колоды по level+pos."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        select id, word, translation
        from words
        where level = $1 and pos = $2
        order by random()
        limit $3
        """,
        level, pos, k
    )
    # Вернём WordCard[] и map word->id
    deck = [WordCard(r["word"], r["translation"]) for r in rows]
    word2id = {r["word"]: r["id"] for r in rows}
    return deck, word2id

async def db_pick_morning_example(word_id: int):
    """Корректный пример для утреннего этапа."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        select en, ru
        from examples
        where word_id = $1 and kind in ('ok','alt_ok')
        order by (case when kind='ok' then 0 else 1 end), random()
        limit 1
        """,
        word_id
    )
    return (row["en"], row["ru"]) if row else None

async def db_pick_evening_pools(word_id: int):
    """
    Вернём:
      ok_pool: список корректных Example,
      bad_pool: список ошибочных Example
    Если bad_pool пуст — дальше используем твою подмену (swap_word_everywhere).
    """
    pool = await get_pool()
    ok_rows = await pool.fetch(
        """select en, ru from examples
           where word_id=$1 and kind in ('ok','alt_ok')""",
        word_id
    )
    bad_rows = await pool.fetch(
        """select en, ru from examples
           where word_id=$1 and kind='bad'""",
        word_id
    )
    ok_pool = [Example(r["en"], r["ru"], uses=[], is_correct=True) for r in ok_rows]
    bad_pool = [Example(r["en"], r["ru"], uses=[], is_correct=False) for r in bad_rows]
    return ok_pool, bad_pool

# ---------- BOT ----------
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def on_start(m: Message):
    # регистрация в БД (без падения, если БД недоступна)
    try:
        uid = await ensure_user(m.from_user.id)
        suffix = f"\nВаш профиль создан (id={uid})." if uid else ""
    except Exception:
        suffix = ""
    USERS[m.from_user.id] = UserState()
    intro = (
        "👔 Welcome to *Trust or Bust: English Game*!\n\n"
        "Вы владелец маленькой консалтинговой фирмы. Компания выходит на международный рынок и вы с сотрудниками "
        "решили улучшить знание английского — каждый день учить по 5 новых слов. И подкрепить это начинание материальной составляющей :)\n\n"
        "Нажми «Какой процесс?».")
    await m.answer(intro + suffix, parse_mode="Markdown", reply_markup=kb_intro())

@dp.callback_query(F.data == "show_process")
async def show_process(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())
    s.stage = "process"
    process_text = (
        "Сначала вы договариваетесь с коллегами о словах, затем проверяете друг друга.\n\n"
        "Сотрудник принесет предложения с этими словами. Ваша задача — решить, корректно ли предложение.\n\n"
        "Вы показываете «Верю/Не верю». Сотрудник тоже показывает карточку. "
        "Если совпало — отлично. Если нет — можно «Признать» (–€50) или «Проверить в словаре» "
        "(если вы правы +€50, если нет –€100).\n\n"
        "Выберите уровень, а потом начните игру."
    )
    await cb.message.answer(process_text, reply_markup=kb_process_only_choose_level())
    await cb.answer()

@dp.callback_query(F.data == "choose_level")
async def choose_level(cb: CallbackQuery):
    await cb.message.answer("Выберите уровень: A1 / A2 / B1 / B2", reply_markup=kb_levels())
    await cb.answer()

@dp.callback_query(F.data.startswith("set_level:"))
async def set_level(cb: CallbackQuery):
    level = cb.data.split(":",1)[1]
    s = USERS.setdefault(cb.from_user.id, UserState())
    s.level = level
    await cb.message.answer(
        f"✅ Уровень установлен: *{level}*\n\nТеперь можно перейти к игре:",
        parse_mode="Markdown",
        reply_markup=kb_main_menu()
    )
    await cb.answer()

async def send_next_morning(msg: Message, s: UserState):
    if s.morning_idx >= len(s.deck):
        s.stage = "evening"
        s.evening_idx = 0
        s.evening_queue = await build_evening_queue(s.deck, s.study_bank, s.word2id)
        await msg.answer("🔎 Этап 2: Проверим предложения сотрудников")
        await send_next_evening(msg, s)
        return

    n = s.morning_idx + 1
    N = len(s.deck)
    card = s.deck[s.morning_idx]

    pair = await db_pick_morning_example(s.word2id[card.word])
    if pair:
        sample_ok = Example(pair[0], pair[1], [card.word], True)
    else:
        sample_ok = STAGE1_EXAMPLES.get(card.word)
    if not sample_ok:
        alt = ALT_OK.get(card.word, [])
        sample_ok = alt[0] if alt else Example(
            f"This is {card.word}.", f"Это {card.translation}.", [card.word], True
        )

    text = (
        f"Слово {n} из {N}\n\n"
        f"**{card.word}** — {card.translation}\n\n"
        f"Пример:\n“{sample_ok.text}”\n_{sample_ok.text_ru}_"
    )
    label = "➡️ Перейти к проверке" if n == N else None
    await msg.answer(text, parse_mode="Markdown", reply_markup=kb_next(label=label, data="morning_next"))

def collect_study_bank(deck: List[WordCard]) -> Dict[str, List[tuple]]:
    bank: Dict[str, List[tuple]] = {}
    for c in deck:
        pairs = []
        if c.word in STAGE1_EXAMPLES:
            e = STAGE1_EXAMPLES[c.word]
            pairs.append((e.text, e.text_ru))
        for alt in ALT_OK.get(c.word, []):
            pairs.append((alt.text, alt.text_ru))
        # уникализируем
        seen, uniq = set(), []
        for t, r in pairs:
            key = (t.strip(), r.strip())
            if key not in seen:
                seen.add(key); uniq.append(key)
        bank[c.word] = uniq
    return bank

@dp.callback_query(F.data == "start_day")
async def start_day(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())
    s.stage = "morning"
    s.balance = 0
    s.results.clear()
    s.morning_idx = 0
    s.evening_idx = 0
    # bank = WORD_BANK.get(s.level) or B1_ADJ
    # k = min(5, len(bank)) or 1
    # s.deck = random.sample(bank, k=k)
    s.deck, s.word2id = await db_pick_deck(s.level, s.pos, k=5)
    s.study_bank = collect_study_bank(s.deck)
    # старт новой сессии в БД
    try:
        uid = await ensure_user(cb.from_user.id)
        s.session_id = await start_session(uid, s.level)
    except Exception as e:
        # временный лог в чат и в stdout
        print("start_day DB ERROR:", repr(e))
        await cb.message.answer(f"⚠️ start_day DB ERROR: {e!r}")
        s.session_id = 0  # офлайн-режим

    await cb.message.answer("📘 Этап 1: Определимся со словами")
    await send_next_morning(cb.message, s)
    await cb.answer()

@dp.callback_query(F.data == "morning_next")
async def on_morning_next(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())
    if s.stage != "morning":
        await cb.answer("Сейчас не этап слов.", show_alert=True); return
    s.morning_idx += 1
    await send_next_morning(cb.message, s)
    await cb.answer()

async def send_next_evening(msg: Message, s: UserState):
    if s.evening_idx >= len(s.evening_queue):
        s.stage = "done"
        correct = sum(1 for r in s.results if r["result"] == "match")
        disputes = sum(1 for r in s.results if str(r["result"]).startswith("dispute"))
        summary = f"""\n{FLAG} День завершён.
Совпало: {correct}
Споров: {disputes}
Баланс: €{s.balance}"""
        try:
            if s.session_id:
                await finish_session(s.session_id, s.balance)
        except Exception:
            pass
        await msg.answer(summary, reply_markup=kb_main_menu())
        return

    item = s.evening_queue[s.evening_idx]
    ex = item.example
    body = f"""Предложение {s.evening_idx+1}/{len(s.evening_queue)}:

“{ex.text}”
_{ex.text_ru}_

Веришь, что корректно?"""
    await msg.answer(body, parse_mode="Markdown", reply_markup=kb_believe())

@dp.callback_query(F.data.startswith("believe:"))
async def on_believe(cb: CallbackQuery):
    user_choice = cb.data.split(":")[1] == "True"
    s = USERS.setdefault(cb.from_user.id, UserState())
    if s.stage != "evening":
        await cb.answer("Сейчас не проверка.", show_alert=True); return

    item = s.evening_queue[s.evening_idx]
    ex = item.example
    truth = ex.is_correct

    # если игрок ошибся → сотрудник показывает истину (всегда)
    # если игрок прав → сотрудник может ошибиться (30%)
    if user_choice != truth:
        employee_card = truth
    else:
        employee_card = truth if random.random() < 0.7 else (not truth)

    await cb.message.answer(
        f"{EMP} Карточка сотрудника: " + (f"{CHECK} Верно" if employee_card else f"{CROSS} Не верно")
    )

    # совпали карточки — просто далее
    if user_choice == employee_card:
        s.results.append({
            "text": ex.text,
            "truth": truth,
            "your_choice": user_choice,
            "employee_card": employee_card,
            "result": "match",
            "delta": 0
        })
        # лог в БД
        try:
            if s.session_id:
                # баланс не меняется
                await append_result(
                    s.session_id,
                    s.evening_idx,      # текущий индекс
                    ex.text, ex.text_ru,
                    truth,
                    user_choice,
                    employee_card,
                    "match",
                    0,
                    s.balance
                )
        except Exception as e:
            print("append_result ERROR:", repr(e))
            await cb.message.answer(f"⚠️ append_result ERROR: {e!r}")
        await cb.message.answer("👍 Совпало. Идём дальше.")
        s.evening_idx += 1
        await send_next_evening(cb.message, s)
        await cb.answer(); return

    # разногласие
    proposal = ex.employee_proposal
    proposal_ru = ex.employee_proposal_ru

    if not employee_card:
        # сотрудник сказал ❌
        if truth is True:
            # игрок прав, сотрудник ошибается → он предложит «поправку», которая ошибочна:
            # сделаем её как «слово-подмена»
            deck_words = [c.word for c in s.deck]
            wrong_pair = None
            # соберём корректный Example, из которого сделаем ошибочный
            src_ok = Example(text=ex.text, text_ru=ex.text_ru, uses=ex.uses, is_correct=True)
            # переиспользуем механизм замены:
            # NOTE: здесь мы оставляем перевод оригинала
            if src_ok.uses:
                base_word = src_ok.uses[0]
                candidates = [w for w in deck_words if w.lower() != base_word.lower()]
                if candidates:
                    repl = random.choice(candidates)
                    wrong_text = swap_word_everywhere(src_ok.text, base_word, repl)
                    if wrong_text != src_ok.text:
                        proposal, proposal_ru = wrong_text, src_ok.text_ru
        else:
            # истина = ошибка, сотрудник прав → предложит корректный вариант
            if not proposal and ex.uses:
                base = STAGE1_EXAMPLES.get(ex.uses[0])
                if base:
                    proposal, proposal_ru = base.text, base.text_ru
    else:
        proposal = None
        proposal_ru = None

    if proposal:
        await cb.message.answer(f"""{NOTE} Сотрудник предлагает вариант:
“{proposal}”
_{proposal_ru or ''}_""", parse_mode="Markdown")
    else:
        await cb.message.answer(f"{EMP} Сотрудник настаивает на своём варианте.")

    s.results.append({
        "text": ex.text,
        "truth": truth,
        "your_choice": user_choice,
        "employee_card": employee_card,
        "result": "dispute_wait",
        "delta": None
    })

    await cb.message.answer("Твой ход:", reply_markup=kb_after_employee())
    await cb.answer()

def format_with_highlights(text: str, highlights: List[str]) -> str:
    out = text
    for frag in sorted(highlights, key=len, reverse=True):
        out = out.replace(frag, f"**_{frag}_**")
    return out

@dp.callback_query(F.data.startswith("dispute:"))
async def on_dispute(cb: CallbackQuery):
    action = cb.data.split(":",1)[1]
    s = USERS.setdefault(cb.from_user.id, UserState())
    if s.stage != "evening":
        await cb.answer("Сейчас не спор.", show_alert=True); return

    idx = next((i for i in range(len(s.results)-1, -1, -1)
                if s.results[i]["result"] == "dispute_wait" and s.results[i]["delta"] is None), None)
    if idx is None:
        await cb.answer("Не найден спор.", show_alert=True); return

    item = s.evening_queue[s.evening_idx]
    ex = item.example
    truth = ex.is_correct
    your_choice = s.results[idx]["your_choice"]

    if truth:
        note = ex.correct_note or "Предложение корректно."
        highlighted = ex.text
    else:
        note = ex.explanation or "В предложении есть ошибка."
        highlighted = format_with_highlights(ex.text, ex.error_highlight)

    if action == "concede":
        s.balance -= 50
        try:
            if s.session_id:
                await append_result(
                    s.session_id, s.evening_idx,
                    ex.text, ex.text_ru, truth,
                    your_choice, s.results[idx]["employee_card"],
                    "dispute_concede",
                    -50, s.balance
                )
        except Exception as e:
            print("append_result ERROR:", repr(e))
            await cb.message.answer(f"⚠️ append_result ERROR: {e!r}")
        s.results[idx]["result"] = "dispute_concede"
        s.results[idx]["delta"] = -50
        await cb.message.answer(f"{GREEN} «Ты прав». Вы платите сотруднику €50.")
    elif action == "check":
        if your_choice == truth:
            s.balance += 50
            try:
                if s.session_id:
                    await append_result(
                        s.session_id, s.evening_idx,
                        ex.text, ex.text_ru, truth,
                        your_choice, s.results[idx]["employee_card"],
                        "dispute_check_win",
                        +50, s.balance
                        )
            except Exception as e:
                print("append_result ERROR:", repr(e))
                await cb.message.answer(f"⚠️ append_result ERROR: {e!r}")
            s.results[idx]["result"] = "dispute_check_win"
            s.results[idx]["delta"] = +50
            await cb.message.answer(f"""{CHECK} Проверка: вы оказались правы. Сотрудник пристыжен.
{note}

“{highlighted}”""", parse_mode="Markdown")
        else:
            s.balance -= 100
            try:
                if s.session_id:
                    await append_result(
                        s.session_id, s.evening_idx,
                        ex.text, ex.text_ru, truth,
                        your_choice, s.results[idx]["employee_card"],
                        "dispute_check_lose",
                        -100, s.balance
                    )
            except Exception as e:
                print("append_result ERROR:", repr(e))
                await cb.message.answer(f"⚠️ append_result ERROR: {e!r}")
            s.results[idx]["result"] = "dispute_check_lose"
            s.results[idx]["delta"] = -100
            await cb.message.answer(f"""{CROSS} Проверка: вы оказались неправы. Сотрудник ликует.
{note}

“{highlighted}”""", parse_mode="Markdown")
    else:
        await cb.answer("Неизвестное действие.", show_alert=True); return

    s.evening_idx += 1
    await send_next_evening(cb.message, s)
    await cb.answer()

@dp.callback_query(F.data == "export_csv")
async def export_csv(cb: CallbackQuery):
    s = USERS.setdefault(cb.from_user.id, UserState())

    # 1) Пытаемся выгрузить из БД всю историю пользователя
    try:
        uid = await ensure_user(cb.from_user.id)
        pool = await get_pool()
        rows = await pool.fetch(
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
            uid
        )
        header = ["id","user_id","level","item_index","sentence_en","sentence_ru",
                  "truth","user_choice","employee_card","outcome","delta","balance_after","created_at"]
        sio = StringIO(); w = csv.writer(sio); w.writerow(header)
        for r in rows:
            w.writerow([r[h] for h in header])
        data = sio.getvalue().encode("utf-8")
        buf = BufferedInputFile(data, filename=f"results_{cb.from_user.id}.csv")
        await cb.message.answer_document(buf, caption="📄 Экспорт из БД готов.")
        await cb.answer()
        return
    except Exception as e:
        print("export DB ERROR:", repr(e))

        # 2) Fallback — твоя текущая выгрузка из оперативной памяти
        filename = f"results_{cb.from_user.id}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["sentence","truth","your_choice","employee_card","result","delta","balance_after_row"])
            bal = 0
            for r in s.results:
                if isinstance(r.get("delta"), int):
                    bal += r["delta"]
                w.writerow([
                    r.get("text",""),
                    r.get("truth",""),
                    r.get("your_choice",""),
                    r.get("employee_card",""),
                    r.get("result",""),
                    r.get("delta",""),
                    bal
                ])
        await cb.message.answer_document(FSInputFile(filename), caption="📄 Экспорт (локально) готов.")
        await cb.answer()


@dp.message(Command("stats"))
async def on_stats(m: Message):
    s = USERS.get(m.from_user.id, UserState())

    # локальный fallback (на случай офлайна БД)
    local_total = sum(1 for r in s.results if r.get("result") in ("match","dispute_concede","dispute_check_win","dispute_check_lose"))
    local_correct = sum(1 for r in s.results if r.get("result") in ("match","dispute_concede","dispute_check_win","dispute_check_lose") and r.get("truth")==r.get("your_choice"))
    local_acc = round((local_correct/local_total)*100,1) if local_total else 0.0

    try:
        uid = await ensure_user(m.from_user.id)
        pool = await get_pool()
        row = await pool.fetchrow(
            """
            select
              count(*) as total,
              count(*) filter (where r.truth = r.user_choice) as correct,
              coalesce(sum(coalesce(r.delta,0)),0) as sum_delta
            from results r
            join sessions s on s.id = r.session_id
            where s.user_id = $1
            """,
            uid
        )
        total = row["total"] or 0
        correct = row["correct"] or 0
        acc = round((correct/total)*100,1) if total else 0.0
        await m.answer(
            "📊 Статистика (по БД)\n"
            f"Всего ответов: {total}\n"
            f"Точность: {acc}%\n"
            f"Суммарная дельта: €{row['sum_delta']}\n"
        )
    except Exception as e:
        print("stats DB ERROR:", repr(e))
        await m.answer(
            "📊 Локальная статистика (за текущую сессию)\n"
            f"Всего ответов: {local_total}\n"
            f"Точность: {local_acc}%\n"
            f"Баланс (локально): €{s.balance}\n"
        )

# Отладка

@dp.message(Command("dbping"))
async def dbping(m: Message):
    try:
        pool = await get_pool()
        row = await pool.fetchval("select 1")
        await m.answer(f"DB ping: {row}")
    except Exception as e:
        await m.answer(f"DB ping ERROR: {e!r}")

@dp.message(Command("dbschema"))
async def dbschema(m: Message):
    try:
        pool = await get_pool()
        q = """
        select table_name, column_name, data_type
        from information_schema.columns
        where table_schema='public' and table_name in ('users','sessions','results','examples','words')
        order by table_name, ordinal_position;
        """
        rows = await pool.fetch(q)
        if not rows:
            await m.answer("нет данных по таблицам users/sessions/results/examples/words")
            return

        out = {}
        for r in rows:
            out.setdefault(r["table_name"], []).append(f"{r['column_name']}: {r['data_type']}")

        # без Markdown/HTML, чистый текст
        text = "\n\n".join([f"{t}\n- " + "\n- ".join(cols) for t, cols in out.items()])

        # телеграм ограничение ~4096 символов — шлём чанками
        limit = 3500
        for i in range(0, len(text), limit):
            await m.answer(text[i:i+limit])

    except Exception as e:
        await m.answer(f"DB schema ERROR: {e!r}")

@dp.message(Command("dbwho"))
async def dbwho(m: Message):
    # кто мы такие для Postgres (проверка роли/хоста)
    try:
        pool = await get_pool()
        row = await pool.fetchrow("select current_user as u, inet_server_addr()::text as host, inet_server_port() as port")
        await m.answer(f"user={row['u']}, host={row['host']}:{row['port']}")
    except Exception as e:
        await m.answer(f"DB who ERROR: {e!r}")

@dp.message(Command("dbcount"))
async def dbcount(m: Message):
    """Показывает количество записей и 'последнее обновление' по ключевым таблицам (под твою схему)."""
    try:
        pool = await get_pool()
        q = """
        with
        u as (
          select count(*) as cnt, max(created_at) as updated
          from users
        ),
        s as (
          select count(*) as cnt, max(created_at) as updated
          from sessions
        ),
        r as (
          select count(*) as cnt, max(created_at) as updated
          from results
        ),
        w as (
          select
            (select count(*) from words) as cnt,
            case
              when exists (
                select 1 from information_schema.columns
                where table_schema='public' and table_name='words' and column_name='created_at'
              )
              then (select max(created_at) from words)
              else null
            end as updated
        ),
        e as (
          select
            (select count(*) from examples) as cnt,
            case
              when exists (
                select 1 from information_schema.columns
                where table_schema='public' and table_name='examples' and column_name='created_at'
              )
              then (select max(created_at) from examples)
              else null
            end as updated
        )
        select
          (select cnt from u) as users_cnt,
          (select updated from u) as users_updated,
          (select cnt from s) as sessions_cnt,
          (select updated from s) as sessions_updated,
          (select cnt from r) as results_cnt,
          (select updated from r) as results_updated,
          (select cnt from w) as words_cnt,
          (select updated from w) as words_updated,
          (select cnt from e) as examples_cnt,
          (select updated from e) as examples_updated
        """
        row = await pool.fetchrow(q)

        def fmt(ts):
            return ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "—"

        txt = (
            "📊 DB counts & last updates\n"
            f"users:    {row['users_cnt']}   (updated: {fmt(row['users_updated'])})\n"
            f"sessions: {row['sessions_cnt']}   (updated: {fmt(row['sessions_updated'])})\n"
            f"results:  {row['results_cnt']}   (updated: {fmt(row['results_updated'])})\n"
            f"words:    {row['words_cnt']}   (updated: {fmt(row['words_updated'])})\n"
            f"examples: {row['examples_cnt']}   (updated: {fmt(row['examples_updated'])})"
        )
        await m.answer(txt)
    except Exception as e:
        await m.answer(f"⚠️ dbcount ERROR: {e!r}")


# ---------- RUN ----------
async def main():
    print("Bot is running…")
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
