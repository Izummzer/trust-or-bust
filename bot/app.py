import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from aiogram.filters import CommandStart
from db import ensure_user

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://example.com")  # позже заменим на Vercel

if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN in environment (GitHub/Railway secrets)")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

def kb_main():
    kb = [
        [InlineKeyboardButton(text="🚀 К игре", callback_data="start")],
        [InlineKeyboardButton(text="🌐 Открыть WebApp", web_app=WebAppInfo(url=WEBAPP_URL))]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


@dp.message(CommandStart())
async def on_start(m: Message):
    uid = await ensure_user(m.from_user.id)
    await m.answer(
        "👔 Trust or Bust — English Game\n"
        f"Ваш профиль создан (id={uid}).",
        reply_markup=kb_main()
    )


@dp.callback_query(F.data == "start")
async def on_start_day(cb: CallbackQuery):
    await cb.message.answer("📘 Этап 1 (заглушка): здесь будем показывать 5 слов.")
    await cb.answer()

async def main():
    print("Bot is running…")
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
