import hmac
import hashlib
import json
import libsql_experimental as libsql
from fastapi import FastAPI, HTTPException
from aiogram import Bot, Dispatcher, Router, types
import asyncio

# 🔹 Настройки
BOT_TOKEN = "7537643325:AAFh38eDVxuEkeor4T57tLKPuoD57ixuQ9o"
DB_URL = "libsql://miniappbd-s7cret.turso.io"
DB_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJleHAiOjE3Njk2OTk4MzYsImlhdCI6MTczODE2MzgzNiwiaWQiOiI5M2ViYmJlOC00MWY3LTQ3MzgtOTE1Ni0xNTE0YTY5NThmNjgifQ.wDq-Xd0888uQc6T9JL4XtlGbOrMhpUH2i4OSZYbhQrIN0fejsmHwMVozR5eFp0l-R2Zpx_TOrMy1A5sg9lbjBA"

# 🔹 Инициализация FastAPI
app = FastAPI()

# 🔹 Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# 🔹 Подключение к Turso
conn = libsql.connect("miniappbd", sync_url=DB_URL, auth_token=DB_TOKEN)
conn.sync() # 🔹 Принудительная синхронизация перед SQL-запросами

# 🔹 Создаём таблицу, если её нет
conn.execute("""
    CREATE TABLE IF NOT EXISTS clicks (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        clicks INTEGER DEFAULT 0
    )
""")
conn.commit()

# 🔹 Проверка подписи Telegram WebApp
def check_telegram_auth(data):
    secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
    received_hash = data.pop("hash", None)

    check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    calculated_hash = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()

    return received_hash == calculated_hash

# 🔹 Авторизация (получаем user_id)
@app.post("/api/auth")
async def auth(data: dict):
    if not check_telegram_auth(data):
        raise HTTPException(status_code=403, detail="Invalid auth")
    
    user_id = int(data["id"])
    username = data.get("username", "Unknown")

    # Проверяем, есть ли пользователь в базе
    result = conn.execute("SELECT clicks FROM clicks WHERE user_id = ?", (user_id,))
    user = result.fetchone()

    if not user:
        conn.execute("INSERT INTO clicks (user_id, username, clicks) VALUES (?, ?, ?)", (user_id, username, 0))
        conn.commit()
        user_clicks = 0
    else:
        user_clicks = user[0]

    return {"user_id": user_id, "clicks": user_clicks}

# 🔹 Запись клика
@app.post("/api/click")
async def record_click(data: dict):
    user_id = data["user_id"]

    conn.execute("UPDATE clicks SET clicks = clicks + 1 WHERE user_id = ?", (user_id,))
    conn.commit()

    return {"status": "ok"}

# 🔹 Получение статистики всех пользователей
@app.get("/api/stats")
async def get_stats():
    result = conn.execute("SELECT username, clicks FROM clicks ORDER BY clicks DESC").fetchall()
    return [{"username": row[0], "clicks": row[1]} for row in result]

# 🔹 Бот принимает клики от Mini App
@router.message(lambda message: message.web_app_data is not None)
async def handle_webapp_data(message: types.Message):
    data = json.loads(message.web_app_data.data)
    user_id = message.from_user.id

    conn.execute("UPDATE clicks SET clicks = clicks + 1 WHERE user_id = ?", (user_id,))
    conn.commit()

    await message.answer(f"Ваши клики: {data['clicks']}")


# 🔹 Запуск бота
async def start_bot():
    await dp.start_polling(bot)

# 🔹 Запуск FastAPI + бота
if __name__ == "__main__":
    asyncio.run(start_bot())  # Запуск бота в основном потоке
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
