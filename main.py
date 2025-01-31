import os  # Импортируем модуль для работы с окружением
import hmac
import hashlib
import json
import libsql_experimental as libsql
from fastapi import FastAPI, HTTPException
from aiogram import Bot, Dispatcher, Router, types
import asyncio

# 🔹 Загружаем переменные из окружения (Railway Variables)
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_URL = os.getenv("DB_URL")
DB_TOKEN = os.getenv("DB_TOKEN")

# 🔹 Инициализация FastAPI
app = FastAPI()

# 🔹 Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# 🔹 Подключение к Turso
conn = libsql.connect("miniappbd", sync_url=DB_URL, auth_token=DB_TOKEN)
conn.sync()  # 🔹 Принудительная синхронизация перед SQL-запросами

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
    print("🔹 [Auth] Полученные данные:", data)  # Логируем запрос

    secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
    received_hash = data.pop("hash", None)

    check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    calculated_hash = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()

    print("🔹 [Auth] Received Hash:", received_hash)
    print("🔹 [Auth] Calculated Hash:", calculated_hash)

    if received_hash != calculated_hash:
        print("❌ [Auth] Подпись не совпадает!")
        return False

    print("✅ [Auth] Подпись Telegram верна!")
    return True

# 🔹 Авторизация (получаем user_id)
@app.post("/api/auth")
async def auth(data: dict):
    print("🔹 [API] Авторизация пользователя:", data)  # Логируем авторизацию

    if not check_telegram_auth(data):
        raise HTTPException(status_code=403, detail="Invalid auth")

    user_id = int(data["id"])
    username = data.get("username", "Unknown")

    # Проверяем, есть ли пользователь в базе
    result = conn.execute("SELECT clicks FROM clicks WHERE user_id = ?", (user_id,))
    user = result.fetchone()

    user_clicks = user[0] if user else 0

    if not user:
        conn.execute("INSERT INTO clicks (user_id, username, clicks) VALUES (?, ?, ?)", (user_id, username, 0))
        conn.commit()

    print(f"✅ [API] Пользователь {user_id} ({username}), кликов: {user_clicks}")
    return {"user_id": user_id, "clicks": user_clicks}

# 🔹 Запись клика
@app.post("/api/click")
async def record_click(data: dict):
    user_id = data["user_id"]
    print(f"🔹 [API] Получен клик от {user_id}")

    conn.execute("UPDATE clicks SET clicks = clicks + 1 WHERE user_id = ?", (user_id,))
    conn.commit()

    return {"status": "ok"}

# 🔹 Получение статистики всех пользователей
@app.get("/api/stats")
async def get_stats():
    print("🔹 [API] Запрос статистики")
    result = conn.execute("SELECT username, clicks FROM clicks ORDER BY clicks DESC").fetchall()
    return [{"username": row[0], "clicks": row[1]} for row in result]

# 🔹 Бот принимает клики от Mini App
@router.message(lambda message: message.web_app_data is not None)
async def handle_webapp_data(message: types.Message):
    data = json.loads(message.web_app_data.data)
    user_id = message.from_user.id

    print(f"🔹 [Bot] Клик от {user_id}, обновляем в БД")
    conn.execute("UPDATE clicks SET clicks = clicks + 1 WHERE user_id = ?", (user_id,))
    conn.commit()

    await message.answer(f"Ваши клики: {data['clicks']}")

# 🔹 Функция запуска FastAPI + бота
async def main():
    loop = asyncio.get_event_loop()

    # 🚀 Запускаем FastAPI + бота параллельно
    bot_task = loop.create_task(dp.start_polling(bot))

    import uvicorn
    server_task = loop.create_task(uvicorn.run(app, host="0.0.0.0", port=8000))

    await asyncio.gather(bot_task, server_task)

# 🔹 Запуск
if __name__ == "__main__":
    asyncio.run(main())
