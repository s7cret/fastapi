import os
import hmac
import hashlib
import json
from urllib.parse import parse_qsl
from operator import itemgetter

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

# Функция проверки подписи согласно рекомендациям Telegram WebApp:
def check_webapp_signature(token: str, init_data: str) -> bool:
    """
    Проверяет подпись initData, переданного от Telegram WebApp.

    Алгоритм:
      1. Разбираем init_data как query string.
      2. Удаляем параметр "hash".
      3. Формируем строку вида: "key1=value1\nkey2=value2\n..." 
         (ключи сортируются лексикографически).
      4. Вычисляем секретный ключ как HMAC с ключом "WebAppData" и токеном бота.
      5. Вычисляем контрольный хэш.
      6. Сравниваем вычисленный хэш с полученным.
    """
    try:
        parsed_data = dict(parse_qsl(init_data))
    except ValueError:
        # init_data не является корректной строкой запроса
        return False
    if "hash" not in parsed_data:
        return False

    received_hash = parsed_data.pop("hash")
    data_check_string = "\n".join(
        f"{k}={v}" for k, v in sorted(parsed_data.items(), key=itemgetter(0))
    )
    # Вычисляем секретный ключ с использованием "WebAppData" (как указано в документации)
    secret_key = hmac.new(key=b"WebAppData", msg=token.encode(), digestmod=hashlib.sha256)
    calculated_hash = hmac.new(
        key=secret_key.digest(),
        msg=data_check_string.encode(),
        digestmod=hashlib.sha256
    ).hexdigest()
    return calculated_hash == received_hash

# Endpoint авторизации
@app.post("/api/auth")
async def auth(data: dict):
    """
    Ожидает, что клиент пришлёт JSON с полем "initData",
    содержащим исходную строку данных от Telegram.
    """
    init_data = data.get("initData")
    print("🔹 [API] initData:", init_data)
    if not init_data:
        raise HTTPException(status_code=400, detail="Missing initData")
    
    if not check_webapp_signature(BOT_TOKEN, init_data):
        raise HTTPException(status_code=403, detail="Invalid auth")
    
    # Если подпись верна, разбираем initData в словарь
    parsed_data = dict(parse_qsl(init_data))
    # Извлекаем поле "user" — оно передаётся как URL-кодированная JSON-строка
    user_json = parsed_data.get("user")
    if not user_json:
        raise HTTPException(status_code=400, detail="Missing user data")
    try:
        user_obj = json.loads(user_json)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid user data format")
    
    user_id = int(user_obj["id"])
    username = user_obj.get("username", "Unknown")
    
    # Проверяем, есть ли пользователь в базе
    result = conn.execute("SELECT clicks FROM clicks WHERE user_id = ?", (user_id,))
    user = result.fetchone()
    user_clicks = user[0] if user else 0
    
    if not user:
        conn.execute("INSERT INTO clicks (user_id, username, clicks) VALUES (?, ?, ?)",
                     (user_id, username, 0))
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
