import os
import hmac
import hashlib
import json
from urllib.parse import parse_qsl
from operator import itemgetter
import sqlite3
import asyncio

import libsql_experimental as libsql
from fastapi import FastAPI, HTTPException
from aiogram import Bot, Dispatcher, Router, types
import uvicorn

# 1. Загружаем переменные окружения (Railway Variables)
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_URL = os.getenv("DB_URL")
DB_TOKEN = os.getenv("DB_TOKEN")

# 2. Инициализация FastAPI
app = FastAPI()

# 3. Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# 4. Подключение к удалённой базе (Turso) через libsql
remote_conn = libsql.connect("miniappbd", sync_url=DB_URL, auth_token=DB_TOKEN)
remote_conn.sync()  # Принудительная синхронизация перед SQL-запросами

# 5. Создаём таблицу в удалённой БД, если её нет
remote_conn.execute("""
    CREATE TABLE IF NOT EXISTS clicks (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        clicks INTEGER DEFAULT 0
    )
""")
remote_conn.commit()

# 6. Создание локальной базы данных (SQLite) для асинхронных записей
local_conn = sqlite3.connect("local.db", check_same_thread=False)
local_conn.row_factory = sqlite3.Row
local_cursor = local_conn.cursor()
local_cursor.execute("""
    CREATE TABLE IF NOT EXISTS local_clicks (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        clicks INTEGER DEFAULT 0
    )
""")
local_conn.commit()

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
        return False
    if "hash" not in parsed_data:
        return False
    received_hash = parsed_data.pop("hash")
    data_check_string = "\n".join(
        f"{k}={v}" for k, v in sorted(parsed_data.items(), key=itemgetter(0))
    )
    secret_key = hmac.new(key=b"WebAppData", msg=token.encode(), digestmod=hashlib.sha256)
    calculated_hash = hmac.new(
        key=secret_key.digest(),
        msg=data_check_string.encode(),
        digestmod=hashlib.sha256
    ).hexdigest()
    return calculated_hash == received_hash

# Функция синхронизации локальной и удалённой баз данных
async def sync_databases():
    """
    Каждые 10 минут:
      - Считывает топ-100 пользователей (по кликам) из локальной БД.
      - Для каждого пользователя суммирует локальные клики с кликами в удалённой БД.
      - Очищает записи для обработанных пользователей из локальной БД.
    """
    while True:
        try:
            print("🔄 [Sync] Начало синхронизации локальной и удалённой БД")
            # Считываем топ-100 пользователей по кликам из локальной БД
            local_cursor.execute("SELECT * FROM local_clicks ORDER BY clicks DESC LIMIT 100")
            local_rows = local_cursor.fetchall()
            if not local_rows:
                print("🔄 [Sync] Нет данных для синхронизации")
            else:
                for row in local_rows:
                    user_id = row["user_id"]
                    username = row["username"]
                    local_clicks = row["clicks"]

                    # Считываем данные из удалённой БД для данного пользователя
                    result = remote_conn.execute("SELECT clicks FROM clicks WHERE user_id = ?", (user_id,))
                    remote_row = result.fetchone()
                    if remote_row:
                        # Прибавляем локальные клики к удалённым
                        new_clicks = remote_row[0] + local_clicks
                        remote_conn.execute("UPDATE clicks SET clicks = ? WHERE user_id = ?", (new_clicks, user_id))
                    else:
                        # Если записи в удалённой БД нет, вставляем новую запись
                        remote_conn.execute(
                            "INSERT INTO clicks (user_id, username, clicks) VALUES (?, ?, ?)",
                            (user_id, username, local_clicks)
                        )
                remote_conn.commit()

                # После обновления удалённой БД удаляем обработанные записи из локальной БД
                user_ids = [row["user_id"] for row in local_rows]
                # Используем параметризованный запрос для удаления записей
                placeholders = ",".join("?" for _ in user_ids)
                local_cursor.execute(f"DELETE FROM local_clicks WHERE user_id IN ({placeholders})", user_ids)
                local_conn.commit()

                print("✅ [Sync] Синхронизация завершена для", len(user_ids), "пользователей")
        except Exception as e:
            print("❌ [Sync] Ошибка синхронизации:", e)
        # Ждем 10 минут (600 секунд)
        await asyncio.sleep(600)

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
    # Разбираем initData
    parsed_data = dict(parse_qsl(init_data))
    user_json = parsed_data.get("user")
    if not user_json:
        raise HTTPException(status_code=400, detail="Missing user data")
    try:
        user_obj = json.loads(user_json)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid user data format")
    user_id = int(user_obj["id"])
    username = user_obj.get("username", "Unknown")
    
    # Читаем статистику из локальной БД
    local_cursor.execute("SELECT clicks FROM local_clicks WHERE user_id = ?", (user_id,))
    row = local_cursor.fetchone()
    if row:
        user_clicks = row["clicks"]
    else:
        # Если записи нет, вставляем новую с 0 кликов
        local_cursor.execute("INSERT INTO local_clicks (user_id, username, clicks) VALUES (?, ?, ?)",
                             (user_id, username, 0))
        local_conn.commit()
        user_clicks = 0
    print(f"✅ [API] Пользователь {user_id} ({username}), кликов: {user_clicks}")
    return {"user_id": user_id, "clicks": user_clicks}

# Endpoint для записи клика (только в локальную БД)
@app.post("/api/click")
async def record_click(data: dict):
    user_id = data["user_id"]
    print(f"🔹 [API] Получен клик от {user_id}")
    # Обновляем локальную БД
    local_cursor.execute("UPDATE local_clicks SET clicks = clicks + 1 WHERE user_id = ?", (user_id,))
    if local_cursor.rowcount == 0:
        # Если записи нет, вставляем новую (хотя теоретически запись должна уже быть)
        local_cursor.execute("INSERT INTO local_clicks (user_id, username, clicks) VALUES (?, ?, ?)",
                             (user_id, "Unknown", 1))
    local_conn.commit()
    return {"status": "ok"}

# Endpoint для получения статистики (из локальной БД)
@app.get("/api/stats")
async def get_stats():
    print("🔹 [API] Запрос статистики")
    local_cursor.execute("SELECT username, clicks FROM local_clicks ORDER BY clicks DESC")
    rows = local_cursor.fetchall()
    stats = [{"username": row["username"], "clicks": row["clicks"]} for row in rows]
    return stats

# Бот принимает клики от Mini App (также обновляет локальную БД)
@router.message(lambda message: message.web_app_data is not None)
async def handle_webapp_data(message: types.Message):
    data = json.loads(message.web_app_data.data)
    user_id = message.from_user.id
    print(f"🔹 [Bot] Клик от {user_id}, обновляем в локальной БД")
    local_cursor.execute("UPDATE local_clicks SET clicks = clicks + 1 WHERE user_id = ?", (user_id,))
    if local_cursor.rowcount == 0:
        local_cursor.execute("INSERT INTO local_clicks (user_id, username, clicks) VALUES (?, ?, ?)",
                             (user_id, "Unknown", 1))
    local_conn.commit()
    await message.answer(f"Ваши клики: {data['clicks']}")

# Функция запуска FastAPI + бота и фонового задания синхронизации
async def main():
    loop = asyncio.get_event_loop()

    # Запускаем фоновую задачу синхронизации каждые 10 минут
    sync_task = loop.create_task(sync_databases())
    
    # Запускаем бота
    bot_task = loop.create_task(dp.start_polling(bot))
    
    # Запускаем FastAPI сервер
    server_task = loop.create_task(uvicorn.run(app, host="0.0.0.0", port=8000))
    
    await asyncio.gather(sync_task, bot_task, server_task)

if __name__ == "__main__":
    asyncio.run(main())
