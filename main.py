from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"greeting": "Hello111, World!111", "message": "Welcome to FastAPI1111!"}
