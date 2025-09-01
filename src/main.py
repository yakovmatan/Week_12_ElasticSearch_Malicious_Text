import asyncio
from contextlib import asynccontextmanager
from src.manager import Manager
from fastapi import FastAPI


manager = Manager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(asyncio.to_thread(manager.set_data))
    yield


app = FastAPI(lifespan=lifespan)

@app.get("/get_antisemitic_with_weapons")
def get_antisemitic_with_weapons():
    if manager.data_in_es:
        return manager.get_antisemitic_with_weapons()
    return {"message": "Data is still being processed."}

@app.get("/get_multiple_weapons")
def get_multiple_weapons():
    if manager.data_in_es:
        return manager.get_multiple_weapons()
    return {"message": f"Data is still being processed.{manager.data_in_es}"}


