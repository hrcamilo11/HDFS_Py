from fastapi import FastAPI
from .auth import auth_router
from .files import files_router

app = FastAPI(title="DFS API")

app.include_router(auth_router, prefix="/api")
app.include_router(files_router, prefix="/api") 