from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from datetime import datetime, timedelta
from jose import JWTError, jwt
from typing import Optional
import grpc
import os, sys

# Ajustar el path para importar los protos
sys.path.append(os.path.join(os.path.dirname(__file__), '../../protos'))
from protos import namenode_pb2, namenode_pb2_grpc

SECRET_KEY = "dfs_secret_key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

NAMENODE_GRPC = "localhost:50050"

auth_router = APIRouter()

bearer_scheme = HTTPBearer(auto_error=True)

class LoginRequest(BaseModel):
    username: str

def get_namenode_stub(address):
    channel = grpc.insecure_channel(address)
    return namenode_pb2_grpc.NameNodeServiceStub(channel)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

@auth_router.post("/login")
def login(request: LoginRequest):
    stub = get_namenode_stub(NAMENODE_GRPC)
    response = stub.Login(namenode_pb2.LoginRequest(username=request.username))
    if not response.success:
        raise HTTPException(status_code=400, detail="Login fallido: " + response.message)
    access_token = create_access_token(
        data={"sub": request.username},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}

@auth_router.post("/logout")
def logout(username: str = Depends(lambda: get_current_user())):
    stub = get_namenode_stub(NAMENODE_GRPC)
    response = stub.Logout(namenode_pb2.LogoutRequest(username=username))
    if not response.success:
        raise HTTPException(status_code=400, detail="Logout fallido: " + response.message)
    return {"message": "Logout exitoso"}

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Token inválido")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido") 