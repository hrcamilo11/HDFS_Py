from fastapi import FastAPI, UploadFile, Request
from src.core.namenode import NameNode

app = FastAPI()
namenode = NameNode()

@app.post("/upload")
async def upload_file(file: UploadFile):
    content = await file.read()
    blocks = namenode.allocate_blocks(len(content))
    # Aquí iría la lógica para enviar los bloques a los DataNodes vía gRPC
    return {"blocks": blocks}

@app.post("/datanode/register")
async def register_datanode(request: Request):
    data = await request.json()
    node_id = data["node_id"]
    namenode.register_datanode(node_id)
    return {"status": "registered"}

@app.post("/datanode/heartbeat")
async def datanode_heartbeat(request: Request):
    data = await request.json()
    node_id = data["node_id"]
    namenode.heartbeat(node_id)
    return {"status": "heartbeat received"}

@app.get("/file/{file_path:path}")
async def get_file_blocks(file_path: str):
    blocks = namenode.get_file_blocks(file_path)
    return {"blocks": blocks}

@app.get("/block/{block_id}")
async def get_block_locations(block_id: str):
    locations = namenode.get_block_locations(block_id)
    return {"locations": locations}

@app.post("/mkdir")
async def make_dir(request: Request):
    data = await request.json()
    dir_path = data["dir_path"]
    namenode.mkdir(dir_path)
    return {"status": "directorio creado"}

@app.post("/rmdir")
async def remove_dir(request: Request):
    data = await request.json()
    dir_path = data["dir_path"]
    namenode.rmdir(dir_path)
    return {"status": "directorio eliminado"}

@app.get("/ls/{dir_path:path}")
async def list_dir(dir_path: str):
    items = namenode.ls(dir_path)
    return {"items": items}

@app.delete("/rm/{file_path:path}")
async def remove_file(file_path: str):
    namenode.rm(file_path)
    return {"status": "archivo eliminado"}

@app.get("/get/{file_path:path}")
async def get_file_content(file_path: str):
    blocks = namenode.get_file_content(file_path)
    return {"blocks": blocks} 