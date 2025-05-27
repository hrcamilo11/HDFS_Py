from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import FileResponse
from .auth import get_current_user
from pathlib import Path
import grpc
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../protos'))
from protos import dfs_pb2, dfs_pb2_grpc, namenode_pb2, namenode_pb2_grpc

NAMENODE_GRPC = "localhost:50050"

files_router = APIRouter(
    dependencies=[Depends(get_current_user)]
)

def get_namenode_stub(address):
    channel = grpc.insecure_channel(address)
    return namenode_pb2_grpc.NameNodeServiceStub(channel)

def get_datanode_stub(address):
    channel = grpc.insecure_channel(address)
    return dfs_pb2_grpc.DataNodeServiceStub(channel)

def split_into_blocks(data: bytes, block_size=64*1024*1024):
    return [data[i:i+block_size] for i in range(0, len(data), block_size)]

@files_router.post("/put")
def put_file(dfs_path: str, file: UploadFile = File(...), username: str = Depends(get_current_user)):
    data = file.file.read()
    stub = get_namenode_stub(NAMENODE_GRPC)
    # Si dfs_path termina en / o es un directorio, agregar el nombre del archivo
    if dfs_path.endswith("/") or dfs_path == "":
        dfs_destination_path = dfs_path.rstrip("/") + "/" + file.filename
    else:
        dfs_destination_path = dfs_path
    dfs_destination_path = dfs_destination_path.replace('//', '/')
    blocks_data = split_into_blocks(data)
    resp = stub.AllocateBlocks(namenode_pb2.AllocateBlocksRequest(file_size=len(data), username=username))
    blocks = resp.block_ids
    for i, block in enumerate(blocks_data):
        block_id = blocks[i]
        loc_resp = stub.GetBlockLocations(namenode_pb2.BlockLocationRequest(block_id=block_id))
        locations = loc_resp.node_ids
        if locations:
            chosen_datanode_id = locations[0]
            datanode_index = int(chosen_datanode_id.replace("datanode", ""))
            datanode_port = 50050 + datanode_index
            datanode_address = f"localhost:{datanode_port}"
            datanode_stub = get_datanode_stub(datanode_address)
            datanode_stub.StoreBlock(dfs_pb2.BlockRequest(content=block, block_id=block_id, replica_nodes=locations))
    add_file_response = stub.AddFile(namenode_pb2.AddFileRequest(username=username, file_path=dfs_destination_path, block_ids=blocks))
    if not add_file_response.success:
        raise HTTPException(status_code=400, detail="Error al registrar el archivo en NameNode")
    return {"message": f"Archivo subido correctamente a {dfs_destination_path}"}

@files_router.get("/get")
def get_file(dfs_path: str, username: str = Depends(get_current_user)):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.GetFileBlocks(namenode_pb2.FileBlocksRequest(file_path=dfs_path, username=username))
    if not resp.block_ids:
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    block_ids = resp.block_ids
    assembled_content = b""
    for block_id in block_ids:
        loc_resp = stub.GetBlockLocations(namenode_pb2.BlockLocationRequest(block_id=block_id))
        locations = loc_resp.node_ids
        block_content = None
        for datanode_id in locations:
            try:
                datanode_index = int(datanode_id.replace("datanode", ""))
                datanode_port = 50050 + datanode_index
                datanode_address = f"localhost:{datanode_port}"
                datanode_stub = get_datanode_stub(datanode_address)
                block_resp = datanode_stub.GetBlock(dfs_pb2.GetBlockRequest(block_id=block_id))
                block_content = block_resp.content
                break
            except Exception:
                continue
        if block_content is not None:
            assembled_content += block_content
        else:
            raise HTTPException(status_code=500, detail=f"No se pudo recuperar el bloque {block_id}")
    # Guardar el archivo temporalmente para servirlo
    temp_dir = "descargas"
    os.makedirs(temp_dir, exist_ok=True)
    file_name = os.path.basename(dfs_path)
    temp_path = os.path.join(temp_dir, file_name)
    with open(temp_path, "wb") as f:
        f.write(assembled_content)
    return FileResponse(temp_path, filename=file_name, media_type="application/octet-stream")

@files_router.get("/ls")
def list_dir(dfs_path: str = "/", username: str = Depends(get_current_user)):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.ListFiles(namenode_pb2.ListFilesRequest(dir_path=dfs_path, username=username))
    # Convertir todos los items a string para evitar problemas de serialización
    files = [str(item) for item in resp.items]
    return {"files": files}

@files_router.post("/mkdir")
def make_dir(dfs_path: str, username: str = Depends(get_current_user)):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.Mkdir(namenode_pb2.MkdirRequest(dir_path=dfs_path, username=username))
    if not resp.success:
        raise HTTPException(status_code=400, detail="Error al crear directorio")
    return {"message": "Directorio creado"}

@files_router.post("/rmdir")
def remove_dir(dfs_path: str, username: str = Depends(get_current_user)):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.Rmdir(namenode_pb2.RmdirRequest(dir_path=dfs_path, username=username))
    if not resp.success:
        raise HTTPException(status_code=400, detail="Error al eliminar directorio. Puede que no exista o no esté vacío.")
    return {"message": "Directorio eliminado"}

@files_router.delete("/rm")
def remove_file(dfs_path: str, username: str = Depends(get_current_user)):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.RemoveFile(namenode_pb2.RemoveFileRequest(file_path=dfs_path, username=username))
    if not resp.success:
        raise HTTPException(status_code=400, detail="Error al eliminar archivo. Puede que no exista o sea un directorio.")
    return {"message": "Archivo eliminado"}

@files_router.post("/mv")
def move_file(src_path: str, dst_path: str, username: str = Depends(get_current_user)):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.Move(namenode_pb2.MoveRequest(source_path=src_path, destination_path=dst_path, username=username))
    if not resp.success:
        raise HTTPException(status_code=400, detail=f"Error al mover: {resp.message}")
    return {"message": f"'{src_path}' movido exitosamente a '{resp.message}'"} 