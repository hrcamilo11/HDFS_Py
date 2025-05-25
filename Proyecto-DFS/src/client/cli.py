import sys
import os
# Calculate the project root directory (Proyecto-DFS)
# This script is in .../Proyecto-DFS/src/client/
# Project root is three levels up.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

import typer
from pathlib import Path
import requests
import grpc
import os
import sys
# Remove the old sys.path.append, it's now handled by the code at the top.
# sys.path.append(os.path.join(os.path.dirname(__file__), '../core')) 
from protos import dfs_pb2, dfs_pb2_grpc, namenode_pb2, namenode_pb2_grpc

app = typer.Typer()
NAMENODE_URL = "http://localhost:8000"
NAMENODE_GRPC = "localhost:50052"
DATANODE_GRPC = "localhost:50051"

# --- Utilidades ---
def split_into_blocks(data: bytes, block_size=64*1024*1024):
    return [data[i:i+block_size] for i in range(0, len(data), block_size)]

def get_datanode_stub(address):
    channel = grpc.insecure_channel(address)
    return dfs_pb2_grpc.DataNodeServiceStub(channel)

def get_namenode_stub(address):
    channel = grpc.insecure_channel(address)
    return namenode_pb2_grpc.NameNodeServiceStub(channel)

# --- Comandos CLI ---
@app.command()
def put(file_path: Path):
    with open(file_path, "rb") as f:
        data = f.read()
    stub = get_namenode_stub(NAMENODE_GRPC)
    # Solicitar asignación de bloques al NameNode vía gRPC
    resp = stub.AllocateBlocks(namenode_pb2.AllocateBlocksRequest(file_size=len(data)))
    blocks = resp.block_ids
    # Enviar bloques a DataNodes
    for i, block in enumerate(split_into_blocks(data)):
        block_id = blocks[i]
        loc_resp = stub.GetBlockLocations(namenode_pb2.BlockLocationRequest(block_id=block_id))
        locations = loc_resp.node_ids
        if locations:
            stub_dn = get_datanode_stub(DATANODE_GRPC) # Aquí deberías mapear node_id a dirección real
            stub_dn.StoreBlock(dfs_pb2.BlockRequest(content=block, block_id=block_id, replica_nodes=locations))
            print(f"Bloque {block_id} enviado a {locations}")
    # Registrar archivo en NameNode
    stub.AddFile(namenode_pb2.AddFileRequest(file_path=str(file_path), block_ids=blocks))
    print(f"Archivo {file_path} registrado en NameNode")

@app.command()
def ls(dir_path: str = "/"):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.ListFiles(namenode_pb2.ListFilesRequest(dir_path=dir_path))
    for item in resp.items:
        print(item)

@app.command()
def get(file_path: str, output_path: Path = None):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.GetFileBlocks(namenode_pb2.FileBlocksRequest(file_path=file_path))
    blocks = resp.block_ids
    print(f"Bloques del archivo {file_path}: {blocks}")
    data = b""
    for block_id in blocks:
        loc_resp = stub.GetBlockLocations(namenode_pb2.BlockLocationRequest(block_id=block_id))
        locations = loc_resp.node_ids
        if locations:
            stub_dn = get_datanode_stub(DATANODE_GRPC)
            block_data = stub_dn.GetBlock(dfs_pb2.BlockRequest(block_id=block_id, content=b"", replica_nodes=[]))
            data += block_data.content
            print(f"Descargado bloque {block_id} desde {locations}")
    if output_path:
        with open(output_path, "wb") as f:
            f.write(data)
        print(f"Archivo reconstruido en {output_path}")
    else:
        print(f"Archivo reconstruido en memoria, tamaño: {len(data)} bytes")

@app.command()
def mkdir(dir_path: str):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.Mkdir(namenode_pb2.MkdirRequest(dir_path=dir_path))
    print("Directorio creado" if resp.success else "Error al crear directorio")

@app.command()
def rmdir(dir_path: str):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.Rmdir(namenode_pb2.RmdirRequest(dir_path=dir_path))
    print("Directorio eliminado" if resp.success else "Error al eliminar directorio")

@app.command()
def rm(file_path: str):
    stub = get_namenode_stub(NAMENODE_GRPC)
    resp = stub.RemoveFile(namenode_pb2.RemoveFileRequest(file_path=file_path))
    print("Archivo eliminado" if resp.success else "Error al eliminar archivo")

if __name__ == "__main__":
    app()