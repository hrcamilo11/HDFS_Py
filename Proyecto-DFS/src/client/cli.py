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
# DATANODE_GRPC = "localhost:50051" # This will be derived dynamically

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
            # Choose the first datanode from the list for simplicity for now
            chosen_datanode_id = locations[0]
            try:
                datanode_index = int(chosen_datanode_id.replace("datanode", "")) # Extracts N from "datanodeN"
                datanode_port = 50052 + datanode_index # Base port 50053 for datanode1
                datanode_address = f"localhost:{datanode_port}"
                stub_dn = get_datanode_stub(datanode_address)
                # Pass all locations for potential replication handling by the chosen datanode, though StoreBlock might only use the local one.
                stub_dn.StoreBlock(dfs_pb2.BlockRequest(content=block, block_id=block_id, replica_nodes=locations))
                print(f"Bloque {block_id} enviado a {chosen_datanode_id} ({datanode_address}) para almacenamiento y replicación en {locations}")
            except ValueError:
                print(f"Error: No se pudo determinar la dirección del DataNode desde el ID '{chosen_datanode_id}'. Se omite el envío del bloque {block_id}.")
                continue
            except grpc.RpcError as e:
                print(f"Error al contactar DataNode {chosen_datanode_id} ({datanode_address}): {e}. Se omite el envío del bloque {block_id}.")
                continue
    # Registrar archivo en NameNode
    dfs_file_path = os.path.basename(file_path)
    add_file_response = stub.AddFile(namenode_pb2.AddFileRequest(file_path=dfs_file_path, block_ids=blocks))
    if add_file_response.success:
        print(f"Archivo {file_path} registrado en NameNode en la ruta DFS: {add_file_response.file_path}")
    else:
        print(f"Error al registrar el archivo {dfs_file_path} en NameNode.")

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
            # Try fetching from the first available datanode, then others if it fails
            block_downloaded = False
            for datanode_id_to_try in locations:
                try:
                    datanode_index = int(datanode_id_to_try.replace("datanode", ""))
                    datanode_port = 50052 + datanode_index
                    datanode_address = f"localhost:{datanode_port}"
                    stub_dn = get_datanode_stub(datanode_address)
                    # Pass only block_id in BlockRequest for GetBlock, content and replica_nodes are not needed.
                    block_data_resp = stub_dn.GetBlock(dfs_pb2.BlockRequest(block_id=block_id))
                    if block_data_resp.success:
                        data += block_data_resp.content
                        print(f"Descargado bloque {block_id} desde {datanode_id_to_try} ({datanode_address})")
                        block_downloaded = True
                        break # Successfully downloaded from this datanode
                    else:
                        print(f"Error al obtener bloque {block_id} desde {datanode_id_to_try}: {block_data_resp.message}. Intentando con el siguiente.")
                except ValueError:
                    print(f"Error: No se pudo determinar la dirección del DataNode desde el ID '{datanode_id_to_try}'. Intentando con el siguiente.")
                except grpc.RpcError as e:
                    print(f"Error al descargar bloque {block_id} desde {datanode_id_to_try} ({datanode_address}): {e}. Intentando con el siguiente.")
            if not block_downloaded:
                print(f"Error: No se pudo descargar el bloque {block_id} desde ninguna de las ubicaciones: {locations}")
                # Handle missing block case - perhaps raise an error or return partial data
                break # Stop trying to reconstruct if a block is missing
        else:
            print(f"Error: No se encontraron ubicaciones para el bloque {block_id}")
            # Handle missing block case
            break # Stop trying to reconstruct if a block is missing
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