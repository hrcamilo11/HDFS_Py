import sys
import os
# Calculate the project root directory (Proyecto-DFS)
# This script is in .../Proyecto-DFS/src/core/
# Project root is three levels up.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

import grpc
import threading
import time
import os
from concurrent import futures
from protos import dfs_pb2_grpc, dfs_pb2
# Ensure PROJECT_ROOT is in sys.path for 'from protos import ...' to work
# This is already handled at the top of the file.

class DataNodeServicer(dfs_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, storage_dir):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

    def StoreBlock(self, request, context):
        block_path = os.path.join(self.storage_dir, request.block_id)
        with open(block_path, "wb") as f:
            f.write(request.content)
        # Replicación a otros DataNodes
        from protos import dfs_pb2
        from protos import dfs_pb2_grpc
        for replica_node in request.replica_nodes[1:]:  # El primero es el actual
            try:
                channel = grpc.insecure_channel(replica_node)
                stub = dfs_pb2_grpc.DataNodeServiceStub(channel)
                stub.StoreBlock(dfs_pb2.BlockRequest(content=request.content, block_id=request.block_id, replica_nodes=[]))
            except Exception as e:
                print(f"Error replicando bloque a {replica_node}: {e}")
        return dfs_pb2.StoreResponse(success=True, message="Bloque almacenado y replicado")

    def ReplicateBlock(self, request, context):
        # Simulación de replicación (en un sistema real, enviaría el bloque a otros DataNodes)
        return self.StoreBlock(request, context)

    def GetBlock(self, request, context):
        import src.core.dfs_pb2 as dfs_pb2
        block_path = os.path.join(self.storage_dir, request.block_id)
        if not os.path.exists(block_path):
            return dfs_pb2.BlockRequest(content=b"", block_id=request.block_id, replica_nodes=[])
        with open(block_path, "rb") as f:
            content = f.read()
        return dfs_pb2.BlockRequest(content=content, block_id=request.block_id, replica_nodes=[])

class DataNode:
    def __init__(self, node_id, namenode_host="localhost:50052", grpc_port=50051, storage_dir="/tmp/default_datanode_storage"):
        self.node_id = node_id
        self.namenode_host = namenode_host
        self.grpc_port = grpc_port
        self.storage_dir = storage_dir # Guardar para referencia si es necesario
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        # Pasar el storage_dir específico al DataNodeServicer
        dfs_pb2_grpc.add_DataNodeServiceServicer_to_server(DataNodeServicer(storage_dir=self.storage_dir), self.server)

    def start(self):
        self.server.add_insecure_port(f"[::]:{self.grpc_port}")
        self.server.start()
        print(f"DataNode {self.node_id} iniciado en puerto {self.grpc_port}")
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        self.server.wait_for_termination()

    def heartbeat_loop(self):
        from protos import namenode_pb2
        from protos import namenode_pb2_grpc
        while True:
            try:
                channel = grpc.insecure_channel(self.namenode_host)
                stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
                stub.Heartbeat(namenode_pb2.HeartbeatRequest(node_id=self.node_id))
                print(f"DataNode {self.node_id} heartbeat enviado al NameNode")
            except Exception as e:
                print(f"Error enviando heartbeat: {e}")
            time.sleep(5)