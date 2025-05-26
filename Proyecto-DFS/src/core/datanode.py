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
        # The first node in replica_nodes is the one that received the initial StoreBlock from the client.
        # We need to identify which node *this* current DataNodeServicer instance is.
        # This information isn't directly available in DataNodeServicer, so we'll assume this servicer
        # is the one the client *thought* it was sending to, which is typically the first in the list.
        # Or, more robustly, the NameNode should perhaps tell each DataNode *who* it is in the replication chain.
        # For now, let's assume this DataNode is request.replica_nodes[0] if the list is not empty.
        # The actual replication targets are the *other* nodes in the list.

        # Determine the current node's ID to avoid self-replication if it's in the list beyond the first position
        # This is tricky without knowing the current DataNode's ID within the servicer.
        # Let's assume the client sends the block to the *first* node in replica_nodes, and that node then replicates to others.
        
        # The client sends replica_nodes like ['datanode1', 'datanode2', 'datanode3']
        # If this is datanode1, it should replicate to datanode2 and datanode3.
        # The request.replica_nodes passed to *this* StoreBlock call (from the client) contains all intended replicas.
        # When this datanode (e.g. datanode1) calls StoreBlock on another (e.g. datanode2 for replication),
        # it should pass an empty replica_nodes list, or a list that excludes datanode2 and datanode1, to prevent loops or further replication by datanode2 based on the original list.
        # For simplicity in this step, the recursive StoreBlock calls will pass an empty replica_nodes list.

        # Identify which node this is. We need the node_id of the current datanode.
        # This is not directly available in the servicer. A simple approach is to assume this datanode is the first one
        # in the list if the call comes from the client. If it's a replication call, replica_nodes should be empty.

        if request.replica_nodes and len(request.replica_nodes) > 1:
            # This assumes the current node is the first in the list and needs to replicate to others.
            current_node_id_from_list = request.replica_nodes[0]
            nodes_to_replicate_to = request.replica_nodes[1:]
            
            for replica_node_id in nodes_to_replicate_to:
                try:
                    # Derive address from replica_node_id (e.g., "datanode2" -> "localhost:50054")
                    datanode_index = int(replica_node_id.replace("datanode", ""))
                    datanode_port = 50052 + datanode_index # Base port 50053 for datanode1
                    replica_address = f"localhost:{datanode_port}"
                    
                    channel = grpc.insecure_channel(replica_address)
                    stub = dfs_pb2_grpc.DataNodeServiceStub(channel)
                    # When replicating, send an empty replica_nodes list to prevent further replication by the next node based on the original list.
                    print(f"Replicando bloque {request.block_id} desde (asumido) {current_node_id_from_list} a {replica_node_id} ({replica_address})")
                    stub.StoreBlock(dfs_pb2.BlockRequest(content=request.content, block_id=request.block_id, replica_nodes=[]))
                    print(f"Bloque {request.block_id} replicado exitosamente a {replica_node_id}")
                except ValueError:
                    print(f"Error: No se pudo determinar la dirección para replicar a DataNode ID '{replica_node_id}'.")
                except grpc.RpcError as e:
                    print(f"Error replicando bloque {request.block_id} a {replica_node_id}: {e}")
                except Exception as e:
                    print(f"Error inesperado replicando bloque {request.block_id} a {replica_node_id}: {e}")
        return dfs_pb2.StoreResponse(success=True, message="Bloque almacenado y replicado")

    def ReplicateBlock(self, request, context):
        # Simulación de replicación (en un sistema real, enviaría el bloque a otros DataNodes)
        return self.StoreBlock(request, context)

    def GetBlock(self, request, context):
        block_path = os.path.join(self.storage_dir, request.block_id)
        if not os.path.exists(block_path):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Block {request.block_id} not found.")
            return dfs_pb2.BlockDataResponse(content=b"", success=False, message=f"Block {request.block_id} not found.")
        try:
            with open(block_path, "rb") as f:
                content = f.read()
            return dfs_pb2.BlockDataResponse(content=content, success=True, message=f"Block {request.block_id} retrieved successfully.")
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error reading block {request.block_id}: {str(e)}")
            return dfs_pb2.BlockDataResponse(content=b"", success=False, message=f"Error reading block {request.block_id}: {str(e)}")

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
        
        # Registrar el DataNode con el NameNode
        self.register_with_namenode()
        
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        self.server.wait_for_termination()

    def register_with_namenode(self):
        from protos import namenode_pb2
        from protos import namenode_pb2_grpc
        try:
            channel = grpc.insecure_channel(self.namenode_host)
            stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
            stub.RegisterDataNode(namenode_pb2.RegisterRequest(node_id=self.node_id))
            print(f"DataNode {self.node_id} registrado con el NameNode.")
        except Exception as e:
            print(f"Error registrando DataNode {self.node_id} con NameNode: {e}")

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