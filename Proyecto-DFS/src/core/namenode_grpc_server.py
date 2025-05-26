import sys
import os
# Calculate the project root directory (Proyecto-DFS)
# This script is in .../Proyecto-DFS/src/core/
# Project root is three levels up.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

import grpc
from concurrent import futures
import time
from src.core.namenode import NameNode
import protos.namenode_pb2_grpc as namenode_pb2_grpc
import protos.namenode_pb2 as namenode_pb2
import protos.dfs_pb2 as dfs_pb2
import threading

class NameNodeService(namenode_pb2_grpc.NameNodeServiceServicer):
    def __init__(self):
        self.namenode = NameNode()
        threading.Thread(target=self.rereplication_loop, daemon=True).start()

    def rereplication_loop(self):
        while True:
            self.namenode.check_and_rereplicate()
            time.sleep(10)

    def RegisterDataNode(self, request, context):
        self.namenode.register_datanode(request.node_id)
        return namenode_pb2.RegisterResponse(success=True)

    def Heartbeat(self, request, context):
        self.namenode.heartbeat(request.node_id)
        return namenode_pb2.HeartbeatResponse(success=True)

    def AllocateBlocks(self, request, context):
        block_ids = self.namenode.allocate_blocks(request.file_size)
        return namenode_pb2.AllocateBlocksResponse(block_ids=block_ids)

    def GetBlockLocations(self, request, context):
        node_ids = self.namenode.get_block_locations(request.block_id)
        return namenode_pb2.BlockLocationResponse(node_ids=node_ids)

    def GetFileBlocks(self, request, context):
        block_ids = self.namenode.get_file_blocks(request.file_path)
        return namenode_pb2.FileBlocksResponse(block_ids=block_ids)

    def AddFile(self, request, context):
        self.namenode.add_file(request.file_path, list(request.block_ids))
        return namenode_pb2.AddFileResponse(success=True, file_path=request.file_path)

    def ListFiles(self, request, context):
        items = self.namenode.ls(request.dir_path)
        return namenode_pb2.ListFilesResponse(items=items)

    def Mkdir(self, request, context):
        self.namenode.mkdir(request.dir_path)
        return namenode_pb2.MkdirResponse(success=True)

    def Rmdir(self, request, context):
        self.namenode.rmdir(request.dir_path)
        return namenode_pb2.RmdirResponse(success=True)

    def RemoveFile(self, request, context):
        self.namenode.rm(request.file_path)
        return namenode_pb2.RemoveFileResponse(success=True)

    def GetFileContent(self, request, context):
        content = self.namenode.get_file_content(request.file_path)
        return namenode_pb2.FileContentResponse(content=content)

    def Move(self, request, context):
        success, message = self.namenode.mv(request.source_path, request.destination_path)
        return namenode_pb2.MoveResponse(success=success, message=message)

    def Login(self, request, context):
        success, message = self.namenode.login(request.username)
        return namenode_pb2.LoginResponse(success=success, message=message)

    def Logout(self, request, context):
        success, message = self.namenode.logout(request.username)
        return namenode_pb2.LogoutResponse(success=success, message=message)

def serve():
    port = '50052'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    namenode_pb2_grpc.add_NameNodeServiceServicer_to_server(NameNodeService(), server)
    server.add_insecure_port('[::]:' + port)
    print(f'NameNode gRPC server iniciado en puerto {port}')
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()