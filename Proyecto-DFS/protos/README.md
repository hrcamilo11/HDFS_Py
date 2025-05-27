# Definiciones de Protocol Buffers

Este directorio contiene los archivos de definición de Protocol Buffer (`.proto`) utilizados para la comunicación entre servicios dentro del Sistema de Archivos Distribuido (DFS).

- `dfs.proto`: Define las estructuras de servicio y mensaje para operaciones generales del DFS, incluyendo la gestión de archivos y bloques.
- `namenode.proto`: Define las estructuras de servicio y mensaje específicamente para operaciones del NameNode, como el registro de DataNodes, el manejo de latidos y la gestión de metadatos.

Estos archivos `.proto` se compilan en código Python (archivos `_pb2.py` y `_pb2_grpc.py`) que permite que diferentes componentes del DFS se comuniquen eficientemente usando gRPC.