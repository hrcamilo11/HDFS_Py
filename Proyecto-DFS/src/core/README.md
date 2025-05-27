# Explicación del Módulo Core

Este directorio contiene los componentes centrales del Sistema de Archivos Distribuido (DFS).

- `datanode.py`: Implementa la funcionalidad del DataNode, responsable de almacenar los bloques de datos reales y de atender las solicitudes de lectura/escritura de los clientes.
- `namenode.py`: Implementa la funcionalidad del NameNode, responsable de gestionar el espacio de nombres del sistema de archivos, los metadatos y las ubicaciones de los bloques.
- `namenode_grpc_server.py`: Configura y ejecuta el servidor gRPC para el NameNode, manejando las llamadas RPC entrantes de los DataNodes y los clientes.