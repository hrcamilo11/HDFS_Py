# Uso de Scripts

Este directorio contiene varios scripts de utilidad para la gestión del Sistema de Archivos Distribuido (DFS).

- `fix_proto_imports.py`: Este script se utiliza para corregir las rutas de importación en los archivos protobuf de Python generados. A menudo es necesario cuando los archivos protobuf se generan en una estructura de directorio diferente a la de su uso.
- `run_datanodes.py`: Este script es responsable de iniciar múltiples instancias de DataNode. Se utiliza para simular un entorno distribuido para pruebas y desarrollo.
- `stop_datanodes.py`: Este script se utiliza para detener de forma segura todas las instancias de DataNode en ejecución iniciadas por `run_datanodes.py`.