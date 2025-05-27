# Proyecto-DFS

Este es un proyecto de Sistema de Archivos Distribuido (DFS) que simula un entorno similar a HDFS.

## Estructura del Proyecto

El proyecto está organizado en las siguientes carpetas y archivos principales:

- `config/`: Contiene archivos de configuración para el DFS, como variables de entorno y ajustes operacionales.
- `descargas/`: Directorio destinado a almacenar archivos descargados del DFS.
- `protos/`: Contiene las definiciones de Protocol Buffers (`.proto`) utilizadas para la comunicación entre los servicios del DFS.
- `scripts/`: Incluye varios scripts de utilidad para la gestión y operación del DFS, como el inicio y la detención de DataNodes.
- `src/`: Contiene el código fuente principal del proyecto.
  - `client/`: Implementa la interfaz de línea de comandos (CLI) para interactuar con el DFS.
  - `core/`: Contiene los componentes centrales del DFS, como las implementaciones de NameNode y DataNode.
- `run_cli.py`: Script principal para ejecutar la interfaz de línea de comandos del DFS.
- `start_dfs.py`: Script para iniciar el sistema de archivos distribuido.
- `requirements.txt`: Lista las dependencias de Python necesarias para el proyecto.

## Scripts Principales

- `run_cli.py`: Permite a los usuarios interactuar con el DFS a través de comandos como `upload`, `download`, `ls`, `rm` y `mkdir`.
- `start_dfs.py`: Inicia los componentes necesarios del DFS, como el NameNode y los DataNodes.
- `scripts/run_datanodes.py`: Inicia múltiples instancias de DataNode para simular un entorno distribuido.
- `scripts/stop_datanodes.py`: Detiene las instancias de DataNode en ejecución.
- `scripts/fix_proto_imports.py`: Utilidad para corregir rutas de importación en archivos protobuf generados.