# Uso de la Interfaz de Línea de Comandos (CLI)

Este directorio contiene la interfaz de línea de comandos (CLI) para interactuar con el Sistema de Archivos Distribuido (DFS).

## Cómo Usar

Para usar la CLI, ejecute `python run_cli.py` desde el directorio raíz del proyecto. La CLI proporciona varios comandos para interactuar con el DFS, tales como:

- `upload <ruta_local> <ruta_dfs>`: Sube un archivo desde `ruta_local` a `ruta_dfs` en el DFS.
- `download <ruta_dfs> <ruta_local>`: Descarga un archivo desde `ruta_dfs` en el DFS a `ruta_local`.
- `ls <ruta_dfs>`: Lista el contenido de un directorio en el DFS.
- `rm <ruta_dfs>`: Elimina un archivo o directorio en el DFS.
- `mkdir <ruta_dfs>`: Crea un nuevo directorio en el DFS.

Para obtener información más detallada sobre cada comando, consulte el mensaje de ayuda de la CLI ejecutando `python run_cli.py --help` o `python run_cli.py <comando> --help`.