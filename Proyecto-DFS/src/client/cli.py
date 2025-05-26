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

import cmd
import time

app = typer.Typer()
NAMENODE_URL = "http://localhost:8000"
NAMENODE_GRPC = "localhost:50052"

# Global variable to store the logged-in username
LOGGED_IN_USER = None
# DATANODE_GRPC = "localhost:50051" # This will be derived dynamically

# --- DFS Path Management ---
_current_dfs_path_components: list[str] = [] # Represents root path /

def get_current_dfs_display_path() -> str:
    """Returns the string representation of the current DFS path, e.g., /foo/bar or /."""
    if not _current_dfs_path_components:
        return "/"
    return "/" + "/".join(_current_dfs_path_components)

def _normalize_path_to_components(path_str: str, base_components: list[str]) -> list[str]:
    """
    Normalizes a given path string relative to base_components.
    Handles absolute paths (starting with '/') and relative paths.
    Resolves '.' and '..'.
    Returns a new list of path components.
    """
    # Standardize to use '/' as separator for internal processing
    # and remove leading/trailing whitespace
    processed_path_str = path_str.strip().replace('\\', '/')
    
    # Determine if the path_str is absolute or relative
    if processed_path_str.startswith("/"):
        # Absolute path: start from root
        current_components = []
        # Remove leading '/' for splitting
        path_segments_str = processed_path_str[1:]
    else:
        # Relative path: start from base_components
        current_components = list(base_components) # Make a copy
        path_segments_str = processed_path_str

    # Split into segments, filter out empty strings resulting from multiple slashes (e.g., foo//bar)
    # or if path_segments_str itself is empty (e.g. from "/" or relative "")
    segments = [segment for segment in path_segments_str.split('/') if segment]

    # Process segments
    for segment in segments:
        if segment == ".":
            # Current directory, do nothing
            pass
        elif segment == "..":
            # Parent directory
            if current_components:
                current_components.pop()
        else:
            # Regular directory/file name
            current_components.append(segment)
    
    return current_components

# --- Utilidades ---
def split_into_blocks(data: bytes, block_size=64*1024*1024):
    return [data[i:i+block_size] for i in range(0, len(data), block_size)]

def get_datanode_stub(address):
    channel = grpc.insecure_channel(address)
    return dfs_pb2_grpc.DataNodeServiceStub(channel)

def get_namenode_stub(address):
    channel = grpc.insecure_channel(address)
    return namenode_pb2_grpc.NameNodeServiceStub(channel)

@app.command()
def login(username: str = typer.Option(..., prompt=True, help="Username for DFS login")):
    """Logs in a user to the DFS."""
    global LOGGED_IN_USER
    try:
        stub = get_namenode_stub(NAMENODE_GRPC)
        response = stub.Login(namenode_pb2.LoginRequest(username=username))
        if response.success:
            LOGGED_IN_USER = username
            print(f"Login exitoso para el usuario: {username}")
            print(f"Mensaje del NameNode: {response.message}")
        else:
            print(f"Error de login: {response.message}")
    except grpc.RpcError as e:
        print(f"Error de conexión con el NameNode para login: {e.details}")
    except Exception as e:
        print(f"Ocurrió un error inesperado durante el login: {e}")

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
    # El nombre del archivo en DFS será el basename del archivo local.
    # La ruta DFS de destino se construye a partir de la ruta actual + nombre del archivo.
    file_name_in_dfs = os.path.basename(file_path)
    # Construir la ruta DFS completa para el archivo
    # _current_dfs_path_components ya está normalizado y no tiene barras iniciales/finales
    if _current_dfs_path_components:
        dfs_destination_path = "/" + "/".join(_current_dfs_path_components) + "/" + file_name_in_dfs
    else: # Estamos en la raíz
        dfs_destination_path = "/" + file_name_in_dfs
    
    # Asegurarse de que no haya barras dobles si _current_dfs_path_components estaba vacío y file_name_in_dfs comienza con /
    # (lo cual no debería pasar con os.path.basename, pero por si acaso)
    dfs_destination_path = dfs_destination_path.replace('//', '/')

    add_file_response = stub.AddFile(namenode_pb2.AddFileRequest(file_path=dfs_destination_path, block_ids=blocks))
    if add_file_response.success:
        print(f"Archivo {file_path} registrado en NameNode en la ruta DFS: {add_file_response.file_path}")
    else:
        print(f"Error al registrar el archivo {dfs_destination_path} en NameNode.")

@app.command()
def ls(dir_path: str = "."):
    """Lista archivos y directorios en la ruta DFS especificada (relativa o absoluta)."""
    global _current_dfs_path_components
    try:
        # Normalizar la ruta de entrada con respecto a la ruta actual
        target_components = _normalize_path_to_components(dir_path, _current_dfs_path_components)
        # Construir la ruta DFS completa para la solicitud gRPC
        dfs_target_path = "/" + "/".join(target_components) if target_components else "/"

        stub = get_namenode_stub(NAMENODE_GRPC)
        resp = stub.ListFiles(namenode_pb2.ListFilesRequest(dir_path=dfs_target_path))
        if resp.items:
            for item in resp.items:
                print(item)
        else:
            # Podríamos querer distinguir entre un directorio vacío y un error/directorio no encontrado.
            # Por ahora, si no hay items, no imprimimos nada o un mensaje de "vacío".
            # Esto depende de cómo el NameNode maneje ListFiles para rutas no existentes.
            # Asumiendo que el NameNode devuelve una lista vacía para directorios vacíos o no existentes.
            print(f"(Directorio vacío o no encontrado: {dfs_target_path})") 
    except Exception as e:
        print(f"Error al listar directorio '{dir_path}': {e}")

@app.command()
def get(file_path: str, output_path: Path = None):
    """Obtiene un archivo del DFS y lo guarda localmente."""
    global _current_dfs_path_components
    try:
        target_components = _normalize_path_to_components(file_path, _current_dfs_path_components)
        dfs_source_path = "/" + "/".join(target_components) if target_components else "/"
        # Si target_components está vacío después de la normalización (ej. 'cd /' y luego 'get .'),
        # no es un archivo válido para obtener.
        if not target_components or dfs_source_path == "/":
            print(f"Error: La ruta '{file_path}' (resuelta a '{dfs_source_path}') no es un archivo válido para 'get'.")
            return

        stub = get_namenode_stub(NAMENODE_GRPC)
        resp = stub.GetFileBlocks(namenode_pb2.FileBlocksRequest(file_path=dfs_source_path))
        blocks = resp.block_ids
        if not blocks:
            print(f"Error: El archivo '{dfs_source_path}' no se encontró en el DFS o está vacío.")
            return
        print(f"Bloques del archivo {dfs_source_path}: {blocks}")
    except Exception as e:
        print(f"Error al procesar la ruta del archivo para 'get' '{file_path}': {e}")
        return
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
    """Crea un directorio en el DFS en la ruta especificada (relativa o absoluta)."""
    global _current_dfs_path_components
    try:
        target_components = _normalize_path_to_components(dir_path, _current_dfs_path_components)
        # Asegurarse de que no estamos intentando crear la raíz o un path vacío
        if not target_components:
            print(f"Error: La ruta '{dir_path}' (resuelta a la raíz '/') no es válida para 'mkdir'.")
            return
        dfs_target_path = "/" + "/".join(target_components)

        stub = get_namenode_stub(NAMENODE_GRPC)
        resp = stub.Mkdir(namenode_pb2.MkdirRequest(dir_path=dfs_target_path))
        if resp.success:
            print(f"Directorio '{dfs_target_path}' creado exitosamente.")
        else:
            # El NameNode debería idealmente devolver un mensaje de error más específico.
            print(f"Error al crear directorio '{dfs_target_path}'.")
    except Exception as e:
        print(f"Error al procesar la ruta para 'mkdir' '{dir_path}': {e}")

@app.command()
def rmdir(dir_path: str):
    """Elimina un directorio vacío del DFS en la ruta especificada (relativa o absoluta)."""
    global _current_dfs_path_components
    try:
        target_components = _normalize_path_to_components(dir_path, _current_dfs_path_components)
        if not target_components:
            print(f"Error: La ruta '{dir_path}' (resuelta a la raíz '/') no es válida para 'rmdir'. No se puede eliminar la raíz.")
            return
        dfs_target_path = "/" + "/".join(target_components)

        stub = get_namenode_stub(NAMENODE_GRPC)
        resp = stub.Rmdir(namenode_pb2.RmdirRequest(dir_path=dfs_target_path))
        if resp.success:
            print(f"Directorio '{dfs_target_path}' eliminado exitosamente.")
        else:
            print(f"Error al eliminar directorio '{dfs_target_path}'. Puede que no exista o no esté vacío.")
    except Exception as e:
        print(f"Error al procesar la ruta para 'rmdir' '{dir_path}': {e}")

@app.command()
def rm(file_path: str):
    """Elimina un archivo del DFS en la ruta especificada (relativa o absoluta)."""
    global _current_dfs_path_components
    try:
        target_components = _normalize_path_to_components(file_path, _current_dfs_path_components)
        if not target_components:
            print(f"Error: La ruta '{file_path}' (resuelta a la raíz '/') no es válida para 'rm'. No se puede eliminar la raíz como un archivo.")
            return
        dfs_target_path = "/" + "/".join(target_components)

        stub = get_namenode_stub(NAMENODE_GRPC)
        resp = stub.RemoveFile(namenode_pb2.RemoveFileRequest(file_path=dfs_target_path))
        if resp.success:
            print(f"Archivo '{dfs_target_path}' eliminado exitosamente.")
        else:
            print(f"Error al eliminar el archivo '{dfs_target_path}'. Puede que no exista o sea un directorio.")
    except Exception as e:
        print(f"Error al procesar la ruta para 'rm' '{file_path}': {e}")

@app.command()
def cd(dir_path: str):
    """Cambia el directorio DFS actual."""
    global _current_dfs_path_components
    try:
        # Para `cd`, no necesitamos verificar si el directorio existe en el NameNode,
        # simplemente actualizamos la ruta del lado del cliente.
        # La validación de la ruta ocurrirá cuando otros comandos (ls, put, get, mkdir, rmdir, rm)
        # intenten usar la ruta completa.
        new_components = _normalize_path_to_components(dir_path, _current_dfs_path_components)
        _current_dfs_path_components = new_components
        print(f"Directorio actual: {get_current_dfs_display_path()}")
    except Exception as e:
        print(f"Error al cambiar de directorio: {e}")

@app.command()
def mv(source_path: str, destination_path: str):
    """Mueve un archivo o directorio (source_path) a una nueva ubicación (destination_path) en el DFS."""
    global _current_dfs_path_components
    try:
        # Normalizar la ruta de origen con respecto a la ruta actual
        source_components = _normalize_path_to_components(source_path, _current_dfs_path_components)
        dfs_source_path = "/" + "/".join(source_components) if source_components else "/"
        if not source_components or dfs_source_path == "/":
            print(f"Error: La ruta de origen '{source_path}' (resuelta a '{dfs_source_path}') no es válida para 'mv'.")
            return

        # Normalizar la ruta de destino con respecto a la ruta actual
        dest_components = _normalize_path_to_components(destination_path, _current_dfs_path_components)
        # Si dest_components está vacío, significa que el destino es la raíz, lo cual es manejado por el NameNode.
        # Si destination_path era solo "/", dest_components será [].
        # Si destination_path era "." y estábamos en la raíz, dest_components será [].
        # Si destination_path era ".." y estábamos en la raíz, dest_components será [].
        # El NameNode _canonical_dfs_path se encargará de convertir esto a "/" si es necesario.
        dfs_destination_path = "/" + "/".join(dest_components) if dest_components else "/"
        
        # No permitir mover a sí mismo (simplificación, el NameNode podría tener una lógica más robusta)
        if dfs_source_path == dfs_destination_path:
            print(f"Error: La ruta de origen y destino son la misma ('{dfs_source_path}').")
            return

        stub = get_namenode_stub(NAMENODE_GRPC)
        resp = stub.Move(namenode_pb2.MoveRequest(source_path=dfs_source_path, destination_path=dfs_destination_path))
        
        if resp.success:
            print(f"'{dfs_source_path}' movido exitosamente a '{resp.message}'.") # resp.message contendrá la ruta final
        else:
            print(f"Error al mover '{dfs_source_path}' a '{dfs_destination_path}': {resp.message}")
            
    except grpc.RpcError as e:
        # Capturar errores específicos de gRPC para dar mensajes más claros
        status_code = e.code()
        details = e.details()
        if status_code == grpc.StatusCode.UNAVAILABLE:
            print(f"Error de conexión: No se pudo conectar al NameNode en {NAMENODE_GRPC}. Detalles: {details}")
        elif status_code == grpc.StatusCode.NOT_FOUND:
            print(f"Error: El NameNode no encontró el método 'Move'. ¿Está el servidor actualizado y el .proto compilado? Detalles: {details}")
        else:
            print(f"Error de RPC al mover: {status_code} - {details}")
    except Exception as e:
        print(f"Error al procesar el comando 'mv' para '{source_path}' -> '{destination_path}': {e}")

class DFSCLI(cmd.Cmd):
    intro = 'Welcome to the DFS shell. Type help or ? to list commands.\n'
    prompt = 'DFS-CLI:/ > '

    def do_ls(self, arg):
        """List files and directories in the specified DFS path.
        Usage: ls [path]
        """
        try:
            if arg:
                app(['ls', arg])
            else:
                app(['ls'])
        except SystemExit:
            pass # typer exits after command, we don't want that in interactive mode

    def do_cd(self, arg):
        """Change the current DFS directory.
        Usage: cd <path>
        """
        try:
            app(['cd', arg])
            self.prompt = f"DFS-CLI:{get_current_dfs_display_path()}> "
        except SystemExit:
            pass

    def do_put(self, arg):
        """Upload a local file to the DFS.
        Usage: put <local_file_path>
        """
        try:
            app(['put', arg])
        except SystemExit:
            pass

    def do_get(self, arg):
        """Download a file from the DFS to a local path.
        Usage: get <dfs_file_path> [output_local_path]
        """
        try:
            args = arg.split(maxsplit=1)
            if len(args) == 2:
                app(['get', args[0], args[1]])
            elif len(args) == 1:
                app(['get', args[0]])
            else:
                print("Usage: get <dfs_file_path> [output_local_path]")
        except SystemExit:
            pass

    def do_rm(self, arg):
        """Remove a file or directory from the DFS.
        Usage: rm <dfs_path>
        """
        try:
            app(['rm', arg])
        except SystemExit:
            pass

    def do_mkdir(self, arg):
        """Create a directory in the DFS.
        Usage: mkdir <dfs_path>
        """
        try:
            app(['mkdir', arg])
        except SystemExit:
            pass

    def do_mv(self, arg):
        """Move or rename a file or directory in the DFS.
        Usage: mv <source_path> <destination_path>
        """
        try:
            args = arg.split(maxsplit=1)
            if len(args) == 2:
                app(['mv', args[0], args[1]])
            else:
                print("Usage: mv <source_path> <destination_path>")
        except SystemExit:
            pass

    def do_login(self, arg):
        """login <username> - Logs in a user to the DFS.
        If no username is provided, it will prompt for it.
        """
        if arg:
            username = arg
        else:
            username = typer.prompt("Username")
        app.run(login, args=[username])
        self.update_prompt()

    def do_logout(self, arg):
        """logout - Logs out the current user from the DFS."""
        app.run(logout)
        self.update_prompt()

    def do_whoami(self, arg):
        """whoami - Shows the currently logged-in user."""
        app.run(whoami)

    def do_ls(self, arg):
        """List files and directories in the specified DFS path.
        Usage: ls [path]
        """
        try:
            if arg:
                app(['ls', arg])
            else:
                app(['ls'])
        except SystemExit:
            pass # typer exits after command, we don't want that in interactive mode

    def do_cd(self, arg):
        """Change the current DFS directory.
        Usage: cd <path>
        """
        try:
            app(['cd', arg])
            self.prompt = f"DFS-CLI:{get_current_dfs_display_path()}> "
        except SystemExit:
            pass

    def do_put(self, arg):
        """Upload a local file to the DFS.
        Usage: put <local_file_path>
        """
        try:
            app(['put', arg])
        except SystemExit:
            pass

    def do_get(self, arg):
        """Download a file from the DFS to a local path.
        Usage: get <dfs_file_path> [output_local_path]
        """
        try:
            args = arg.split(maxsplit=1)
            if len(args) == 2:
                app(['get', args[0], args[1]])
            elif len(args) == 1:
                app(['get', args[0]])
            else:
                print("Usage: get <dfs_file_path> [output_local_path]")
        except SystemExit:
            pass

    def do_rm(self, arg):
        """Remove a file or directory from the DFS.
        Usage: rm <dfs_path>
        """
        try:
            app(['rm', arg])
        except SystemExit:
            pass

    def do_mkdir(self, arg):
        """Create a directory in the DFS.
        Usage: mkdir <dfs_path>
        """
        try:
            app(['mkdir', arg])
        except SystemExit:
            pass

    def do_mv(self, arg):
        """Move or rename a file or directory in the DFS.
        Usage: mv <source_path> <destination_path>
        """
        try:
            args = arg.split(maxsplit=1)
            if len(args) == 2:
                app(['mv', args[0], args[1]])
            else:
                print("Usage: mv <source_path> <destination_path>")
        except SystemExit:
            pass

    def do_exit(self, arg):
        """exit - Exits the DFS CLI."""
        return True

    def do_quit(self, arg):
        """quit - Exits the DFS CLI."""
        return True

    def preloop(self):
        """Called once before the command loop starts."""
        print("Bienvenido al DFS CLI. Por favor, inicie sesión con el comando 'login'.")
        # Optionally, prompt for login immediately
        # self.do_login('') # This would force a login prompt at startup

if __name__ == "__main__":
    cli = DFSCLI()
    cli.cmdloop()