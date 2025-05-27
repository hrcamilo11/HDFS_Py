import sys
import os
# Calculate the project root directory (Proyecto-DFS)
# This script is in .../Proyecto-DFS/src/client/
# Project root is three levels up.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from pathlib import Path
from typing import Optional
import requests
import grpc
import os
import sys
# Remove the old sys.path.append, it's now handled by the code at the top.
# sys.path.append(os.path.join(os.path.dirname(__file__), '../core')) 
from protos import dfs_pb2, dfs_pb2_grpc, namenode_pb2, namenode_pb2_grpc

import cmd
import time
import logging

NAMENODE_URL = "http://localhost:50050"
NAMENODE_GRPC = "localhost:50050"

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

class DFSCLI(cmd.Cmd):
    intro = 'Welcome to the DFS shell. Type help or ? to list commands.\n'
    prompt = 'DFS-CLI:/ > '

    def __init__(self):
        super().__init__()
        self._current_dfs_path_components = []
        self._current_user = None
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - DFSCLI - %(levelname)s - %(message)s')
        logging.info("DFSCLI initialized.")

    def get_current_dfs_display_path(self) -> str:
        """Returns the string representation of the current DFS path, e.g., /foo/bar or /."""
        if not self._current_dfs_path_components:
            return "/"
        return "/" + "/".join(self._current_dfs_path_components)

    def _normalize_path_to_components(self, path_str: str, base_components: list[str]) -> list[str]:
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

    def login(self, username: str) -> tuple[bool, str]:
        """Logs in a user to the DFS."""
        logging.info(f"Attempting to login user: '{username}'. Current user: '{self._current_user}'.")
        if self._current_user:
            if self._current_user == username:
                logging.info(f"User '{username}' is already logged in.")
                return True, f"Ya has iniciado sesión como '{username}'."
            else:
                logging.info(f"User '{self._current_user}' is logged in. Prompting to switch to '{username}'.")
                confirm_str = input(f"Ya has iniciado sesión como '{self._current_user}'. ¿Deseas cambiar a '{username}'? (y/n): ").lower()
                if confirm_str != 'y':
                    logging.info("User declined to switch accounts.")
                    return False, "Cambio de usuario cancelado."
                logging.info(f"User agreed to switch from '{self._current_user}' to '{username}'. Proceeding with new login.")

        if not username:
            logging.warning("Login attempt with empty username.")
            return False, "Username cannot be empty."

        try:
            logging.info(f"Connecting to NameNode at {NAMENODE_GRPC} for login as '{username}'.")
            stub = get_namenode_stub(NAMENODE_GRPC)
            response = stub.Login(namenode_pb2.LoginRequest(username=username))
            logging.info(f"NameNode login response for '{username}': success={response.success}, message='{response.message}'.")
            if response.success:
                self._current_user = username
                # Reset path to user's root upon login
                self._current_dfs_path_components = [] 
                self.prompt = f"DFS-CLI:{self.get_current_dfs_display_path()} {self._current_user}$ "
                logging.info(f"User '{username}' logged in successfully. Path reset. Prompt updated. Message: {response.message}")
                return True, f"Login exitoso para el usuario: {username}. Mensaje del NameNode: {response.message}"
            else:
                self._current_user = None # Ensure user is None if login fails
                logging.warning(f"Login failed for '{username}': {response.message}")
                return False, f"Error de login: {response.message}"
        except grpc.RpcError as e:
            self._current_user = None # Ensure user is None on error
            logging.error(f"gRPC error during login for '{username}': {e.details() if hasattr(e, 'details') else e}", exc_info=True)
            return False, f"Error de conexión con el NameNode para login: {e.details() if hasattr(e, 'details') else e}"
        except Exception as e:
            self._current_user = None # Ensure user is None on error
            logging.error(f"Unexpected error during login for '{username}': {e}", exc_info=True)
            return False, f"Ocurrió un error inesperado durante el login: {e}"

    def logout(self):
        """Logs out the current user from the DFS."""
        logging.info(f"Logout attempt for user: '{self._current_user}'.")
        if not self._current_user:
            logging.warning("Logout attempt when no user is logged in.")
            print("No hay ningún usuario logueado.")
            return

        try:
            logging.info(f"Connecting to NameNode at {NAMENODE_GRPC} for logout of '{self._current_user}'.")
            stub = get_namenode_stub(NAMENODE_GRPC)
            resp = stub.Logout(namenode_pb2.LogoutRequest(username=self._current_user))
            logging.info(f"NameNode logout response for '{self._current_user}': success={resp.success}, message='{resp.message}'.")
            if resp.success:
                print(f"Logout exitoso para el usuario: {self._current_user}")
                print(f"Mensaje del NameNode: {resp.message}")
                logging.info(f"User '{self._current_user}' logged out successfully from client side. Message: {resp.message}")
                self._current_user = None
                self._current_dfs_path_components = [] # Reset path to root
                self.prompt = f"DFS-CLI:{self.get_current_dfs_display_path()} > " # Update prompt
            else:
                logging.warning(f"Logout failed for '{self._current_user}': {resp.message}")
                print(f"Error de logout: {resp.message}")
        except grpc.RpcError as e:
            logging.error(f"gRPC error during logout for '{self._current_user}': {e.details() if hasattr(e, 'details') else e}", exc_info=True)
            print(f"Error de conexión al NameNode: {e.details() if hasattr(e, 'details') else e}")
        except Exception as e:
            logging.error(f"Unexpected error during logout for '{self._current_user}': {e}", exc_info=True)
            print(f"Error inesperado durante el logout: {e}")

    def whoami(self):
        """Shows the currently logged-in user."""
        if self._current_user:
            print(f"Usuario actual: {self._current_user}")
        else:
            print("No hay ningún usuario logueado.")

    def put(self, file_path: Path):
        with open(file_path, "rb") as f:
            data = f.read()
        if not self._current_user:
            print("Error: No hay usuario logueado. Por favor, inicie sesión para subir archivos.")
            return

        stub = get_namenode_stub(NAMENODE_GRPC)
        resp = stub.AllocateBlocks(namenode_pb2.AllocateBlocksRequest(file_size=len(data), username=self._current_user))
        blocks = resp.block_ids
        for i, block in enumerate(split_into_blocks(data)):
            block_id = blocks[i]
            loc_resp = stub.GetBlockLocations(namenode_pb2.BlockLocationRequest(block_id=block_id))
            locations = loc_resp.node_ids
            if locations:
                chosen_datanode_id = locations[0]
                try:
                    datanode_index = int(chosen_datanode_id.replace("datanode", ""))
                    datanode_port = 50050 + datanode_index
                    datanode_address = f"localhost:{datanode_port}"
                    stub_dn = get_datanode_stub(datanode_address)
                    stub_dn.StoreBlock(dfs_pb2.BlockRequest(content=block, block_id=block_id, replica_nodes=locations))
                    print(f"Bloque {block_id} enviado a {chosen_datanode_id} ({datanode_address}) para almacenamiento y replicación en {locations}")
                except ValueError:
                    print(f"Error: No se pudo determinar la dirección del DataNode desde el ID '{chosen_datanode_id}'. Se omite el envío del bloque {block_id}.")
                    continue
                except grpc.RpcError as e:
                    print(f"Error al contactar DataNode {chosen_datanode_id} ({datanode_address}): {e}. Se omite el envío del bloque {block_id}.")
                    continue
        file_name_in_dfs = os.path.basename(file_path)
        if self._current_dfs_path_components:
            dfs_destination_path = "/" + "/".join(self._current_dfs_path_components) + "/" + file_name_in_dfs
        else:
            dfs_destination_path = "/" + file_name_in_dfs
        dfs_destination_path = dfs_destination_path.replace('//', '/')
        add_file_response = stub.AddFile(namenode_pb2.AddFileRequest(username=self._current_user, file_path=dfs_destination_path, block_ids=blocks))
        if add_file_response.success:
            print(f"Archivo {file_path} registrado en NameNode en la ruta DFS: {add_file_response.file_path}")
        else:
            print(f"Error al registrar el archivo {dfs_destination_path} en NameNode.")

    def ls(self, dir_path: str = ".", _print_results: bool = True):
        """Lista archivos y directorios en la ruta DFS especificada (relativa o absoluta)."""
        try:
            target_components = self._normalize_path_to_components(dir_path, self._current_dfs_path_components)
            dfs_target_path = "/" + "/".join(target_components) if target_components else "/"

            if not self._current_user:
                raise Exception("User is not logged in. Please log in using 'login <username>' command.")

            stub = get_namenode_stub(NAMENODE_GRPC)
            resp = stub.ListFiles(namenode_pb2.ListFilesRequest(dir_path=dfs_target_path, username=self._current_user))
            formatted_items = []
            for item in resp.items:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    formatted_items.append({
                        "name": item[0],
                        "type": item[1],
                        "size": item[2] if len(item) > 2 else None,
                        "date": item[3] if len(item) > 3 else None,
                        "owner": item[4] if len(item) > 4 else None,
                        "group": item[5] if len(item) > 5 else None,
                        "permissions": item[6] if len(item) > 6 else None
                    })
                else:
                    formatted_items.append({
                        "name": str(item),
                        "type": "unknown"
                    })

            if _print_results:
                if formatted_items:
                    print(f"Contenido de {self.get_current_dfs_display_path()}:")
                    print(f"{'Tipo':<10} {'Nombre'}")
                    print(f"{'----------':<10} {'------'}")
                    for item in formatted_items:
                        item_name = item.get('name', '')
                        item_type = item.get('type', 'unknown')
                        
                        if item_type == "directory":
                            print(f"{'Directorio':<10} {item_name}")
                        else:
                            # Extract extension or default to 'Archivo' if no extension
                            name_parts = item_name.split('.')
                            # Check if there's an actual extension (more than one part and last part is not empty)
                            if len(name_parts) > 1 and name_parts[-1]:
                                file_type_display = name_parts[-1]
                            else:
                                file_type_display = 'Directorio'
                            print(f"{file_type_display:<10} {item_name}")
                else:
                    print(f"El directorio '{dfs_target_path}' está vacío o no existe.")
            return formatted_items
        except Exception as e:
            print(f"Error al listar archivos en '{dir_path}': {e}")
            return []

    def get(self, dfs_path: str, output_path: Optional[Path] = None):
        """Descarga un archivo del DFS y lo reconstruye localmente."""
        try:
            target_components = self._normalize_path_to_components(dfs_path, self._current_dfs_path_components)
            if not target_components:
                logging.warning(f"Invalid DFS path for 'get': {dfs_path}")
                print(f"Error: La ruta '{dfs_path}' no es válida para 'get'.")
                return
            dfs_target_path = "/" + "/".join(target_components)

            if not self._current_user:
                logging.warning("'get' command attempted without a logged-in user.")
                print("Error: No hay usuario logueado. Por favor, inicie sesión para descargar archivos.")
                return

            logging.info(f"Attempting to get file: '{dfs_target_path}' for user '{self._current_user}'")
            stub = get_namenode_stub(NAMENODE_GRPC)
            
            # 1. Call NameNode to get block IDs for dfs_target_path
            file_blocks_response = stub.GetFileBlocks(
                namenode_pb2.FileBlocksRequest(username=self._current_user, file_path=dfs_target_path)
            )
            block_ids = file_blocks_response.block_ids

            if not block_ids:
                logging.warning(f"No blocks found for DFS file '{dfs_target_path}'. File might be empty or not exist.")
                print(f"Error: No se encontraron bloques para el archivo '{dfs_target_path}'. Puede que el archivo esté vacío o no exista.")
                # Decide if an empty file should be created or an error shown
                # For now, let's create an empty file if output_path is specified
                if output_path is None:
                    file_name = target_components[-1]
                    output_path = Path.cwd() / file_name
                else:
                    output_path = Path(output_path)
                    if output_path.is_dir():
                        file_name = target_components[-1]
                        output_path = output_path / file_name
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, 'wb') as f:
                    pass # Create empty file
                print(f"Archivo '{dfs_target_path}' (vacío o no encontrado) guardado como '{output_path}'.")
                return

            logging.info(f"Blocks to download for '{dfs_target_path}': {block_ids}")

            # Determine output file path
            if output_path is None:
                file_name = target_components[-1]
                output_path = Path.cwd() / file_name
            else:
                output_path = Path(output_path)
                if output_path.is_dir():
                    file_name = target_components[-1]
                    output_path = output_path / file_name
            # 2. For each block, get locations and fetch from DataNode
            assembled_content = b""
            for block_id in block_ids:
                logging.info(f"Fetching block '{block_id}' for file '{dfs_target_path}'.")
                loc_resp = stub.GetBlockLocations(namenode_pb2.BlockLocationRequest(block_id=block_id))
                locations = loc_resp.node_ids
                if not locations:
                    logging.error(f"No locations found for block '{block_id}'. Skipping block.")
                    print(f"Error: No se encontraron ubicaciones para el bloque '{block_id}'. Se omite este bloque.")
                    # Potentially raise an error or handle missing block
                    continue
                
                # Try fetching from available datanodes
                block_content = None
                for datanode_id in locations:
                    try:
                        datanode_index = int(datanode_id.replace("datanode", ""))
                        datanode_port = 50050 + datanode_index # Assuming port convention
                        datanode_address = f"localhost:{datanode_port}"
                        logging.info(f"Attempting to fetch block '{block_id}' from {datanode_id} ({datanode_address})")
                        stub_dn = get_datanode_stub(datanode_address)
                        # Assuming DataNode has a GetBlock RPC method
                        # This might need adjustment based on dfs.proto
                        block_resp = stub_dn.GetBlock(dfs_pb2.GetBlockRequest(block_id=block_id))
                        block_content = block_resp.content
                        logging.info(f"Successfully fetched block '{block_id}' from {datanode_id}.")
                        break # Got the block, no need to try other datanodes
                    except ValueError:
                        logging.warning(f"Could not parse DataNode ID '{datanode_id}' for block '{block_id}'.")
                    except grpc.RpcError as e_dn:
                        logging.warning(f"Failed to fetch block '{block_id}' from {datanode_id} ({datanode_address}): {e_dn.details() if hasattr(e_dn, 'details') else e_dn}")
                    except AttributeError as e_attr:
                        # This is to catch if GetBlock is not defined on DataNode stub
                        logging.error(f"DataNode gRPC method error for block '{block_id}' from {datanode_id}: {e_attr}", exc_info=True)
                        print(f"Error interno del cliente: El DataNode no soporta la operación necesaria para obtener el bloque '{block_id}'.")
                        # This is a critical client/proto mismatch, might need to stop
                        raise # Re-raise to indicate a more severe problem

                if block_content is not None:
                    assembled_content += block_content
                else:
                    logging.error(f"Failed to fetch block '{block_id}' from any DataNode. File will be incomplete.")
                    print(f"Error: No se pudo descargar el bloque '{block_id}' de ningún DataNode. El archivo estará incompleto.")
                    # Decide on error handling: partial file, or delete and error out

            # Create the 'descargas' directory if it doesn't exist
            download_dir = Path(PROJECT_ROOT) / "descargas"
            download_dir.mkdir(parents=True, exist_ok=True)

            # Construct the full output path within the 'descargas' directory
            output_file_name = Path(output_path).name
            final_output_path = download_dir / output_file_name

            # Write the assembled content to the final output path
            with open(final_output_path, 'wb') as f_out:
                f_out.write(assembled_content)
            print(f"Archivo '{dfs_target_path}' descargado a '{final_output_path}'.")

        except grpc.RpcError as e:
            logging.error(f"gRPC error during get for '{dfs_path}': {e.details() if hasattr(e, 'details') else e}", exc_info=True)
            print(f"Error de conexión al NameNode o DataNode: {e.details() if hasattr(e, 'details') else e}")
        except FileNotFoundError:
            # This specific exception might be for local file system issues if output_path is complex
            logging.error(f"Error: El archivo local de destino o su directorio padre no existe para '{output_path}'.", exc_info=True)
            print(f"Error: El archivo local de destino o su directorio padre no existe: '{output_path}'.")
        except Exception as e:
            logging.error(f"Error inesperado durante 'get' para '{dfs_path}': {e}", exc_info=True)
            print(f"Error inesperado al procesar 'get' para '{dfs_path}': {e}")

    def mkdir(self, dir_path: str):
        """Crea un directorio en el DFS en la ruta especificada (relativa o absoluta)."""
        try:
            target_components = self._normalize_path_to_components(dir_path, self._current_dfs_path_components)
            if not target_components:
                print(f"Error: La ruta '{dir_path}' (resuelta a la raíz '/') no es válida para 'mkdir'.")
                return
            dfs_target_path = "/" + "/".join(target_components)

            if not self._current_user:
                print("Error: No hay usuario logueado. Por favor, inicie sesión para crear directorios.")
                return

            stub = get_namenode_stub(NAMENODE_GRPC)
            resp = stub.Mkdir(namenode_pb2.MkdirRequest(dir_path=dfs_target_path, username=self._current_user))
            if resp.success:
                print(f"Directorio '{dfs_target_path}' creado exitosamente.")
            else:
                print(f"Error al crear directorio '{dfs_target_path}'.")
        except Exception as e:
            print(f"Error al procesar la ruta para 'mkdir' '{dir_path}': {e}")

    def rmdir(self, dir_path: str):
        """Elimina un directorio vacío del DFS en la ruta especificada (relativa o absoluta)."""
        try:
            target_components = self._normalize_path_to_components(dir_path, self._current_dfs_path_components)
            if not target_components:
                print(f"Error: La ruta '{dir_path}' (resuelta a la raíz '/') no es válida para 'rmdir'. No se puede eliminar la raíz.")
                return
            dfs_target_path = "/" + "/".join(target_components)

            if not self._current_user:
                print("Error: No hay usuario logueado. Por favor, inicie sesión para eliminar directorios.")
                return

            stub = get_namenode_stub(NAMENODE_GRPC)
            resp = stub.Rmdir(namenode_pb2.RmdirRequest(dir_path=dfs_target_path, username=self._current_user))
            if resp.success:
                print(f"Directorio '{dfs_target_path}' eliminado exitosamente.")
            else:
                print(f"Error al eliminar el directorio '{dfs_target_path}'. Puede que no exista o no esté vacío.")
        except Exception as e:
            print(f"Error al procesar la ruta para 'rmdir' '{dir_path}': {e}")

    def rm(self, file_path: str):
        """Elimina un archivo del DFS en la ruta especificada (relativa o absoluta)."""
        try:
            target_components = self._normalize_path_to_components(file_path, self._current_dfs_path_components)
            if not target_components:
                print(f"Error: La ruta '{file_path}' (resuelta a la raíz '/') no es válida para 'rm'. No se puede eliminar la raíz como un archivo.")
                return
            dfs_target_path = "/" + "/".join(target_components)

            if not self._current_user:
                print("Error: No hay usuario logueado. Por favor, inicie sesión para eliminar archivos.")
                return

            stub = get_namenode_stub(NAMENODE_GRPC)
            resp = stub.RemoveFile(namenode_pb2.RemoveFileRequest(file_path=dfs_target_path, username=self._current_user))
            if resp.success:
                print(f"Archivo '{dfs_target_path}' eliminado exitosamente.")
            else:
                print(f"Error al eliminar el archivo '{dfs_target_path}'. Puede que no exista o sea un directorio.")
        except Exception as e:
            print(f"Error al procesar la ruta para 'rm' '{file_path}': {e}")

    def cd(self, dir_path: str):
        """Cambia el directorio DFS actual."""
        try:
            new_components = self._normalize_path_to_components(dir_path, self._current_dfs_path_components)
            self._current_dfs_path_components = new_components
            print(f"Directorio actual: {self.get_current_dfs_display_path()}")
        except Exception as e:
            print(f"Error al cambiar de directorio: {e}")

    def mv(self, source_path: str, destination_path: str):
        """Mueve un archivo o directorio (source_path) a una nueva ubicación (destination_path) en el DFS."""
        try:
            source_components = self._normalize_path_to_components(source_path, self._current_dfs_path_components)
            dfs_source_path = "/" + "/".join(source_components) if source_components else "/"
            if not source_components or dfs_source_path == "/":
                print(f"Error: La ruta de origen '{source_path}' (resuelta a '{dfs_source_path}') no es válida para 'mv'.")
                return

            dest_components = self._normalize_path_to_components(destination_path, self._current_dfs_path_components)
            dfs_destination_path = "/" + "/".join(dest_components) if dest_components else "/"
            
            if dfs_source_path == dfs_destination_path:
                print(f"Error: La ruta de origen y destino son la misma ('{dfs_source_path}').")
                return

            if not self._current_user:
                print("Error: No hay usuario logueado. Por favor, inicie sesión para mover archivos/directorios.")
                return

            stub = get_namenode_stub(NAMENODE_GRPC)
            resp = stub.Move(namenode_pb2.MoveRequest(source_path=dfs_source_path, destination_path=dfs_destination_path, username=self._current_user))
            
            if resp.success:
                print(f"'{dfs_source_path}' movido exitosamente a '{resp.message}'.")
            else:
                print(f"Error al mover '{dfs_source_path}' a '{dfs_destination_path}': {resp.message}")
        except Exception as e:
            print(f"Error al procesar la ruta para 'mv' '{source_path}' a '{destination_path}': {e}")

    def do_ls(self, arg):
        """List files and directories in the specified DFS path.
        Usage: ls [path]
        """
        try:
            self.ls(arg) if arg else self.ls()
        except Exception as e:
            print(f"Error executing ls: {e}")

    def do_cd(self, arg):
        """Change the current DFS directory.
        Usage: cd <path>
        """
        try:
            self.cd(arg)
            self.prompt = f"DFS-CLI:{self.get_current_dfs_display_path()}> "
        except Exception as e:
            print(f"Error executing cd: {e}")

    def do_put(self, arg):
        """Upload a file to the DFS: put <local_file_path>"""
        logging.info(f"'put' command invoked with raw argument: '{arg}'")
        if not self._current_user:
            logging.warning("'put' command attempted without a logged-in user.")
            print("Por favor, inicie sesión primero con 'login <username>'.")
            return
        if not arg:
            logging.warning("'put' command invoked with no argument.")
            print("Uso: put <local_file_path>")
            return

        # Strip leading/trailing quotes that cmd module might pass if path has spaces
        processed_arg = arg.strip()
        if (processed_arg.startswith('"') and processed_arg.endswith('"')) or \
           (processed_arg.startswith("'") and processed_arg.endswith("'")):
            processed_arg = processed_arg[1:-1]
        
        logging.info(f"Processed file path for 'put': '{processed_arg}'")

        try:
            file_path = Path(processed_arg)
            if not file_path.exists() or not file_path.is_file():
                logging.error(f"Local file '{processed_arg}' not found or not a file.")
                print(f"Error: El archivo local '{processed_arg}' no existe o no es un archivo.")
                return
            logging.info(f"Attempting to upload file: {file_path}")
            self.put(file_path) # Call the internal put method
        except Exception as e:
            logging.error(f"Error executing put for '{processed_arg}': {e}", exc_info=True)
            print(f"Error ejecutando put: {e}")

    def do_get(self, arg):
        """Download a file from the DFS to a local path.
        Usage: get <dfs_file_path> [output_local_path]
        """
        try:
            args = arg.split(maxsplit=1)
            if len(args) == 2:
                self.get(args[0], Path(args[1]))
            else:
                self.get(args[0])
        except Exception as e:
            print(f"Error executing get: {e}")

    def do_rm(self, arg):
        """Remove a file or directory from the DFS.
        Usage: rm <dfs_path>
        """
        try:
            self.rm(arg)
        except Exception as e:
            print(f"Error executing rm: {e}")

    def do_mkdir(self, arg):
        """Create a directory in the DFS.
        Usage: mkdir <dfs_path>
        """
        try:
            self.mkdir(arg)
        except Exception as e:
            print(f"Error executing mkdir: {e}")

    def do_rmdir(self, arg):
        """Remove an empty directory from the DFS.
        Usage: rmdir <dfs_path>
        """
        try:
            self.rmdir(arg)
        except Exception as e:
            print(f"Error executing rmdir: {e}")

    def do_mv(self, arg):
        """Move or rename a file or directory in the DFS.
        Usage: mv <source_path> <destination_path>
        """
        try:
            args = arg.split(maxsplit=1)
            if len(args) == 2:
                self.mv(args[0], args[1])
            else:
                print("Usage: mv <source_path> <destination_path>")
        except Exception as e:
            print(f"Error executing mv: {e}")

    def complete_ls(self, text, line, begidx, endidx):
        return [i.get('name') for i in self.ls(dir_path=".", _print_results=False) if i.get('name', '').startswith(text)]

    def complete_cd(self, text, line, begidx, endidx):
        return [i.get('name') for i in self.ls(dir_path=".", _print_results=False) if i.get('name', '').startswith(text)]

    def complete_rm(self, text, line, begidx, endidx):
        return [i.get('name') for i in self.ls(dir_path=".", _print_results=False) if i.get('name', '').startswith(text)]

    def complete_mkdir(self, text, line, begidx, endidx):
        return [i.get('name') for i in self.ls(dir_path=".", _print_results=False) if i.get('name', '').startswith(text)]

    def complete_mv(self, text, line, begidx, endidx):
        # Autocompletado para el primer argumento (source_path)
        args = line.split()
        if len(args) == 2 and not line.endswith(' '):
            return [i.get('name') for i in self.ls(dir_path=".", _print_results=False) if i.get('name', '').startswith(text)]
        # Autocompletado para el segundo argumento (destination_path)
        elif len(args) == 3 or (len(args) == 2 and line.endswith(' ')): # Si ya se escribió el primer argumento y se puso un espacio
            return [i.get('name') for i in self.ls(dir_path=".", _print_results=False) if i.get('name', '').startswith(text)]
        return []

    def complete_put(self, text, line, begidx, endidx):
        # Autocompletado para archivos locales
        return [f for f in os.listdir('.') if f.startswith(text)]

    def complete_get(self, text, line, begidx, endidx):
        # Autocompletado para archivos DFS
        args = line.split()
        if len(args) == 2 and not line.endswith(' '):
            return [i.get('name') for i in self.ls(dir_path=".", _print_results=False) if i.get('name', '').startswith(text)]
        # Autocompletado para el segundo argumento (output_local_path)
        elif len(args) == 3 or (len(args) == 2 and line.endswith(' ')): # Si ya se escribió el primer argumento y se puso un espacio
            return [f for f in os.listdir('.') if f.startswith(text)]
        return []

    def do_login(self, arg):
        """login <username> - Logs in a user to the DFS.
        If no username is provided, it will prompt for it.
        """
        try:
            if arg:
                username = arg
            else:
                username = input("Username: ")
            success, message = self.login(username)
            print(message)
            if success:
                self.update_prompt()
        except Exception as e:
            print(f"Error executing login: {e}")

    def do_logout(self, arg):
        """logout - Logs out the current user from the DFS.
        """
        try:
            self.logout()
            self.update_prompt()
        except Exception as e:
            print(f"Error executing logout: {e}")

    def do_whoami(self, arg):
        """whoami - Shows the currently logged-in user.
        """
        try:
            self.whoami()
        except Exception as e:
            print(f"Error executing whoami: {e}")

    def do_exit(self, arg):
        """exit - Exits the DFS CLI.
        """
        return True

    def do_quit(self, arg):
        """quit - Exits the DFS CLI.
        """
        return True

    def update_prompt(self):
        """Updates the prompt to reflect the current DFS path and logged-in user."""
        user_info = f"({self._current_user})" if self._current_user else ""
        self.prompt = f"DFS-CLI:{self.get_current_dfs_display_path()}{user_info}> "

    def preloop(self):
        """Called once before the command loop starts."""
        print("Bienvenido al DFS CLI. Por favor, inicie sesión con el comando 'login'.")
        self.update_prompt()
        # Optionally, prompt for login immediately


if __name__ == "__main__":
    cli = DFSCLI()
    cli.cmdloop()