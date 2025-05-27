import time
import threading
import random
import os
import posixpath
import logging

class NameNode:
    def _canonical_dfs_path(self, username: str, path_str: str) -> str:
        """
        Normalizes a DFS-style path to a canonical form for a specific user:
        - Uses forward slashes '/'.
        - Is absolute (starts with '/').
        - Resolves '.' and '..' components (via posixpath.normpath).
        - No trailing slash unless it's the root '/'.
        - Multiple slashes are collapsed.
        - Prepends '/user/<username>' to the path.
        """
        if not username:
            raise ValueError("Username cannot be empty.")

        # Ensure path starts with / if it's not already, as all paths in DFS are absolute from root.
        temp_path = path_str
        if not temp_path.startswith('/'):
            temp_path = '/' + temp_path
        
        # Use posixpath.normpath for normalization. It always uses '/' as separator.
        canonical = posixpath.normpath(temp_path)
        
        # Prepend the user's root directory
        user_root = f"/user/{username}"
        if canonical == "/":
            full_path = user_root
        else:
            full_path = posixpath.join(user_root, canonical.lstrip('/'))
        
        return full_path

    def __init__(self, replication_factor=3, block_size_mb=64):
        self.user_block_maps = {}  # {username: {file_path: [block_ids]}}
        self.block_locations = {}  # {block_id: [node_id]}
        self.data_nodes = {} # {node_id: {'last_heartbeat': timestamp, 'blocks': set()}}
        self.replication_factor = replication_factor
        self.block_size_mb = block_size_mb
        self.lock = threading.Lock()
        self.active_users = {} # {username: last_login_time}
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - NAMENODE - %(levelname)s - %(message)s')
        logging.info("NameNode initialized.")

    def _check_user_logged_in(self, username: str):
        """Checks if a user is logged in. Raises an exception if not."""
        if username not in self.active_users:
            logging.warning(f"Action denied for non-logged-in user: '{username}'. Active users: {list(self.active_users.keys())}")
            raise Exception(f"User '{username}' is not logged in. Please login first.")
        logging.info(f"User '{username}' is logged in. Proceeding with action.")


    def register_datanode(self, node_id):
        with self.lock:
            self.data_nodes[node_id] = {'last_heartbeat': time.time(), 'blocks': set()}

    def heartbeat(self, node_id):
        with self.lock:
            if node_id in self.data_nodes:
                self.data_nodes[node_id]['last_heartbeat'] = time.time()

    def allocate_blocks(self, username: str, file_size: int) -> list[str]:
        self._check_user_logged_in(username)
        with self.lock:

            num_blocks = (file_size + self.block_size_mb * 1024 * 1024 - 1) // (self.block_size_mb * 1024 * 1024)
            block_ids = [f"block_{int(time.time()*1000)}_{i}_{random.randint(0,9999)}" for i in range(num_blocks)]
            
            node_ids = list(self.data_nodes.keys())
            if not node_ids:
                raise Exception("No hay DataNodes registrados para asignar bloques.")

            if len(node_ids) < self.replication_factor:
                raise Exception(f"No hay suficientes DataNodes ({len(node_ids)}) para cumplir con el factor de replicación ({self.replication_factor}).")

            for block_id in block_ids:
                # Selección aleatoria de nodos para replicación
                if len(node_ids) < self.replication_factor:
                    # Esto ya se verifica arriba, pero es una doble seguridad.
                    raise Exception(f"No hay suficientes DataNodes ({len(node_ids)}) para el factor de replicación ({self.replication_factor})")
                
                # Barajar la lista de nodos disponibles y tomar los primeros 'replication_factor'
                random.shuffle(node_ids) # Baraja la lista en el lugar
                selected_nodes = node_ids[:self.replication_factor]
                
                self.block_locations[block_id] = selected_nodes
                for n_id in selected_nodes: 
                    self.data_nodes[n_id]['blocks'].add(block_id)
            return block_ids

    def get_block_locations(self, block_id):
        with self.lock:
            return self.block_locations.get(block_id, [])

    def get_file_content(self, username: str, file_path: str):
        self._check_user_logged_in(username)
        with self.lock:
            if username not in self.active_users:
                raise Exception(f"User '{username}' is not logged in.")


            canonical_path = self._canonical_dfs_path(username, file_path)
            user_map = self.user_block_maps.setdefault(username, {})
            return user_map.get(canonical_path, [])

    def get_file_blocks(self, username: str, file_path: str) -> list[str]:
        self._check_user_logged_in(username)
        with self.lock:
            canonical_path = self._canonical_dfs_path(username, file_path)
            user_map = self.user_block_maps.setdefault(username, {})
            # If the path exists and is a file (not a directory, which is represented by an empty list)
            if canonical_path in user_map and user_map[canonical_path] != []:
                return user_map[canonical_path]
            else:
                raise Exception(f"File '{file_path}' not found or is a directory.")

    def add_file(self, username: str, file_path: str, block_ids: list[str]):
        self._check_user_logged_in(username)
        with self.lock:
            canonical_path = self._canonical_dfs_path(username, file_path)
            user_map = self.user_block_maps.setdefault(username, {})
            if canonical_path in user_map and user_map[canonical_path] == []:
                raise Exception(f"No se puede crear el archivo '{canonical_path}' porque ya existe un directorio con ese nombre.")
            user_map[canonical_path] = block_ids

    def mkdir(self, username: str, dir_path: str):
        self._check_user_logged_in(username)
        with self.lock:

            canonical_path = self._canonical_dfs_path(username, dir_path)
            user_map = self.user_block_maps.setdefault(username, {})
            if canonical_path in user_map:
                # Check if it's a file or directory
                if user_map[canonical_path] == []:
                    raise Exception(f"El directorio '{canonical_path}' ya existe.")
                else:
                    raise Exception(f"No se puede crear el directorio '{canonical_path}' porque ya existe un archivo con ese nombre.")
            user_map[canonical_path] = [] # Represents a directory

    def rmdir(self, username: str, dir_path: str):
        self._check_user_logged_in(username)
        with self.lock:

            canonical_dir_to_delete = self._canonical_dfs_path(username, dir_path)
            user_map = self.user_block_maps.setdefault(username, {})

            if canonical_dir_to_delete not in user_map:
                raise Exception(f"El directorio '{canonical_dir_to_delete}' no existe.")
            
            if user_map[canonical_dir_to_delete] != []: # Check if it's marked as a directory
                raise Exception(f"La ruta '{canonical_dir_to_delete}' no es un directorio.")

            # Check if the directory is empty
            children = []
            for item_path in user_map.keys():
                if item_path == canonical_dir_to_delete:
                    continue
                parent_of_item = posixpath.dirname(item_path)
                if parent_of_item == canonical_dir_to_delete:
                    children.append(posixpath.basename(item_path))
            
            if children:
                raise Exception(f"El directorio '{canonical_dir_to_delete}' no está vacío. Contiene: {children}")
            
            del user_map[canonical_dir_to_delete]
            print(f"Directorio '{canonical_dir_to_delete}' eliminado.")

    def ls(self, username: str, dir_path: str):
        self._check_user_logged_in(username)
        with self.lock:

            query_canonical_path = self._canonical_dfs_path(username, dir_path)
            user_map = self.user_block_maps.setdefault(username, {})
            results = []
            for item_canonical_path in user_map.keys():
                parent_dir_of_item = posixpath.dirname(item_canonical_path)
                if parent_dir_of_item == query_canonical_path:
                    results.append(posixpath.basename(item_canonical_path))
            return sorted(list(set(results)))

    def mv(self, username: str, source_path_str: str, destination_path_str: str):
        self._check_user_logged_in(username)
        with self.lock:

            canonical_source = self._canonical_dfs_path(username, source_path_str)
            canonical_dest = self._canonical_dfs_path(username, destination_path_str)
            user_map = self.user_block_maps.setdefault(username, {})

            if canonical_source == f"/user/{username}":
                return False, "No se puede mover el directorio raíz de usuario."
            if canonical_dest == f"/user/{username}": 
                final_target_path = self._canonical_dfs_path(username, posixpath.join("/", posixpath.basename(canonical_source)))
            else:
                final_target_path = canonical_dest

            # Verificar si el origen existe
            if canonical_source not in user_map:
                return False, f"La ruta de origen '{canonical_source}' no existe."

            # Verificar si el destino es un subdirectorio del origen (para directorios)
            if user_map[canonical_source] == [] and final_target_path.startswith(canonical_source + '/') : # Es un directorio
                return False, f"No se puede mover un directorio ('{canonical_source}') a un subdirectorio de sí mismo ('{final_target_path}')."

            # Verificar si el destino ya existe (si no es el mismo que el origen después de la normalización)
            effective_final_target_path = final_target_path
            if final_target_path in user_map and user_map[final_target_path] == []: # Si el destino es un directorio existente
                effective_final_target_path = self._canonical_dfs_path(username, posixpath.join(final_target_path, posixpath.basename(canonical_source)))
            
            if effective_final_target_path in user_map and effective_final_target_path != canonical_source:
                 return False, f"La ruta de destino '{effective_final_target_path}' ya existe."

            # Actualizamos final_target_path para que sea la ruta efectiva donde se moverá el item.
            final_target_path = effective_final_target_path

            # Lógica de movimiento
            if user_map[canonical_source] == []: # Es un directorio
                # Mover el directorio en sí
                user_map[final_target_path] = []
                del user_map[canonical_source]

                # Mover todos los elementos hijos
                items_to_move = []
                for item_path in list(user_map.keys()):
                    if item_path.startswith(canonical_source + '/'):
                        items_to_move.append(item_path)
                
                for old_item_path in items_to_move:
                    relative_to_source = posixpath.relpath(old_item_path, canonical_source)
                    new_item_path = posixpath.join(final_target_path, relative_to_source)
                    new_item_path_canonical = self._canonical_dfs_path(username, new_item_path)

                    user_map[new_item_path_canonical] = user_map.pop(old_item_path)
                print(f"Directorio '{canonical_source}' y su contenido movido a '{final_target_path}'.")
                return True, final_target_path # Devuelve la ruta final donde se movió.
            else: # Es un archivo
                user_map[final_target_path] = user_map.pop(canonical_source)
                print(f"Archivo '{canonical_source}' movido a '{final_target_path}'.")
                return True, final_target_path # Devuelve la ruta final donde se movió.

    def login(self, username: str) -> tuple[bool, str]:
        logging.info(f"Login attempt for user: '{username}'.")
        with self.lock:
            if not username:
                logging.warning("Login attempt with empty username.")
                return False, "Username cannot be empty."
            
            if username in self.active_users:
                logging.info(f"User '{username}' attempted to log in again. Already active.")
                self.active_users[username] = time.time() # Update last seen time
                logging.info(f"Active users: {list(self.active_users.keys())}")
                return True, f"User '{username}' is already logged in. Session refreshed."

            self.active_users[username] = time.time()
            logging.info(f"User '{username}' logged in successfully.")
            logging.info(f"Active users: {list(self.active_users.keys())}")
            # Asegúrate que _canonical_dfs_path es llamado correctamente si es necesario aquí
            # Por ejemplo, si necesitas la ruta canónica para el mensaje de retorno o lógica interna.
            # canonical_home_path = self._canonical_dfs_path(username, '/') 
            # return True, f"User '{username}' logged in successfully. Home: {canonical_home_path}"
            return True, f"User '{username}' logged in successfully."

    def logout(self, username: str) -> tuple[bool, str]:
        logging.info(f"Logout attempt for user: '{username}'.")
        with self.lock:
            if username not in self.active_users:
                logging.warning(f"Logout attempt for non-active user: '{username}'.")
                logging.info(f"Active users: {list(self.active_users.keys())}")
                return False, f"User '{username}' is not logged in."
            
            del self.active_users[username]
            logging.info(f"User '{username}' logged out successfully.")
            logging.info(f"Active users: {list(self.active_users.keys())}")
            return True, f"User '{username}' logged out successfully."

    def rm(self, username: str, file_path: str):
        self._check_user_logged_in(username)
        with self.lock:

            canonical_path = self._canonical_dfs_path(username, file_path)
            user_map = self.user_block_maps.setdefault(username, {})
            
            if canonical_path not in user_map:
                raise Exception(f"El archivo o directorio '{canonical_path}' no existe.")
            
            # Check if it's a directory (represented by an empty list of blocks)
            if user_map[canonical_path] == []:
                raise Exception(f"'{canonical_path}' es un directorio. Use rmdir para eliminar directorios.")

            blocks_to_remove = user_map.pop(canonical_path, None)
            if blocks_to_remove is None: 
                return

            for block_id in blocks_to_remove: 
                if block_id in self.block_locations:
                    nodes_with_block = self.block_locations.pop(block_id, [])
                    for node_id in nodes_with_block:
                        if node_id in self.data_nodes and 'blocks' in self.data_nodes[node_id]:
                            self.data_nodes[node_id]['blocks'].discard(block_id)
            print(f"Archivo '{canonical_path}' y sus bloques asociados eliminados de los metadatos.")

    def get_file_content(self, username: str, file_path: str):
        self._check_user_logged_in(username)
        # Simulación: solo retorna los bloques asignados
        with self.lock:
            canonical_path = self._canonical_dfs_path(username, file_path)
            user_map = self.user_block_maps.setdefault(username, {})
            return user_map.get(canonical_path, [])

    def get_file_blocks(self, username: str, file_path: str) -> list[str]:
        self._check_user_logged_in(username)
        with self.lock:
            canonical_path = self._canonical_dfs_path(username, file_path)
            user_map = self.user_block_maps.setdefault(username, {})
            # If the path exists and is a file (not a directory, which is represented by an empty list)
            if canonical_path in user_map and user_map[canonical_path] != []:
                return user_map[canonical_path]
            else:
                raise Exception(f"File '{file_path}' not found or is a directory.") 

    def check_and_rereplicate(self):
        with self.lock:
            now = time.time()
            inactive_threshold = 30  # Segundos para considerar un nodo inactivo
            
            all_registered_nodes = set(self.data_nodes.keys())
            active_nodes_current_check = {
                n_id for n_id, data in self.data_nodes.items() 
                if now - data.get('last_heartbeat', 0) <= inactive_threshold
            }
            inactive_nodes_detected_this_check = all_registered_nodes - active_nodes_current_check

            if inactive_nodes_detected_this_check:
                print(f"NameNode: DataNodes inactivos detectados en esta revisión: {inactive_nodes_detected_this_check}")

            blocks_to_rereplicate_map = {} # block_id -> {'current_live_nodes': set(), 'needed_count': int}

            for block_id, nodes_hosting_block in list(self.block_locations.items()): # Iterar sobre copia por si se modifica
                current_live_replicas_for_block = [n_id for n_id in nodes_hosting_block if n_id in active_nodes_current_check]
                
                num_live_replicas = len(current_live_replicas_for_block)
                
                if num_live_replicas < self.replication_factor:
                    needed = self.replication_factor - num_live_replicas
                    if needed > 0:
                        blocks_to_rereplicate_map[block_id] = {
                            'current_live_nodes': set(current_live_replicas_for_block),
                            'needed_count': needed
                        }
            
            if not blocks_to_rereplicate_map:
                # print("NameNode: No hay bloques que necesiten re-replicación inmediata.")
                return

            print(f"NameNode: Bloques que necesitan re-replicación: {list(blocks_to_rereplicate_map.keys())}")

            for block_id, info in blocks_to_rereplicate_map.items():
                needed_count = info['needed_count']
                current_block_holders = info['current_live_nodes'] # Nodos activos que ya tienen este bloque

                # Nodos candidatos para nuevas réplicas: activos y NO tienen ya este bloque.
                potential_new_targets = [
                    n_id for n_id in active_nodes_current_check
                    if n_id not in current_block_holders
                ]
                
                if not potential_new_targets:
                    print(f"NameNode: Advertencia - No hay DataNodes candidatos disponibles para re-replicar el bloque {block_id} (todos los activos ya lo tienen o no hay otros activos).")
                    continue

                # Barajar los nodos candidatos para selección aleatoria
                random.shuffle(potential_new_targets)
                
                # Seleccionar los 'needed_count' nodos necesarios de la lista barajada
                nodes_to_receive_replica = potential_new_targets[:needed_count]

                if len(nodes_to_receive_replica) < needed_count:
                    print(f"NameNode: Advertencia - No se pudieron encontrar suficientes ({len(nodes_to_receive_replica)} de {needed_count}) DataNodes únicos y disponibles para re-replicar completamente el bloque {block_id}.")

                # Simular la re-replicación (en un sistema real, esto implicaría una comunicación con DataNodes)
                source_node_for_replication = next(iter(current_block_holders), None) if current_block_holders else None
                if not source_node_for_replication:
                    print(f"NameNode: Error Crítico - El bloque {block_id} ha perdido todas sus réplicas activas. No se puede re-replicar sin una fuente.")
                    continue

                for target_node_id in nodes_to_receive_replica:
                    print(f"NameNode: Iniciando re-replicación (simulada) del bloque {block_id} desde {source_node_for_replication} hacia {target_node_id}")
                    # Actualizar metadatos
                    if block_id in self.block_locations:
                        self.block_locations[block_id].append(target_node_id)
                    else: # El bloque podría haber sido eliminado mientras tanto
                        print(f"NameNode: El bloque {block_id} ya no existe en block_locations, se omite la re-replicación para {target_node_id}.")
                        continue
                    
                    if target_node_id in self.data_nodes: # Asegurarse que el nodo aún está registrado y activo
                        self.data_nodes[target_node_id]['blocks'].add(block_id)
                    else:
                        print(f"NameNode: El nodo destino {target_node_id} ya no está registrado o activo. No se pudo agregar el bloque {block_id}.")
            
            # Lógica para eliminar DataNodes completamente inactivos del registro (opcional, manejar con cuidado)
            # Por ahora, los nodos inactivos permanecen en self.data_nodes pero no se usan para nuevas asignaciones
            # y sus bloques se re-replican. Una limpieza periódica más agresiva podría hacerse aquí.
            # Ejemplo: si un nodo ha estado inactivo por mucho tiempo (ej. 24 horas) y todos sus bloques
            # (si los tuviera) ya han sido re-replicados o no existen, entonces eliminarlo.
            # Esta parte es compleja y requiere una política clara para evitar la pérdida de datos.
            # for node_id_to_remove in list(inactive_nodes_detected_this_check): # Iterar sobre copia
            #    if now - self.data_nodes.get(node_id_to_remove, {}).get('last_heartbeat', 0) > VERY_LONG_TIME_THRESHOLD:
            #        # Verificar si es seguro eliminarlo (todos sus bloques están OK en otros nodos)
            #        # ... lógica de verificación ...
            #        if safe_to_remove_datanode_permanently:
            #            print(f"NameNode: Eliminando permanentemente el DataNode {node_id_to_remove} del registro.")
            #            del self.data_nodes[node_id_to_remove]
            #            # También limpiar de block_locations si aún aparece allí (aunque la re-replicación debería haberlo manejado)
            #            for block_id, nodes in list(self.block_locations.items()):
            #                if node_id_to_remove in nodes:
            #                    self.block_locations[block_id] = [n for n in nodes if n != node_id_to_remove]
            #                    if not self.block_locations[block_id]: # Si era la última copia
            #                        print(f"NameNode: Advertencia - El bloque {block_id} perdió su última copia al eliminar {node_id_to_remove}.")
            #                        # Esto indica un problema en la lógica de re-replicación previa.
            pass # Fin de check_and_rereplicate