import time
import threading
import random
import os
import posixpath

class NameNode:
    def _canonical_dfs_path(self, path_str: str) -> str:
        """
        Normalizes a DFS-style path to a canonical form:
        - Uses forward slashes '/'.
        - Is absolute (starts with '/').
        - Resolves '.' and '..' components (via posixpath.normpath).
        - No trailing slash unless it's the root '/'.
        - Multiple slashes are collapsed.
        """
        if not path_str:
            # This case should ideally be handled by client sending valid paths
            # or be an error, but for safety, default to root.
            return "/"

        # Ensure path starts with / if it's not already, as all paths in DFS are absolute from root.
        temp_path = path_str
        if not temp_path.startswith('/'):
            temp_path = '/' + temp_path
        
        # Use posixpath.normpath for normalization. It always uses '/' as separator.
        canonical = posixpath.normpath(temp_path)
        
        # posixpath.normpath("/") is "/"
        # posixpath.normpath("/foo/./bar//baz/../qux") is "/foo/bar/qux"
        # posixpath.normpath("/../foo") is "/foo" (handles '..' at root correctly)
        return canonical

    def __init__(self, replication_factor=3, block_size_mb=64):
        self.block_map = {}  # {file_path: [block_ids]}
        self.block_locations = {}  # {block_id: [node_id]}
        self.data_nodes = {} # {node_id: {'last_heartbeat': timestamp, 'blocks': set()}}
        self.replication_factor = replication_factor
        self.block_size_mb = block_size_mb
        self.lock = threading.Lock()
        self.active_users = {} # {username: last_login_time}

    def register_datanode(self, node_id):
        with self.lock:
            self.data_nodes[node_id] = {'last_heartbeat': time.time(), 'blocks': set()}

    def heartbeat(self, node_id):
        with self.lock:
            if node_id in self.data_nodes:
                self.data_nodes[node_id]['last_heartbeat'] = time.time()

    def allocate_blocks(self, file_size: int) -> list[str]:
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

    def get_file_blocks(self, file_path):
        with self.lock:
            canonical_path = self._canonical_dfs_path(file_path)
            return self.block_map.get(canonical_path, [])

    def add_file(self, file_path, block_ids):
        with self.lock:
            canonical_path = self._canonical_dfs_path(file_path)
            if canonical_path in self.block_map and self.block_map[canonical_path] == []:
                raise Exception(f"No se puede crear el archivo '{canonical_path}' porque ya existe un directorio con ese nombre.")
            self.block_map[canonical_path] = block_ids

    def mkdir(self, dir_path):
        with self.lock:
            canonical_path = self._canonical_dfs_path(dir_path)
            if canonical_path in self.block_map:
                # Check if it's a file or directory
                if self.block_map[canonical_path] == []:
                    raise Exception(f"El directorio '{canonical_path}' ya existe.")
                else:
                    raise Exception(f"No se puede crear el directorio '{canonical_path}' porque ya existe un archivo con ese nombre.")
            self.block_map[canonical_path] = [] # Represents a directory

    def rmdir(self, dir_path):
        with self.lock:
            canonical_dir_to_delete = self._canonical_dfs_path(dir_path)

            if canonical_dir_to_delete not in self.block_map:
                raise Exception(f"El directorio '{canonical_dir_to_delete}' no existe.")
            
            if self.block_map[canonical_dir_to_delete] != []: # Check if it's marked as a directory
                raise Exception(f"La ruta '{canonical_dir_to_delete}' no es un directorio.")

            # Check if the directory is empty
            children = []
            for item_path in self.block_map.keys():
                if item_path == canonical_dir_to_delete:
                    continue
                parent_of_item = posixpath.dirname(item_path)
                if parent_of_item == canonical_dir_to_delete:
                    children.append(posixpath.basename(item_path))
            
            if children:
                raise Exception(f"El directorio '{canonical_dir_to_delete}' no está vacío. Contiene: {children}")
            
            del self.block_map[canonical_dir_to_delete]
            print(f"Directorio '{canonical_dir_to_delete}' eliminado.")

    def ls(self, dir_path):
        with self.lock:
            query_canonical_path = self._canonical_dfs_path(dir_path)
            results = []
            for item_canonical_path in self.block_map.keys():
                parent_dir_of_item = posixpath.dirname(item_canonical_path)
                if parent_dir_of_item == query_canonical_path:
                    results.append(posixpath.basename(item_canonical_path))
            return sorted(list(set(results)))

    def mv(self, source_path_str: str, destination_path_str: str):
        with self.lock:
            canonical_source = self._canonical_dfs_path(source_path_str)
            canonical_dest = self._canonical_dfs_path(destination_path_str)

            if canonical_source == "/":
                return False, "No se puede mover el directorio raíz '/' ."
            if canonical_dest == "/": # Mover a la raíz es permitido, pero el destino final será /nombre_del_origen
                final_target_path = self._canonical_dfs_path(posixpath.join("/", posixpath.basename(canonical_source)))
            else:
                final_target_path = canonical_dest

            # Verificar si el origen existe
            if canonical_source not in self.block_map:
                return False, f"La ruta de origen '{canonical_source}' no existe."

            # Verificar si el destino es un subdirectorio del origen (para directorios)
            if self.block_map[canonical_source] == [] and final_target_path.startswith(canonical_source + '/') : # Es un directorio
                return False, f"No se puede mover un directorio ('{canonical_source}') a un subdirectorio de sí mismo ('{final_target_path}')."

            # Verificar si el destino ya existe (si no es el mismo que el origen después de la normalización)
            # Esta condición es compleja porque si final_target_path es un directorio, queremos mover *dentro* de él.
            # Si final_target_path es un archivo, es un error.
            # Si final_target_path no existe, está bien.

            # Primero, determinamos la ruta de destino *efectiva*.
            effective_final_target_path = final_target_path
            if final_target_path in self.block_map and self.block_map[final_target_path] == []: # Si el destino es un directorio existente
                effective_final_target_path = self._canonical_dfs_path(posixpath.join(final_target_path, posixpath.basename(canonical_source)))
            
            if effective_final_target_path in self.block_map and effective_final_target_path != canonical_source:
                 return False, f"La ruta de destino '{effective_final_target_path}' ya existe."

            # Actualizamos final_target_path para que sea la ruta efectiva donde se moverá el item.
            final_target_path = effective_final_target_path

            # Lógica de movimiento
            if self.block_map[canonical_source] == []: # Es un directorio
                # Mover el directorio en sí
                self.block_map[final_target_path] = []
                del self.block_map[canonical_source]

                # Mover todos los elementos hijos
                # Necesitamos iterar sobre una copia de las claves si vamos a modificar el diccionario
                items_to_move = []
                for item_path in list(self.block_map.keys()):
                    if item_path.startswith(canonical_source + '/'):
                        items_to_move.append(item_path)
                
                for old_item_path in items_to_move:
                    # Construir la nueva ruta para el item hijo
                    # Ejemplo: source=/a/b, item_path=/a/b/c/d.txt, final_target_path=/x/y (que es el nuevo /a/b)
                    # new_item_path debe ser /x/y/c/d.txt
                    relative_to_source = posixpath.relpath(old_item_path, canonical_source)
                    new_item_path = posixpath.join(final_target_path, relative_to_source)
                    new_item_path_canonical = self._canonical_dfs_path(new_item_path)

                    self.block_map[new_item_path_canonical] = self.block_map.pop(old_item_path)
                print(f"Directorio '{canonical_source}' y su contenido movido a '{final_target_path}'.")
                return True, final_target_path # Devuelve la ruta final donde se movió.
            else: # Es un archivo
                self.block_map[final_target_path] = self.block_map.pop(canonical_source)
                print(f"Archivo '{canonical_source}' movido a '{final_target_path}'.")
                return True, final_target_path # Devuelve la ruta final donde se movió.

    def login_user(self, username: str):
        with self.lock:
            current_time = time.time()
            if username in self.active_users:
                message = f"Usuario '{username}' ya estaba activo. Actualizando tiempo de login."
            else:
                message = f"Nuevo usuario '{username}' ha iniciado sesión."
            self.active_users[username] = current_time
            print(f"[NameNode] Alerta: {message} Usuarios activos: {len(self.active_users)}")
            return True, message

    def rm(self, file_path):
        with self.lock:
            canonical_path = self._canonical_dfs_path(file_path)
            
            if canonical_path not in self.block_map:
                raise Exception(f"El archivo o directorio '{canonical_path}' no existe.")
            
            # Check if it's a directory (represented by an empty list of blocks)
            if self.block_map[canonical_path] == []:
                raise Exception(f"'{canonical_path}' es un directorio. Use rmdir para eliminar directorios.")

            blocks_to_remove = self.block_map.pop(canonical_path, None)
            if blocks_to_remove is None: # Should be caught by 'not in self.block_map' already
                return

            # blocks_to_remove should be a list of block_ids for a file
            # If it was something else (e.g. somehow not a list, or not an empty list for dir), 
            # it implies an inconsistent state, but pop would have removed it.
            # The primary check is that it's not a directory (block_map[canonical_path] == [])

            for block_id in blocks_to_remove: # blocks_to_remove is list of block_ids
                if block_id in self.block_locations:
                    nodes_with_block = self.block_locations.pop(block_id, [])
                    for node_id in nodes_with_block:
                        if node_id in self.data_nodes and 'blocks' in self.data_nodes[node_id]:
                            self.data_nodes[node_id]['blocks'].discard(block_id)
            print(f"Archivo '{canonical_path}' y sus bloques asociados eliminados de los metadatos.")

    def get_file_content(self, file_path):
        # Simulación: solo retorna los bloques asignados
        with self.lock:
            canonical_path = self._canonical_dfs_path(file_path)
            return self.block_map.get(canonical_path, []) 

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