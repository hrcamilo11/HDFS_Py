import time
import threading
import random
import os

class NameNode:
    def __init__(self, replication_factor=3, block_size_mb=64):
        self.block_map = {}  # {file_path: [block_ids]}
        self.block_locations = {}  # {block_id: [node_id]}
        self.data_nodes = {} # {node_id: {'last_heartbeat': timestamp, 'blocks': set()}}
        self.replication_factor = replication_factor
        self.block_size_mb = block_size_mb
        self.lock = threading.Lock()

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
        with self.lock: # Acceso seguro a block_locations
            return self.block_locations.get(block_id, [])

    def get_file_blocks(self, file_path):
        with self.lock: # Acceso seguro a block_map
            return self.block_map.get(file_path, [])

    def add_file(self, file_path, block_ids):
        with self.lock: # Acceso seguro a block_map
            self.block_map[file_path] = block_ids 

    def mkdir(self, dir_path):
        with self.lock:
            normalized_path = os.path.normpath(dir_path)
            if normalized_path in self.block_map:
                raise Exception(f"El directorio o archivo '{normalized_path}' ya existe.")
            self.block_map[normalized_path] = [] # Representa un directorio vacío

    def rmdir(self, dir_path):
        with self.lock:
            normalized_path = os.path.normpath(dir_path)
            if normalized_path not in self.block_map:
                 raise Exception(f"El directorio '{normalized_path}' no existe.")
            if self.block_map[normalized_path] != []: # No es un directorio o no está vacío según nuestra convención
                raise Exception(f"La ruta '{normalized_path}' no es un directorio vacío (puede ser un archivo o un directorio con contenido de bloques).")
            
            # Verificar si hay elementos dentro de este directorio
            # Un directorio se considera vacío si no hay claves en block_map que comiencen con dir_path + separador
            prefix_to_check = normalized_path + os.sep
            if normalized_path == "." or normalized_path == "/": # Manejo especial para raíz
                prefix_to_check = os.sep if normalized_path == "/" else ""

            children = [
                item for item in self.block_map.keys() 
                if item.startswith(prefix_to_check) and item != normalized_path # Excluirse a sí mismo
            ]
            if children:
                raise Exception(f"El directorio '{normalized_path}' no está vacío. Contiene: {children}")
            del self.block_map[normalized_path]

    def ls(self, dir_path):
        with self.lock:
            normalized_query_path = os.path.normpath(dir_path)
            if normalized_query_path == ".": # ls en el directorio actual (raíz para block_map)
                normalized_query_path = ""
            
            results = []
            for item_path in self.block_map.keys():
                # Normalizar item_path para comparación
                normalized_item_path = os.path.normpath(item_path)
                
                # Calcular el directorio padre del item
                parent_dir_of_item = os.path.dirname(normalized_item_path)
                if parent_dir_of_item == normalized_query_path:
                    results.append(os.path.basename(normalized_item_path))
            
            return sorted(list(set(results)))


    def rm(self, file_path):
        with self.lock:
            normalized_path = os.path.normpath(file_path)
            if normalized_path not in self.block_map: 
                raise Exception(f"El archivo o directorio '{normalized_path}' no existe.")
            
            # Si es un directorio (representado por una lista vacía de bloques), usar rmdir
            if isinstance(self.block_map[normalized_path], list) and not self.block_map[normalized_path]:
                # Podríamos llamar a self.rmdir aquí, pero rmdir tiene sus propias verificaciones de vacío.
                # Para rm, si es un directorio, simplemente se niega la operación o se redirige.
                raise Exception(f"'{normalized_path}' es un directorio. Use rmdir para eliminar directorios.")

            blocks_to_remove = self.block_map.pop(normalized_path, None) # pop devuelve la lista de block_ids o None
            if blocks_to_remove is None: # Ya fue eliminado o no existía (doble chequeo)
                return

            if not isinstance(blocks_to_remove, list): # No era un archivo con bloques
                print(f"Advertencia: '{normalized_path}' no parece ser un archivo con bloques en block_map.")
                return

            for block_id in blocks_to_remove:
                if block_id in self.block_locations:
                    nodes_with_block = self.block_locations.pop(block_id, [])
                    for node_id in nodes_with_block:
                        if node_id in self.data_nodes and 'blocks' in self.data_nodes[node_id]:
                            self.data_nodes[node_id]['blocks'].discard(block_id)
            print(f"Archivo '{normalized_path}' y sus bloques asociados eliminados de los metadatos.")

    def get_file_content(self, file_path):
        # Simulación: solo retorna los bloques asignados
        with self.lock:
            normalized_path = os.path.normpath(file_path)
            return self.block_map.get(normalized_path, []) 

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