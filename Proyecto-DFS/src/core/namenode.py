import time
import threading
import random
import os

class NameNode:
    def __init__(self, replication_factor=2, block_size_mb=64):
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
        num_blocks = (file_size + self.block_size_mb * 1024 * 1024 - 1) // (self.block_size_mb * 1024 * 1024)
        block_ids = [f"block_{int(time.time()*1000)}_{i}_{random.randint(0,9999)}" for i in range(num_blocks)]
        # Asignar bloques a DataNodes con menos carga
        node_ids = list(self.data_nodes.keys())
        if not node_ids:
            raise Exception("No hay DataNodes registrados")
        for block_id in block_ids:
            # Selección round-robin + aleatorio para balanceo
            selected_nodes = sorted(node_ids, key=lambda n: len(self.data_nodes[n]['blocks']))[:self.replication_factor]
            self.block_locations[block_id] = selected_nodes
            for n in selected_nodes:
                self.data_nodes[n]['blocks'].add(block_id)
        return block_ids

    def get_block_locations(self, block_id):
        return self.block_locations.get(block_id, [])

    def get_file_blocks(self, file_path):
        return self.block_map.get(file_path, [])

    def add_file(self, file_path, block_ids):
        self.block_map[file_path] = block_ids 

    def mkdir(self, dir_path):
        if dir_path in self.block_map:
            raise Exception("El directorio ya existe")
        self.block_map[dir_path] = []

    def rmdir(self, dir_path):
        if dir_path not in self.block_map or self.block_map[dir_path]:
            raise Exception("El directorio no existe o no está vacío")
        del self.block_map[dir_path]

    def ls(self, dir_path):
        # Listar archivos y directorios bajo dir_path
        return [k for k in self.block_map if os.path.dirname(k) == dir_path]

    def rm(self, file_path):
        if file_path not in self.block_map:
            raise Exception("Archivo no existe")
        del self.block_map[file_path]

    def get_file_content(self, file_path):
        # Simulación: solo retorna los bloques asignados
        return self.block_map.get(file_path, []) 

    def check_and_rereplicate(self):
        # Detectar DataNodes inactivos y re-replicar bloques
        import time
        now = time.time()
        inactive_nodes = [n for n, v in self.data_nodes.items() if now - v['last_heartbeat'] > 15]
        for node_id in inactive_nodes:
            print(f"DataNode inactivo detectado: {node_id}")
            # Re-replicar bloques de este nodo
            for block_id in list(self.data_nodes[node_id]['blocks']):
                # Buscar nodos activos para replicar
                active_nodes = [n for n in self.data_nodes if n not in inactive_nodes]
                if not active_nodes:
                    print("No hay DataNodes activos para re-replicar el bloque", block_id)
                    continue
                # Seleccionar un nodo activo con menos bloques
                target_node = min(active_nodes, key=lambda n: len(self.data_nodes[n]['blocks']))
                self.block_locations[block_id].append(target_node)
                self.data_nodes[target_node]['blocks'].add(block_id)
                print(f"Re-replicando bloque {block_id} a {target_node}")
            del self.data_nodes[node_id] 