import sys
import os
import argparse
import subprocess
import time

# Calculate the project root directory (Proyecto-DFS)
# This script is in .../Proyecto-DFS/scripts/
# Project root is one level up.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.core.datanode import DataNode

def start_datanode_process(node_id, port, namenode_addr, storage_base_dir):
    """Inicia un proceso DataNode."""
    storage_dir = os.path.join(storage_base_dir, node_id)
    os.makedirs(storage_dir, exist_ok=True)
    
    # Comando para ejecutar un DataNode individualmente (como si fuera un script)
    # Esto requiere que DataNode pueda ser invocado de esta manera o tener un script wrapper.
    # Por ahora, vamos a instanciarlo y correrlo directamente en un nuevo proceso Python.
    # Esto es más complejo de manejar entre procesos que un script dedicado por DataNode.
    
    # Para simplificar, este script lanzará otros scripts de Python que inician un DataNode.
    # Creamos un script temporal o usamos uno existente si DataNode es directamente ejecutable.
    
    # Dado que DataNode no es un script ejecutable directamente con `python -m src.core.datanode`
    # y no queremos crear scripts temporales, usaremos subprocess para llamar a python
    # y ejecutar un pequeño bootstrap que importe y corra DataNode.
    # Esto es un poco un hack. Idealmente, tendrías un `src/core/datanode_runner.py` o similar.

    cmd = [
        sys.executable, # Path al interprete de Python actual
        "-c", 
        f"import sys; sys.path.insert(0, r'{PROJECT_ROOT}'); import protos.dfs_pb2_grpc as dfs_pb2_grpc; import protos.dfs_pb2 as dfs_pb2; from src.core.datanode import DataNode; dn = DataNode(node_id='{node_id}', grpc_port={port}, namenode_host='{namenode_addr}', storage_dir=r'{storage_dir}'); dn.start()"
    ]
    
    print(f"Iniciando DataNode {node_id} en puerto {port} con almacenamiento en {storage_dir}...")
    # Para Windows, es mejor no usar Popen con start_new_session si quieres que las ventanas de consola aparezcan
    # o si quieres gestionarlos más fácilmente. subprocess.CREATE_NEW_CONSOLE abre una nueva ventana.
    process = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
    print(f"DataNode {node_id} iniciado con PID: {process.pid}")
    with open("datanode_pids.txt", "a") as f:
        f.write(str(process.pid) + "\n")
    return process

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iniciar múltiples DataNodes DFS.")
    parser.add_argument("-n", "--num_datanodes", type=int, required=True, help="Número de DataNodes a iniciar.")
    parser.add_argument("--start_port", type=int, default=50053, help="Puerto gRPC inicial para los DataNodes.")
    parser.add_argument("--namenode", type=str, default="localhost:50052", help="Dirección del NameNode (host:puerto gRPC).")
    parser.add_argument("--storage_base", type=str, default="c:\\Users\\Camilo\\dfs_storage", help="Directorio base para el almacenamiento de los DataNodes.")

    args = parser.parse_args()

    if args.num_datanodes <= 0:
        print("El número de DataNodes debe ser positivo.")
        sys.exit(1)

    print(f"Iniciando {args.num_datanodes} DataNode(s)...")
    print(f"NameNode en: {args.namenode}")
    print(f"Directorio base de almacenamiento: {args.storage_base}")
    print(f"Puertos comenzando desde: {args.start_port}")

    processes = []
    for i in range(args.num_datanodes):
        node_id = f"datanode{i+1}"
        port = args.start_port + i
        # Asegurarse que el puerto del namenode no colisione con los datanodes
        if port == int(args.namenode.split(':')[-1]):
            print(f"Advertencia: El puerto {port} para {node_id} podría colisionar con el puerto del NameNode. Incrementando puerto para este DataNode.")
            port += 1 # Simple ajuste, podría necesitar lógica más robusta
            # También se debería re-chequear colisiones con otros datanodes después de este ajuste.

        p = start_datanode_process(node_id, port, args.namenode, args.storage_base)
        processes.append(p)
        time.sleep(1) # Dar un pequeño respiro entre inicios

    print(f"\n{args.num_datanodes} DataNode(s) iniciados.")
    print("Presiona Ctrl+C en esta ventana para terminar los procesos DataNode.")

    try:
        while True:
            time.sleep(5)
            # Aquí podrías añadir lógica para monitorizar los procesos si es necesario
            # Por ejemplo, verificar si alguno ha terminado inesperadamente.
            all_running = True
            for i, p in enumerate(processes):
                if p.poll() is not None: # El proceso ha terminado
                    print(f"El proceso DataNode {i+1} (PID: {p.pid}) ha terminado con código {p.returncode}.")
                    all_running = False
            if not all_running:
                print("Uno o más procesos DataNodes han terminado inesperadamente.")
                break # Salir del bucle si un DataNode termina inesperadamente

    except KeyboardInterrupt:
        print("\nSeñal de interrupción recibida. Intentando detener DataNodes...")
        # Llamar al script stop_datanodes.py para manejar la terminación
        try:
            subprocess.run([sys.executable, os.path.join(PROJECT_ROOT, "scripts", "stop_datanodes.py")], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error al ejecutar stop_datanodes.py: {e}")
        except FileNotFoundError:
            print("Error: stop_datanodes.py no encontrado. Asegúrate de que el path sea correcto.")
        print("Proceso de terminación de DataNodes iniciado.")

    finally:
        # Asegurarse de limpiar el archivo de PIDs si el script termina por cualquier otra razón
        if os.path.exists("datanode_pids.txt"):
            os.remove("datanode_pids.txt")
            print("Archivo de PIDs limpiado al finalizar run_datanodes.py.")