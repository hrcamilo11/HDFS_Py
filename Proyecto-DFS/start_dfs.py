import subprocess
import os
import sys
import time

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(script_dir)

# Paths to the server scripts
NAMENODE_SERVER_PATH = os.path.join(project_root, 'src', 'core', 'namenode_grpc_server.py')
DATANODES_SCRIPT_PATH = os.path.join(project_root, 'scripts', 'run_datanodes.py')

def start_server(script_path, name, args=None):
    if args is None:
        args = []
    print(f"Starting {name} server...")
    # Use sys.executable to ensure the correct Python interpreter is used
    command = [sys.executable, script_path] + args
    process = subprocess.Popen(command, 
                               stdout=sys.stdout,
                               stderr=sys.stderr)
    print(f"{name} server started with PID: {process.pid}")
    return process

if __name__ == "__main__":
    namenode_process = None
    datanodes_process = None
    namenode_process = None
    datanodes_process = None

    try:
        # Start NameNode
        namenode_process = start_server(NAMENODE_SERVER_PATH, "NameNode")
        time.sleep(3) # Give NameNode a moment to start (increased to 3 seconds as requested)

        # Ask user for number of DataNodes
        num_datanodes = 0
        while num_datanodes <= 0:
            try:
                num_datanodes_str = input("¿Cuántos DataNodes desea crear? (Ingrese un número entero positivo): ")
                num_datanodes = int(num_datanodes_str)
                if num_datanodes <= 0:
                    print("Por favor, ingrese un número entero positivo.")
            except ValueError:
                print("Entrada inválida. Por favor, ingrese un número entero.")

        # Start DataNodes with the specified number
        datanodes_process = start_server(DATANODES_SCRIPT_PATH, "DataNodes", args=["-n", str(num_datanodes)])

        print("DFS servers are running. Press Ctrl+C to stop them.")
        
        # Keep the main script running to keep subprocesses alive
        # Wait for both processes to terminate
        while namenode_process.poll() is None or datanodes_process.poll() is None:
            time.sleep(1) # Wait a bit before checking again

        if namenode_process.poll() is not None:
            print(f"NameNode process exited with code {namenode_process.returncode}")
        if datanodes_process.poll() is not None:
            print(f"DataNodes process exited with code {datanodes_process.returncode}")

    except KeyboardInterrupt:
        print("\nStopping DFS servers...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Detener DataNodes usando el script dedicado
        try:
            subprocess.run([sys.executable, os.path.join(project_root, "scripts", "stop_datanodes.py")], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error al ejecutar stop_datanodes.py: {e}")
        except FileNotFoundError:
            print("Error: stop_datanodes.py no encontrado. Asegúrate de que el path sea correcto.")

        if namenode_process and namenode_process.poll() is None:
            namenode_process.terminate()
            namenode_process.wait(timeout=5)
            if namenode_process.poll() is None:
                namenode_process.kill()
            print("NameNode stopped.")
        
        if datanodes_process and datanodes_process.poll() is None:
            datanodes_process.terminate()
            datanodes_process.wait(timeout=5)
            if datanodes_process.poll() is None:
                datanodes_process.kill()
            print("DataNodes stopped.")
        print("All DFS servers stopped.")