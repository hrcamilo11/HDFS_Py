import os
import sys
import subprocess
import time

PID_FILE = "datanode_pids.txt"

def stop_datanodes():
    if not os.path.exists(PID_FILE):
        print(f"No se encontró el archivo de PIDs: {PID_FILE}")
        return

    with open(PID_FILE, "r") as f:
        pids = [int(line.strip()) for line in f if line.strip()]

    if not pids:
        print("No hay PIDs de DataNodes registrados para detener.")
        return

    print(f"Intentando detener {len(pids)} DataNode(s)...")
    for pid in pids:
        try:
            if sys.platform == "win32":
                # En Windows, usar taskkill para cerrar la ventana de consola
                try:
                    # Verificar si el proceso aún está corriendo antes de intentar terminarlo
                    subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    print(f"DataNode PID: {pid} terminado con taskkill.")
                    time.sleep(2) 
                except subprocess.CalledProcessError as e:
                    if e.returncode == 128: # Proceso no encontrado
                        print(f"DataNode PID: {pid} ya no está corriendo.")
                        time.sleep(2) 
                    else:
                        print(f"Error al usar taskkill para PID {pid}: {e.stderr.decode().strip()}")
                        time.sleep(2)
                except FileNotFoundError:
                    print("taskkill o tasklist no encontrado. Asegúrate de que estén en tu PATH.")
                    time.sleep(2) 
            else:
                # Para otros sistemas operativos, usar terminate/kill estándar
                try:
                    os.kill(pid, 15) # SIGTERM
                    time.sleep(2) # Dar tiempo para que termine
                    try:
                        os.kill(pid, 9) # SIGKILL si sigue vivo
                        print(f"DataNode PID: {pid} forzado a terminar.")
                    except OSError:
                        pass # Ya terminó
                    print(f"DataNode PID: {pid} terminado.")
                    time.sleep(2)
                except OSError as e:
                    print(f"Error al intentar detener el DataNode PID {pid}: {e}")
        except Exception as e:
            print(f"Error al intentar detener el DataNode PID {pid}: {e}")

    # Limpiar el archivo de PIDs después de intentar detenerlos
    try:
        os.remove(PID_FILE)
        print("Archivo de PIDs limpiado.")
        time.sleep(3) 
    except FileNotFoundError:
        print(f"El archivo de PIDs {PID_FILE} ya no existe o fue eliminado previamente.")

if __name__ == "__main__":
    time.sleep(1) 
    stop_datanodes()