import os
import re
import subprocess

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROTOS_DIR = os.path.join(PROJECT_ROOT, 'protos')

def run_protoc():
    print("Running protoc to recompile .proto files...")
    command = [
        "python",
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTOS_DIR}",
        f"--python_out={PROTOS_DIR}",
        f"--grpc_python_out={PROTOS_DIR}",
        os.path.join(PROTOS_DIR, "namenode.proto"),
        os.path.join(PROTOS_DIR, "dfs.proto")
    ]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print("Protoc output:", result.stdout)
        print("Protoc finished successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running protoc: {e}")
        print("Stderr:", e.stderr)
        exit(1)

def fix_imports(file_path):
    print(f"Fixing imports in {file_path}...")
    with open(file_path, 'r') as f:
        content = f.read()

    # Fix namenode_pb2 import
    content = re.sub(r"^import (namenode_pb2) as (namenode__pb2)$", r"from protos import \1 as \2", content, flags=re.MULTILINE)
    # Fix dfs_pb2 import
    content = re.sub(r"^import (dfs_pb2) as (dfs__pb2)$", r"from protos import \1 as \2", content, flags=re.MULTILINE)

    with open(file_path, 'w') as f:
        f.write(content)
    print(f"Imports fixed in {file_path}.")

if __name__ == "__main__":
    run_protoc()
    fix_imports(os.path.join(PROTOS_DIR, 'namenode_pb2_grpc.py'))
    fix_imports(os.path.join(PROTOS_DIR, 'dfs_pb2_grpc.py'))
    print("All proto imports fixed.")