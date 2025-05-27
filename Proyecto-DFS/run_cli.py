import os
import subprocess
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

def run_cli():
    print("Starting HDFS client CLI...")
    # Add the project root to PYTHONPATH to ensure all modules are found
    env = os.environ.copy()
    if 'PYTHONPATH' in env:
        env['PYTHONPATH'] = f"{PROJECT_ROOT};{env['PYTHONPATH']}"
    else:
        env['PYTHONPATH'] = PROJECT_ROOT

    # Command to run the CLI application
    # Assuming the main CLI entry point is in src/client/cli.py
    command = [sys.executable, os.path.join(PROJECT_ROOT, 'src', 'client', 'cli.py')]

    try:
        subprocess.run(command, env=env, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running CLI: {e}")
        print("Stderr:", e.stderr)
    except FileNotFoundError:
        print("Error: Python executable not found. Make sure Python is installed and in your PATH.")

if __name__ == "__main__":
    run_cli()