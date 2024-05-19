import os
import subprocess
import threading


def run_all():
    root_dir = os.getcwd()
    def run_script(script_name):
        subprocess.run(["python", script_name])

    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith("Run.py") and os.path.join(root_dir, filename) != __file__:
                filepath = os.path.join(dirpath, filename)
                script_thread = threading.Thread(target=run_script, args=(filename,))
                script_thread.start()
                print(f"Running {filename}")
            

    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith("Run.py") and os.path.join(root_dir, filename) != __file__:
                filepath = os.path.join(dirpath, filename)
                script_thread = threading.Thread(target=run_script, args=(filename,))
                script_thread.join()
                print(f"Joining {filename}")      


run_all()

