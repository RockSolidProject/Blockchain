import sys
import subprocess

if len(sys.argv) < 2:
    print("Usage: python run.py <n>")
    sys.exit(1)

n = sys.argv[1]

cmd = ["mpiexec", "-n", n, "python3", "testMpi.py"]

try:
    subprocess.run(cmd, check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running MPI program: {e}")
