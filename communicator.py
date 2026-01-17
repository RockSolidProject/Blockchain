

from mpi4py import MPI
import multiprocessing
import hashlib
import time
import os

import random



def startNode():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        mainNodeFunction(comm)
    else :
        workerNodeFunction(comm)

def mainNodeFunction(comm):
    size = comm.Get_size()
    print(f"Main node started with {size - 1} worker nodes.", flush=True)

    storeQueue = multiprocessing.Queue()

    serveThread = multiprocessing.Process(target=serveThreadFunction, args=(storeQueue,))
    storeThread = multiprocessing.Process(target=storeThreadFunction, args=(storeQueue, size))

    serveThread.start()
    storeThread.start()

    serveThread.join()
    storeThread.join()

def serveThreadFunction(storeQueue: multiprocessing.Queue):
    while True:
        time.sleep(random.uniform(1, 3))
        random_string = 'random number: ' + str(random.randint(1, 100))
        storeQueue.put(random_string)
        print(f"ServeProcess: Added '{random_string}' to queue (size: {storeQueue.qsize()})", flush=True)

def workerNodeFunction(comm):
    rank = comm.Get_rank()
    data = comm.recv(source=0, tag=0)
    print(f"Worker node {rank} started and received data: {data}", flush=True)

def storeThreadFunction(storeQueue: multiprocessing.Queue, comm):
    while True:
        try:
            data = storeQueue.get(timeout=0.1)
            print(f"StoreProcess: Retrieved '{data}' from queue (size: {storeQueue.qsize()})", flush=True)

            for worker_rank in range(1, comm.Get_size()):
                comm.send(data + f" from store process", dest=worker_rank, tag=0)

        except:
            continue




