from logging import exception

from mpi4py import MPI
import threading
import hashlib
import time
import os
import random
import queue
from blockchain import Blockchain
import json_util

lock = threading.Lock()


SENDING_THREADS_TAG = 0
DATA_TO_STORE_TAG = 1
NUM_THREADS_PER_WORKER = 4

def startNode():
    required = MPI.THREAD_MULTIPLE
    provided = MPI.Query_thread()
    if provided < required:
        print(f"Warning: MPI thread support level is {provided}, but {required} is required", flush=True)

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        mainNodeFunction(comm)
    else:
        workerNodeFunction(comm)

def mainNodeFunction(comm):

    storeQueue = queue.Queue()

    serveThread = threading.Thread(target=serveThreadFunction, args=(storeQueue,))
    storeThread = threading.Thread(target=storeThreadFunction, args=(storeQueue, comm))

    serveThread.start()
    storeThread.start()

    serveThread.join()
    storeThread.join()


def serveThreadFunction(storeQueue: queue.Queue):

    while True:
        time.sleep(random.uniform(1, 2))
        random_string = 'random number: ' + str(random.randint(1, 100))
        storeQueue.put(random_string)


def storeThreadFunction(storeQueue: queue.Queue, comm):

    numWorkers = comm.Get_size()
    worker_threads = {}

    for workerRank in range(1, comm.Get_size()):
        num_threads = comm.recv(source=workerRank, tag=SENDING_THREADS_TAG)
        worker_threads[workerRank] = num_threads
        print(f"Main node: Worker {workerRank} will run {num_threads} threads", flush=True)

    totalThreads = sum(worker_threads.values())
    step = totalThreads

    while True:
        try:
            data = storeQueue.get(timeout=5)
            startingToken = 0
            for workerRank in range(1, numWorkers):
                comm.send(f"starting token {startingToken} step {step} data {data}", dest=workerRank, tag=DATA_TO_STORE_TAG)
                startingToken += worker_threads[workerRank]
        except:
            continue



def workerNodeFunction(comm):
    lock = threading.Lock()
    rank = comm.Get_rank()
    num_threads = os.cpu_count() or 1
    comm.send(num_threads, dest=0, tag=SENDING_THREADS_TAG)

    stop_mining = threading.Event()
    mined_block_queue = queue.Queue()

    # Pass queue to listener so it can put received data in it
    listener = threading.Thread(target=workerListenerThread, args=(comm, rank, stop_mining, mined_block_queue, lock))
    listener.daemon = True
    listener.start()

    miner = threading.Thread(target=workerMinerThread, args=(comm, rank, stop_mining, mined_block_queue, lock))
    miner.start()

    miner.join()
    listener.join()


def workerListenerThread(comm, rank, stop_mining, mined_block_queue, threadingLock):

    while True:
        data = comm.recv(source=0, tag=DATA_TO_STORE_TAG)

        # Stop current mining and queue new data
        stop_mining.set()
        mined_block_queue.empty()
        mined_block_queue.put(data)
        stop_mining.clear()


def workerMinerThread(comm, rank, stop_mining, mined_block_queue, threadingLock):
    """Mine blocks using data from the queue"""
    bc = Blockchain()

    while True:
        # Wait for data to mine
        try:
            data = mined_block_queue.get(timeout=1)
        except queue.Empty:
            time.sleep(0.1)
            continue

        print(f"Worker {rank} starting to mine with data: {data}", flush=True)

        # Parse the data string (format: "starting token X step Y data Z")
        # Or just use the whole string as block data


