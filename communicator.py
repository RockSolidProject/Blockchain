import json
from logging import exception
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from flask import Flask, jsonify

from mpi4py import MPI
import threading
import time
import os
import queue

from main import print_block

import blockchain
from blockchain import Blockchain

load_dotenv()
def get_mqtt_config():
    broker = os.getenv("MQTT_BROKER")
    port = int(os.getenv("MQTT_PORT", "8883"))
    username = os.getenv("MQTT_USERNAME")
    password = os.getenv("MQTT_PASSWORD")
    topic = os.getenv("MQTT_TOPIC", "messages")
    if not broker or not username or not password:
        raise RuntimeError("Missing MQTT config in environment (.env)")
    return broker, port, username, password, topic
MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD, MQTT_TOPIC = get_mqtt_config()

lock = threading.Lock()


SENDING_THREADS_TAG = 0
DATA_TO_STORE_TAG = 1
BLOCK_RECEIVE_TAG = 2
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
    blockchain = Blockchain()
    chainLock = threading.Lock()

    serveThread = threading.Thread(target=serveThreadFunction, args=(storeQueue, chainLock, blockchain))
    storeThread = threading.Thread(target=storeThreadFunction, args=(storeQueue,chainLock, blockchain, comm))

    serveThread.start()
    storeThread.start()

    serveThread.join()
    storeThread.join()


def serveThreadFunction(storeQueue: queue.Queue, chainLock: threading.Lock, blockchain: blockchain.Blockchain):
    app = Flask(__name__)

    @app.route('/')
    def index():
        blocks_json = ""
        with chainLock:
            for block in blockchain.chain:
                if blocks_json == "":
                    blocks_json = "[" + block.data
                else:
                    blocks_json = f"{blocks_json},{block.data}"
        return blocks_json+"]"

    @app.route('/hello')
    def hello():
        return "Hello World!"

    def run_flask():
        app.run(host='0.0.0.0', port=5001, debug=False, use_reloader=False)

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("MQTT: Connected successfully!", flush=True)
            client.subscribe(MQTT_TOPIC)
            print(f"MQTT: Subscribed to {MQTT_TOPIC}", flush=True)
        else:
            print(f"MQTT: Connection failed with code {rc}", flush=True)

    def on_message(client, userdata, msg):
        payload = msg.payload.decode()
        print(f"MQTT: Received message: {payload} on topic {msg.topic}", flush=True)
        storeQueue.put(payload)

    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.tls_set()

    print(f"MQTT: Connecting to {MQTT_BROKER}:{MQTT_PORT} ...", flush=True)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("MQTT: Disconnecting...", flush=True)
    finally:
        client.loop_stop()
        client.disconnect()


def storeThreadFunction(storeQueue: queue.Queue,chainLock: threading.Lock,blockchain: blockchain.Blockchain, comm):

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
            data = storeQueue.get(timeout = 0.1)

            startingToken = 0
            with chainLock:
                latestBlock = blockchain.getLatestBlock()

            for workerRank in range(1, numWorkers):
                dataToStore = {
                    "prevHash": latestBlock.hash or "0",
                    "previousBlockIndex": latestBlock.index,
                    "data": data,
                    "timestamp": time.time(),
                    "difficulty": blockchain.getDifficulty(),
                    "step": step,
                    "startingToken": startingToken
                }
                startingToken = startingToken + worker_threads[workerRank]
                comm.send(json.dumps(dataToStore), dest=workerRank, tag=DATA_TO_STORE_TAG)
                startingToken += worker_threads[workerRank]
            # must check if the block is received from any worker

            while True:
                block = comm.recv(source=MPI.ANY_SOURCE, tag=BLOCK_RECEIVE_TAG)

                with chainLock:
                    if not blockchain.addBlock(block):
                        # print(f"Main node: Received invalid block from worker", flush=True)
                        continue
                    else:
                        print_block(block)

                break


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

        # print(f"Worker {rank} starting to mine with data: {data}", flush=True)
        block = blockchain.mineBlockParallel(
            data,
            stop_mining
        )
        if(block is None):
            continue

        comm.send(block, dest=0, tag=BLOCK_RECEIVE_TAG)
