import os
import time
import hashlib
import threading
import multiprocessing
import json_util

lock = threading.Lock()

class Block:
    def __init__(self, index, data, timestamp, previousHash, difficulty=1, nonce=0):
        data = json_util.to_deterministic_json(data)
        self.index = index
        self.data = data
        self.timestamp = timestamp
        self.previousHash = previousHash
        self.difficulty = difficulty
        self.nonce = nonce
        self.hash = self.calculateHash()

    def calculateHash(self):
        blockString = f"{self.index}{self.data}{self.timestamp}{self.previousHash}{self.difficulty}{self.nonce}"
        return hashlib.sha256(blockString.encode()).hexdigest()

class Blockchain:
    def __init__(self):
        self.chain = [self.createFirstBlock()]
        self.blockGenerationInterval = 10  # seconds
        self.difficultyAdjustmentInterval = 10  # blocks

    def createFirstBlock(self):
        return Block(0, "First Block", time.time(), "0", difficulty=1, nonce=0)

    def getLatestBlock(self):
        return self.chain[-1]

    def mineBlock(self, data):
        previousBlock = self.getLatestBlock()
        newIndex = previousBlock.index + 1
        newTimestamp = time.time()
        newDifficulty = self.getDifficulty()
        block = Block(newIndex, data, newTimestamp, previousBlock.hash, newDifficulty)
        lastBlockIndex = self.getLatestBlock().index

        while not block.hash.startswith('0' * block.difficulty) and lastBlockIndex == self.getLatestBlock().index:
            block.nonce += 1
            block.hash = block.calculateHash()

        if lastBlockIndex != self.getLatestBlock().index:
            return False
        with lock:
            if self.isValidNewBlock(block, self.getLatestBlock()):
                self.chain.append(block)
                return block
            return False

    def getDifficulty(self):
        latestBlock = self.getLatestBlock()
        if latestBlock.index % self.difficultyAdjustmentInterval == 0 and latestBlock.index != 0:
            return self.getAdjustedDifficulty()
        return latestBlock.difficulty

    def getAdjustedDifficulty(self):
        latestBlock = self.getLatestBlock()
        prevAdjustmentBlock = self.chain[-self.difficultyAdjustmentInterval]

        expectedTime = self.blockGenerationInterval * self.difficultyAdjustmentInterval
        timeTaken = latestBlock.timestamp - prevAdjustmentBlock.timestamp

        if timeTaken < expectedTime / 2:
            return prevAdjustmentBlock.difficulty + 1
        elif timeTaken > expectedTime * 2:
            return max(1, prevAdjustmentBlock.difficulty - 1)
        return prevAdjustmentBlock.difficulty

    def isValidNewBlock(self, newBlock, previousBlock):
        if previousBlock.index + 1 != newBlock.index:
            return False
        if previousBlock.hash != newBlock.previousHash:
            return False
        if newBlock.calculateHash() != newBlock.hash:
            return False
        if not newBlock.hash.startswith('0' * newBlock.difficulty):
            return False
        if newBlock.timestamp > time.time() + 60:
            return False
        if newBlock.timestamp < previousBlock.timestamp - 60:
            return False
        return True

    def isValidChain(self, chainToValidate):
        if chainToValidate[0].previousHash != "0":
            return False

        for i in range(1, len(chainToValidate)):
            if not self.isValidNewBlock(chainToValidate[i], chainToValidate[i - 1]):
                return False
        return True

    def getComulativeDifficulty(self, chain):
        result = 0
        for block in chain:
            result += 2 ** block.difficulty
        return result

    def isBiggerCumulativeDifficulty(self, chainToCompare):
        return self.getComulativeDifficulty(self.chain) < self.getComulativeDifficulty(chainToCompare)

    def addBlock(self, block):
        with lock:
            latest_block = self.getLatestBlock()
            if self.isValidNewBlock(block, latest_block):
                self.chain.append(block)
                return True
            return False


    def mineBlockParallel(self, data):
        previousBlock = self.getLatestBlock()
        newIndex = previousBlock.index + 1
        newTimestamp = time.time()
        newDifficulty = self.getDifficulty()
        prevHash = previousBlock.hash

        #num_processes = os.cpu_count() or 1
        num_processes = 2

        args = [
            (newIndex, data, newTimestamp, prevHash, newDifficulty, i, num_processes)
            for i in range(num_processes)
        ]

        winning_nonce = None

        with multiprocessing.Pool(processes=num_processes) as pool:
            for res in pool.imap_unordered(_mine_worker, args, chunksize=1):
                if res is not None:
                    winning_nonce = res
                    pool.terminate()  #ustavimo ostale procese
                    break

        if winning_nonce is None:
            return False

        block = Block(newIndex, data, newTimestamp, prevHash, newDifficulty, winning_nonce)

        with lock:
            if self.isValidNewBlock(block, self.getLatestBlock()):
                self.chain.append(block)
                return block
        return False


def _mine_worker(args):
    newIndex, data, newTimestamp, prevHash, newDifficulty, worker_id, num_workers = args

    nonce = worker_id
    step = num_workers
    target = '0' * newDifficulty

    json_data = json_util.to_deterministic_json(data)
    while True:
        block_string = f"{newIndex}{json_data}{newTimestamp}{prevHash}{newDifficulty}{nonce}"
        hash_result = hashlib.sha256(block_string.encode()).hexdigest()

        if hash_result.startswith(target):
            return nonce
        nonce += step


def mineBlockParallel(args, stopFlag: threading.Event):
    json = json_util.from_json(args)
    prevHash = json["prevHash"]
    previousBlockIndex = json["previousBlockIndex"]
    data = json["data"]
    newIndex = previousBlockIndex + 1
    timestamp = json["timestamp"]
    difficulty = json["difficulty"]
    startingToken = json["startingToken"]

    num_processes = os.cpu_count() or 1

    args = [
        (newIndex, data, timestamp, prevHash, difficulty, startingToken + i, num_processes)
        for i in range(num_processes)
    ]

    winning_nonce = None

    with multiprocessing.Pool(processes=num_processes) as pool:
        result = pool.imap_unordered(_mine_worker, args, chunksize=1)

        while True:
            if stopFlag.is_set():
                pool.terminate()
                pool.join()
                return None

            try:
                res = result.next(timeout=0.1)
                if res is not None:
                    winning_nonce = res
                    pool.terminate()
                    pool.join()
                    break
            except StopIteration:
                break
            except multiprocessing.TimeoutError:
                continue

    if winning_nonce is None:
        return False

    block = Block(newIndex, data, timestamp, prevHash, difficulty, winning_nonce)
    return block
