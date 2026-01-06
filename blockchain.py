import time
import hashlib
import threading

lock = threading.Lock()

class Block:
    def __init__(self, index, data, timestamp, previousHash, difficulty=4, nonce=0):
        self.index = index
        self.data = data
        self.timestamp = timestamp
        self.previousHash = previousHash
        self.difficulty = difficulty
        self.nonce = nonce
        self.hash = self.calculateHash()

    def calculateHash(self):
        blockString = f"{self.index}{self.timestamp}{self.data}{self.previousHash}{self.difficulty}{self.nonce}"
        return hashlib.sha256(blockString.encode()).hexdigest()


class Blockchain:
    def __init__(self):
        self.chain = [self.createFirstBlock()]
        self.blockGenerationInterval = 10  # seconds
        self.difficultyAdjustmentInterval = 10  # blocks

    def createFirstBlock(self):
        return Block(0, "First Block", time.time(), "0")

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
            return prevAdjustmentBlock.difficulty - 1
        return prevAdjustmentBlock.difficulty

    def isValidNewBlock(self, newBlock, previousBlock):
        if previousBlock.index + 1 != newBlock.index:
            return False
        if previousBlock.hash != newBlock.previousHash:
            return False
        if newBlock.calculateHash() != newBlock.hash:
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

    def isLongerChain(self, chainToCompare):
        return len(self.chain) < len(chainToCompare)

    def getComulativeDifficulty(self, chain):
        result = 0
        for block in chain:
            result += 2 ** block.difficulty
        return result

    def isBiggerCumulativeDifficulty(self, chainToCompare):
        return self.getComulativeDifficulty(self.chain) < self.getComulativeDifficulty(chainToCompare)