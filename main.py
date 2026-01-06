import time
from blockchain import Blockchain
import json_util

def print_block(b):
    print(f"Index: {b.index}, Timestamp: {b.timestamp:.3f}, Data: {json_util.from_json(b.data)}, "
          f"PrevHash: {b.previousHash[:8]}, Hash: {b.hash[:8]}, "
          f"Difficulty: {b.difficulty}, Nonce: {b.nonce}")

def main():
    bc = Blockchain()
    print("Genesis block:")
    print_block(bc.getLatestBlock())

    target_height = 80
    block_count = 0

    try:
        while bc.getLatestBlock().index < target_height:
            start = time.time()
            block = bc.mineBlockParallel({
                "block_num": block_count + 1,
                "type": "danger",
                "message": "Climbing hold rotated on south wall",
                "location": "Neki pac",
                "timestamp": "cas"
            })
            elapsed = time.time() - start

            if not block:
                print("mineBlockParallel returned False (no block mined).")
                break

            print("\nMined block:")
            print_block(block)
            print(f"Mined in {elapsed:.3f}s")
            block_count += 1

    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    print("\nFull chain:")
    for b in bc.chain:
        print_block(b)

    print("\nChain valid:", bc.isValidChain(bc.chain))
    print("Cumulative difficulty:", bc.getComulativeDifficulty(bc.chain))

if __name__ == "__main__":
    main()
