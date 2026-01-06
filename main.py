from blockchain import Blockchain
import time

def print_block(b):
    print(f"Index: {b.index}, Timestamp: {b.timestamp:.3f}, Data: {b.data}, "
          f"PrevHash: {b.previousHash[:8]}, Hash: {b.hash[:8]}, "
          f"Difficulty: {b.difficulty}, Nonce: {b.nonce}")

def main():
    bc = Blockchain()
    print("Genesis block:")
    print_block(bc.getLatestBlock())

    for i in range(1, 50):
        print(f"\nMining block {i}...")
        result = bc.mineBlock(f"Block {i} data")
        if not result:
            print("Mining aborted or failed (another chain update).")
        else:
            print("Mined block:")
            print_block(result)
        time.sleep(0.1)

    print("\nFull chain:")
    for b in bc.chain:
        print_block(b)

    print("\nChain valid:", bc.isValidChain(bc.chain))
    print("Cumulative difficulty:", bc.getComulativeDifficulty(bc.chain))

if __name__ == "__main__":
    main()
