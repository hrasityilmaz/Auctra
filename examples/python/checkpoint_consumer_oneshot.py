import os
from commit_engine import CommitEngine

CHECKPOINT_PATH = "/tmp/orders-consumer.ckpt"

def load_or_capture(engine: CommitEngine):
    if os.path.exists(CHECKPOINT_PATH):
        return engine.load_checkpoint(CHECKPOINT_PATH)
    return engine.capture_checkpoint(True)

def main():
    engine = CommitEngine(shard_count=4, inline_max=16)
    cp = load_or_capture(engine)

    items, next_cp = engine.read_from_checkpoint(cp, limit=128)

    print("items =", len(items))
    for item in items:
        print(item.seqno, item.key, item.value)

    print("next checkpoint cursors =", next_cp.cursors)

if __name__ == "__main__":
    main()