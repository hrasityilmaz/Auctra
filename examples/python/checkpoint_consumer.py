import os
import time

from auctra_core import CommitEngine

CHECKPOINT_PATH = "/tmp/orders-consumer.ckpt"

def load_or_capture(engine: CommitEngine):
    if os.path.exists(CHECKPOINT_PATH):
        return engine.load_checkpoint(CHECKPOINT_PATH)
    return engine.capture_checkpoint(True)

def main():
    engine = CommitEngine(shard_count=4, inline_max=16)
    cp = load_or_capture(engine)

    while True:
        items, next_cp = engine.read_from_checkpoint(cp, limit=128)

        if not items:
            time.sleep(0.2)
            continue

        for item in items:
            print(item.seqno, item.key, item.value)

        tmp = CHECKPOINT_PATH + ".tmp"
        engine.save_checkpoint_data(tmp, next_cp)
        os.replace(tmp, CHECKPOINT_PATH)

        cp = next_cp

if __name__ == "__main__":
    main()