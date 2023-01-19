import time

import zstandard
import lz4.frame

def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


data = open("./datasets/data.store", mode="rb").read()
data2 = open("./datasets/data.json", mode="rb").read()


for _ in range(3):
    start = time.perf_counter()
    total = 0
    block_size = 0
    count = 0
    for chunk in divide_chunks(data, 512 << 10):
        block_size += len(chunk)
        count += 1
        compressed_data = zstandard.compress(chunk, level=1)
        total += len(compressed_data)
        zstandard.decompress(compressed_data)
    stop = time.perf_counter() - start
    pct_reduction = 100 + (((total - len(data)) / len(data)) * 100)
    print(f"Took: {stop}s {pct_reduction:.2f}% Original, {total}B vs {len(data)}B, Avg Block Size: {block_size / count:.2f}B")


for _ in range(3):
    start = time.perf_counter()
    total = 0
    block_size = 0
    count = 0
    for chunk in divide_chunks(data2, 512 << 10):
        block_size += len(chunk)
        count += 1
        compressed_data = zstandard.compress(chunk, level=1)
        total += len(compressed_data)
        zstandard.decompress(compressed_data)
    stop = time.perf_counter() - start
    pct_reduction = 100 + (((total - len(data)) / len(data)) * 100)
    print(f"Took: {stop}s {pct_reduction:.2f}% Original, {total}B vs {len(data)}B, Avg Block Size: {block_size / count:.2f}B")
