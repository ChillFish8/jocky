import time

import zstandard

def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

with open("../datasets/data.json", encoding="utf-8") as file:
    samples = [line.encode() for line in file]

data = open("../datasets/data.json", mode="rb").read()

start = time.perf_counter()
total = 0
block_size = 0
count = 0
for chunk in divide_chunks(samples, 6_000):
    chunk = b"\n".join(chunk)
    block_size += len(chunk)
    count += 1
    total += len(zstandard.compress(chunk, level=1))
stop = time.perf_counter() - start
print(f"Took: {stop}s {total}B Avg Block Size: {block_size / count:.2f}B")

start = time.perf_counter()
total = 0
block_size = 0
count = 0
for chunk in divide_chunks(samples, 6_000):
    chunk = b"\n".join(chunk)
    block_size += len(chunk)
    count += 1
    total += len(zstandard.compress(chunk, level=3))
stop = time.perf_counter() - start
print(f"Took: {stop}s {total}B Avg Block Size: {block_size / count:.2f}B")

start = time.perf_counter()
total = 0
block_size = 0
count = 0
for chunk in divide_chunks(samples, 6_000):
    chunk = b"\n".join(chunk)
    block_size += len(chunk)
    count += 1
    total += len(zstandard.compress(chunk, level=-3))
stop = time.perf_counter() - start
print(f"Took: {stop}s {total}B Avg Block Size: {block_size / count:.2f}B")

