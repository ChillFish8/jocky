import time

import zstandard

def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

with open("../datasets/data.json", mode="rb") as file:
    raw = file.read()
    samples = [sample for sample in divide_chunks(raw, 5 << 10)]

data = open("../datasets/data.json", mode="rb").read()

dict_data = zstandard.train_dictionary(25 << 20, samples[:50], level=3, notifications=3)
compressor = zstandard.ZstdCompressor(level=3, dict_data=dict_data)
decompressor = zstandard.ZstdDecompressor(dict_data=dict_data)

start = time.perf_counter()
for chunk in divide_chunks(data, 512 << 10):
    block = compressor.compress(chunk)
    decompressor.decompress(block)
stop = time.perf_counter() - start
print(f"Took: {stop}s")

start = time.perf_counter()
for chunk in divide_chunks(data, 512 << 10):
    block = zstandard.compress(chunk, level=3)
    zstandard.decompress(block)
stop = time.perf_counter() - start
print(f"Took: {stop}s")

