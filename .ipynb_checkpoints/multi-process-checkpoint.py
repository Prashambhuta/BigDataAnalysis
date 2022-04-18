import multiprocessing as mp
import numpy as np
from time import time

np.random.RandomState(1)
arr = np.random.randint(0, 10, size=[10000, 10])
data = arr.tolist()
print(data[:5])

def count_even(one_row):
    e = 0
    for i in one_row:
        if i%2 == 0:
            e += 1
    return e

results = []

for row in data:
    e = count_even(row)
    results.append(e)

print(sum(results))

cpu_pool = mp.Pool(mp.cpu_count())
results = [cpu_pool.apply(count_even, args=(row, )) for row in data]

print(sum(results))

results = cpu_pool.starmap(count_even, [(row, ) for row in data])

print(sum(results))

# global results2
results2 = []

# cpu_pool.close()

# results = cpu_pool.starmap(count_even, [(row, ) for row in data])


def gather_results(result):
    global results2
    results2.append(result)


for i, row in enumerate(data):
    cpu_pool.apply_async(count_even, args=(row, ), callback = gather_results)

cpu_pool.join()
cpu_pool.close()
