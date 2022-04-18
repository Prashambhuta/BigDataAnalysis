import multiprocessing as mp
import pandas as pd
import numpy as np
from haversine import haversine
from time import perf_counter
import matplotlib.pyplot as plt

def count_stats(df_group):
    df_group = df_group.sort_values(by=["# Timestamp"])

    df_group["Latitude_shifted"] = df_group["Latitude"].shift(+1)
    df_group["Longitude_shifted"] = df_group["Longitude"].shift(+1)
    df_group = df_group[1:]
    if df_group.shape[0] == 0:
        return 0

    df_group["Distance"] = df_group.apply(lambda row: haversine((row["Latitude"], row["Longitude"]), (row["Latitude_shifted"], row["Longitude_shifted"])), axis = 1)
    return df_group["Distance"].sum()

def run_mp(cpu_count):
    global results
    results = []

    def gather_results(result):
        global results
        results.append(result)

    cpu_pool = mp.Pool(cpu_count)
    for vessel in df["MMSI"].unique():
        if np.isnan(vessel) == False:
            df_arg = df[df["MMSI"] == vessel]
            cpu_pool.apply_async(count_stats, args=(df_arg,), callback=gather_results)
    cpu_pool.close()
    cpu_pool.join()
    return results

if __name__ == '__main__':
    df = pd.read_csv("../task-1/aisdk-2022-02-19.csv")[["# Timestamp",
                                                            "MMSI", "Latitude", "Longitude"]]
    df = df[df["Latitude"] <= 90]

    time = []
    for i in range(1, mp.cpu_count() + 1):
        starttime = perf_counter()
        results = run_mp(i)
        results = np.array(results)
        d = {results.mean(), results.std(), results.min(), results.max()}
        endtime = perf_counter()
        time.append(endtime - starttime)

    print(d)

    plt.plot(range(1, mp.cpu_count() + 1), time)
    plt.title('Time vs. CPU count')
    plt.xlabel('Number of CPUs used')
    plt.ylabel('TIme (in s)')
    plt.savefig("Time_vs_cpu.png")
    plt.show()

