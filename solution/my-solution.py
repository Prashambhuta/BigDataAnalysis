# Calculate certain attributes using multiprocessing

import pandas as pd
import numpy as np
from haversine import haversine
import time
import multiprocessing as mp
import os


def distance_calculation(data, vessel):
    """
    Calculating the distance travelled.
    :param data: datset
    :param vessel: unique vessel
    :return: distance
    """
    # Getting individual vessel data.
    vessel_data = data.loc[data["MMSI"] == vessel]

    # filter data
    vessel_data = vessel_data.loc[vessel_data["Longitude"] < 90]

    if len(vessel_data) < 1:
        return {vessel_data: 0}

    # sorting by time first
    vessel_data = vessel_data.sort_values(by=["# Timestamp"])

    # Shifting columns
    lat1 = vessel_data.Latitude.shift().values
    lat2 = vessel_data.loc[0:, 'Latitude'].values

    lon1 = vessel_data.Longitude.shift().values
    lon2 = vessel_data.loc[0:, 'Longitude'].values

    lat1[0], lon1[0] = lat2[0], lon2[0]

    return {vessel: haversine((lat1, lon1), (lat2, lon2)).sum()}


if __name__ == "__main__":
    print("Welcome to PyCharm")
    dir_path = os.getcwd()
    filename = ("../task-1/aisdk-2022-02-19.csv")
    chunksize = 2000000
    df = pd.read_csv(filename, iterator=True, chunksize=chunksize, usecols=[
        "# Timestamp","MMSI", "Latitude", "Longitude"])

    # Preparing for cpu pooling
    cpus = mp.cpu_count()
    pool = mp.Pool(cpus)

    # Initiating things to count
    total = {}
    counter = 1

    # Processing the chunks
    for chunk_df in df:
        print(f"Chunk number being processed: {counter}, data index currently "
              f"read: {counter * chunksize}")
        counter += 1

        # Getting all vessel ids in chunk
        vessels = chunk_df["MMSI"].unique()

        # Calculating distance of each vessel in parallel
        results = [pool.apply(distance_calculation, args=(chunk_df, vessel)) for
                   vessel in vessels]

        # Summing the results from the previous results
        emt = {total.update({k: v + total.get(k, 0)}) for d in results for k,
                                                                           v in
               d.items()}
        print(f"The total distance by vessel in kms: {total.get(257316000)}")

        # if counter > 2: break

    pool.close()
    print(total)
    df = pd.DataFrame.from_dict(total, orient='index')
    print(df.head())
    print(df.describe())

