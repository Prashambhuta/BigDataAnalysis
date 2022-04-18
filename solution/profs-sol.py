import pandas as pd
import numpy as np
import multiprocessing as mp


# vectorized haversine function to calculate distance with geographical coordinates.
def haversine(lat1, lon1, lat2, lon2, to_radians=True, earth_radius=6371):
    if to_radians:
        lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(
        (lon2 - lon1) / 2.0) ** 2
    return earth_radius * 2 * np.arcsin(np.sqrt(a))


def distance_vessel(vessel, data):
    # Filtering data for particular vessel
    vessel_data = data.loc[data["MMSI"] == vessel]
    # Removing some noise in data
    vessel_data = vessel_data.loc[vessel_data["Latitude"] < 90]
    # do not process empty data
    if len(vessel_data) < 1:
        return {vessel: 0}
    # Shifting column to calculate distance between flowing location of vessels
    lat1 = vessel_data.Latitude.shift().values
    lat2 = vessel_data.loc[0:, 'Latitude'].values
    long1 = vessel_data.Longitude.shift().values
    long2 = vessel_data.loc[0:, 'Longitude'].values
    lat1[0], long1[0] = lat2[0], long2[0]
    return {vessel: haversine(lat1, long1, lat2, long2).sum()}


if __name__ == '__main__':
    print('PyCharm')
    filename = ("../task-1/aisdk-2022-02-19.csv")

    chunksize = 2000000

    # Reading csv in iterate mode.
    iter_csv = pd.read_csv(filename, iterator=True, chunksize=chunksize,
                           usecols=["MMSI", "Latitude", "Longitude"])

    # Preparing CPU pull
    cpus = mp.cpu_count()
    print(cpus)
    pool = mp.Pool(cpus)

    total = {}
    counter = 1
    # Process each chunk individualy. It can be processes in parallel as well.
    for chunk_df in iter_csv:
        print(
            f"Chunk being processed {counter}, data index read: {counter * chunksize}")
        counter += 1

        # Geting all vessels IDs in chunck
        vessels = chunk_df["MMSI"].unique()

        # Calculating each vessel in parallel
        results = [pool.apply(distance_vessel, args=(vessel, chunk_df)) for vessel
                   in vessels]

        # summing results parallel results with previous chunks results
        emt = {total.update({k: v + total.get(k, 0)}) for d in results for k, v in
               d.items()}
        print(f'Test vessel distance in kilometers: {total.get(257316000)}')

        # Limiting number of chunks for debuging
        # if counter >= 2: break
    pool.close()
    print(total)
    df = pd.DataFrame.from_dict(total, orient='index')
    print(df.head(10))
    print(df.describe())