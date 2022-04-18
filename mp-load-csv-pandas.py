#!usr/bin/env python3

% % time
LARGE_FILE = "train.csv"
CHUNKSIZE = 1000  # processing 100,000 rows at a time


def process_frame(df):
    # process data frame
    return len(df)


if __name__ == '__main__':
    reader = pd.read_table(LARGE_FILE, chunksize=CHUNKSIZE)
    pool = mp.Pool(4)  # use 4 processes

    funclist = []
    for df in reader:
        # process each data frame
        f = pool.apply_async(process_frame, [df])
        funclist.append(f)

    result = 0
    for f in funclist:
        result += f.get(timeout=10)  # timeout in 10 seconds

    print(f"There are {result} rows of data")