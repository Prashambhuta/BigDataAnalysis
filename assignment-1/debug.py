import time
import pandas as pd
import numpy as np
import multiprocessing as mp
import collections
import matplotlib.pyplot as plt


def count_chars(string_text):
    """
    Counts and returns the characters inside a string.
    https://stackoverflow.com/questions/991350/counting-repeated-characters-in-a-string-in-python/43085208#43085208

    :param string_text: just a string
    :return: perhaps a dict
    """
    char_dict = collections.defaultdict(int)

    # calculate character and append dict
    for char in string_text:
        char_dict[char] += 1

    return char_dict


def character_count(data, row_number):
    """
    Count the characters inside the string. return dict with following format :
    {character: number of times it appears}

    :param data: nx2, where col1 = row_number, col2 = text
    :param row_number: the row_number for the task
    :return:
    dict: {character (as str): no_of_appearance (int)}
    """
    # get the row out of the dataframe
    row_text = data.iloc[row_number, :]

    # apply method to calculate the no of characters (use Stackoverflow, google)
    results = count_chars(row_text["abstract"])

    # return the results_dict
    return results


if __name__ == "__main__":
    print("WELCOME")
    filename = "../assignment-1/covid_abstracts.csv"

    # chunks
    chunk_size = 1000

    # iterables df
    iter_df = pd.read_csv(filename, iterator=True, chunksize=chunk_size,
                          usecols=["abstract"])

    # Initiate time_taken list
    time_taken = []

    # Pooling
    cpus = mp.cpu_count()

    # start timer
    start_time = time.perf_counter()

    # results
    final_result = {}

    # Pool based on current cpu
    pool = mp.Pool(4)

    # For loop for iter_df
    for chunks in iter_df:
        # Define row_number
        row_number = list(range(len(chunks)))

        # Apply pool
        results = [pool.apply(character_count, args=(chunks, row)) for row in
                       row_number]

        # Update final_result
        dumm = [final_result.update({k: v + final_result.get(k, 0)}) for d in
                    results for k, v in d.items()]

    # stop the timer
    stop_time = time.perf_counter()

    # calculate time taken
    calculated_time = stop_time - start_time
    print(calculated_time)

    # append to list
    time_taken.append(calculated_time)

        # results
        # final_result = {}

        # Create final dataset
        # final_df = pd.DataFrame.from_dict(final_result, orient='index')
        # print(final_df.head())

    # Draw performance graph
    # plt.plot(range(2, cpus + 1), time_taken)
    # plt.xlabel("NO OF CPUS USED")
    # plt.ylabel("TIME TAKEN")
    # plt.title("Time taken for x no of CPU to run the program.")
    # plt.show()


