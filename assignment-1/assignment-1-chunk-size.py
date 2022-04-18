import time
import pandas as pd
import numpy as np
import multiprocessing as mp
import collections
import matplotlib.pyplot as plt


def count_chars(string_text):
    """
    Counts and returns the characters dict inside a string.
    https://stackoverflow.com/questions/991350/counting-repeated-characters-in-a-string-in-python/43085208#43085208

    :param string_text: just a string
    :return: a dict with key as character, and value as count
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
    results_dict = count_chars(row_text["abstract"])

    # return the results_dict
    return results_dict


if __name__ == "__main__":
    print("WELCOME")
    filename = "../assignment-1/covid_abstracts.csv"
    # df = pd.read_csv("../assignment-1/covid_abstracts.csv")
    # A = "Hey this contains some text"
    # B = "Hey more text here as well"
    # df = pd.DataFrame([[A, 1], [B, 2]], columns=["abstract", "row_number"])

    # chunks
    chunk_sizes = [10, 100, 500, 1000]

    # Initiate time_taken list
    time_taken = []

    # Pooling
    cpus = mp.cpu_count()

    final_result = {}

    # for loop for process
    for chunk_size in chunk_sizes:
        iter_df = pd.read_csv(filename, chunksize=chunk_size,
                              usecols=["abstract"])
        # iterables df

        # start timer
        start_time = time.perf_counter()

        # results
        # final_result = {}

        # Pool based on current cpu
        pool = mp.Pool(cpus)

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
    print(final_result)

    # Create final dataset
    final_df = pd.DataFrame.from_dict(final_result, orient='index', columns = ["Numbers"])
    final_df = final_df.sort_values(by=["Numbers"], ascending=False)
    final_df.head(10).to_csv("../assignment-1/output_2.csv")
    print(final_df.head(10))

    # Draw performance graph
    plt.plot(chunk_sizes, time_taken)
    plt.xlabel("CHUNK SIZE")
    plt.ylabel("TIME TAKEN")
    plt.title("Time taken program to run for various Chunk size.")
    plt.show()