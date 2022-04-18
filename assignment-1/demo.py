import pandas as pd
import numpy as np
import multiprocessing as mp
import collections


def count_chars(string_text):
    """
    Counts and returns the characters inside a string.
    https://stackoverflow.com/questions/991350/counting-repeated-characters-in-a-string-in-python/43085208#43085208

    :param string_text: just a string
    :return: perhaps a dict
    """
    # arr = np.fromstring(s, dtype=np.uint8)
    # unique_char, char_count = np.unique(arr, return_counts=True)
    #
    # return dict(zip(unique_char, char_count))

    char_dict = collections.defaultdict(int)

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
    # initiate empty results_dict
    row_results = {}

    # get the row out of the dataframe
    row_text = data.iloc[row_number, :]

    # apply method to calculate the no of characters (use Stackoverflow, google)
    results = count_chars(row_text["abstract"])

    # append to results_dict

    # return the results_dict
    return results


if __name__ == "__main__":
    print("WELCOME")
    # df = pd.read_csv("../assignment-1/covid_abstracts.csv")
    A = "Hey this contains some text"
    B = "Hey more text here as well"

    df = pd.DataFrame([[A, 1], [B, 2]], columns=["abstract", "row_number"])

    # chunks

    # iterables df

    # Pooling

    # results
    final_result = {}

    # run the code
    for i in range(len(df)):
        dummy = character_count(df, i)
        empt = {final_result.update({k: v + final_result.get(k, 0)}) for k, v in
                dummy.items()}

    print(final_result)

    # update the results table
