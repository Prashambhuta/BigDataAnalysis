# Assignment -1

**Big Data**

### Calculate unique characters and count using Parallel computing. Display top 10 characters.

|Idea | Desc. |
| ---- | -----|
|Use row_numbers | Use row numbers as argument to be passed. |



## Row Number Approach

* Add Row Number to dataframe
* def function for calculation of characters.
* for each row, calculate the dictionary for {character: no_of appearances}
* append to a results dict the result, using sum, get method of dictionary
* get len of dict - unique characters, `sum` - total number of appearances.

### Where is parallel_computing.

* Each row acts as a chunk, because the text in the row is large.

## Graph to calculate speed.

### Next steps:

- [x] Implement some pooling
- _Implement small df from original data_
* Implement graph to calculate speed
* Implement the whole thing
* Implement with chunks..



