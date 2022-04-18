# Assignment 2

The goal is to use NoSQl cluster, and upload, filter, process the data in a new collation. Finally the goal is to generate particular plots within the data.

**Note** - The tasks needs to be done using parallel computing.

## Task details:
* Create a cluster of NoSQL databases. The cluster can configure on your personal machine or on MIF virtual machines. 
* Insert into database data from CSV file in a parallel manner. (please think about the chunk size. In presentation motivate chunk size selection) (I recommend using MongoClient as a separate instance in each parallel thread or parallel task)
* Perform data noise filtering in a parallel manner and store filtered data in a separate collation. (I recommend create indexes)
* Calculate (delta t) for each filtered vessel and plot a histogram. Delta t is a time difference in milliseconds between two subsequential in time data points. 
* Presentation of the solution. The solution will be presented by one student from the group that will be chosen randomly from the same group. The presentation shall me max 5 slides, that will present solutions done in 1 to 4 tasks. 

## Filter criteria:

* All vessels that have less than 100 data points are considered noise
* Data points with missing or invalid fields: Navigational status, 
* MMSI, Latitude, Longitude, Navigational status, ROT, SOG, COG, Heading.  (Please approach invalid field creatively)
* Vessel data point where COG ==0
* Latitude and longitude out of range of WGS84 