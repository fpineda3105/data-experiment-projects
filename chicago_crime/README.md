# Processing Chicago Crime Data

This is an experiment using data from the [Data Chicago](https://data.cityofchicago.org/)

## The first step

The first step is to filter the Data, it's a csv file of [1.7GB](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) of data from 2001 to Present. The idea is to work with a portion of that 2020-2021.

The Main method reads the file and filter the required data and writes to a new file using [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) and [Queue](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Queue) to speed up the processing.
