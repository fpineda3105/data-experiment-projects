import csv
import os
import time
from datetime import datetime
import multiprocessing as mp
from multiprocessing import Queue, Process
import logging as log

log.basicConfig(filename='log_multi_processing.log', encoding='utf-8', level=log.DEBUG)

def read_file(filename: str, from_year: int, q: Queue) -> None:

    with open(filename, 'r', newline='') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        q.put(csv_reader.fieldnames)
        start_timing = time.time()
        log.info(f'Starting reading: pushed {csv_reader.fieldnames} from {mp.current_process()}')        
        for row in csv_reader:
            str_date = row['Date']
            date = datetime.strptime(str_date, '%m/%d/%Y %I:%M:%S %p')
            if date.year >= from_year:
                q.put_nowait(row)

        q.put_nowait(None)
        end_timing = time.time()
        log.info(f'Finished reading at {end_timing - start_timing} from {mp.current_process()}')


def write_file(filename: str, q: Queue) -> None:

    with open(filename, 'w', newline='') as csvfile:
        fieldnames = q.get()
        start_timing = time.time()
        
        log.info(f'Starting Writing: {fieldnames} from {mp.current_process()}')
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csv_writer.writeheader()
        
        while ((row := q.get()) != None):
            csv_writer.writerow(row)

        end_timing = time.time()
        log.info(f'Finish Writing at {end_timing - start_timing} from {mp.current_process()}')


if __name__ == '__main__':
    processes = []
    queue = Queue()

    current_dir = os.getcwd()

    most_recent_data_file = os.path.join(
        current_dir, 'data', 'Crimes_-_2020_to_Present.csv')
    writer_proc = Process(target=write_file, args=(
        most_recent_data_file, queue))
    processes.append(writer_proc)
    writer_proc.start()

    crime_full_data_file = os.path.join(
        current_dir, 'data', 'Crimes_-_2001_to_Present.csv')

    start = time.time()

    reader_proc = Process(target=read_file, args=(
        crime_full_data_file, 2020, queue))
    processes.append(reader_proc)
    reader_proc.start()
    
    for proc in processes:
        proc.join()

    end = time.time()
    log.info(f'Execution time in seconds is: {end - start} from {mp.current_process()}')
