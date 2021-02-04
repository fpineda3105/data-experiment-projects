import csv
import os
import time
from datetime import datetime
import threading

import logging as log
from queue import Queue

log.basicConfig(filename='log_threading.log', encoding='utf-8', level=log.DEBUG)

def read_file(filename: str, from_year: int, q: Queue) -> None:

    with open(filename, 'r', newline='') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        q.put(csv_reader.fieldnames)
        start_timing = time.time()
        log.info(f'Starting reading: pushed {csv_reader.fieldnames} from {threading.current_thread()}')        
        for row in csv_reader:
            str_date = row['Date']
            date = datetime.strptime(str_date, '%m/%d/%Y %I:%M:%S %p')
            if date.year >= from_year:
                q.put_nowait(row)

        q.put_nowait(None)
        end_timing = time.time()
        log.info(f'Finished reading in {end_timing - start_timing} from {threading.current_thread()}')


def write_file(filename: str, q: Queue) -> None:

    with open(filename, 'w', newline='') as csvfile:
        fieldnames = q.get()
        start_timing = time.time()
        
        log.info(f'Starting Writing: {fieldnames} from {threading.current_thread()}')
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csv_writer.writeheader()
        q.task_done()
        
        while ((row := q.get()) != None):
            csv_writer.writerow(row)
            q.task_done()

        end_timing = time.time()
        log.info(f'Finish Writing in {end_timing - start_timing} from {threading.current_thread()}')


if __name__ == '__main__':
    threads = []
    queue = Queue()

    current_dir = os.getcwd()

    most_recent_data_file = os.path.join(
        current_dir, 'data', 'Crimes_-_2020_to_Present.csv')
    writer_thread = threading.Thread(target=write_file, args=(
        most_recent_data_file, queue))
    threads.append(writer_thread)
    writer_thread.start()

    crime_full_data_file = os.path.join(
        current_dir, 'data', 'Crimes_-_2001_to_Present.csv')

    start = time.time()

    reader_proc = threading.Thread(target=read_file, args=(
        crime_full_data_file, 2020, queue))
    threads.append(reader_proc)
    reader_proc.start()
    
    for proc in threads:
        proc.join()

    end = time.time()
    log.info(f'Execution time in seconds is: {end - start} from {threading.current_thread()}')
