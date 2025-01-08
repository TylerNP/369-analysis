import csv 
import sys
import time
import datetime
from collections import defaultdict

path = r"Data\2022_place_canvas_history.csv\2022_place_canvas_history.csv"

def arg_to_datetime(date : str, time : str) -> datetime:
    date_info = date.split("-")
    return datetime.datetime(int(date_info[0]), int(date_info[1]), int(date_info[2]), int(time))

def most_placed_color_and_pixel(start : datetime, end : datetime) -> None:
    pixels = defaultdict(int)
    coords = defaultdict(int)
    start_time = time.perf_counter_ns()

    with open(path, "r", encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        header_indices = {header: index for index, header in enumerate(headers)}
        for row in csv_reader:
            tokens = row[header_indices['timestamp']].split(" ")
            date_info = tokens[0].split("-")
            time_info = tokens[1].split(":")
            timestamp = datetime.datetime(int(date_info[0]), int(date_info[1]), int(date_info[2]), int(time_info[0]))
            if timestamp < start:
                continue
            if timestamp > end:
                continue

            color = row[header_indices['pixel_color']]
            coord = row[header_indices['coordinate']]
            pixels[color] += 1
            coords[coord] += 1

    end_time = time.perf_counter_ns()
    print(f"Elapsed time: {end_time-start_time} ns\t {(end_time-start_time)/1000000} ms \t {(end_time-start_time)/1000000000} s")
    if pixels:
        print(f"Most Used Color {max(pixels, key=pixels.get)} {pixels[max(pixels, key=pixels.get)]}")
        print(f"Most Changed Coord {max(coords, key=coords.get)} {coords[max(coords, key=coords.get)]}")
        print(pixels)

def read_within_timeframe(start : datetime, end : datetime, rows : int):
    print(start, end)
    start_time = time.perf_counter_ns()

    with open(path, "r", encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        header_indices = {header: index for index, header in enumerate(headers)}
        for i, row in enumerate(csv_reader):
            if i > rows:
                break
            tokens = row[header_indices['timestamp']].split(" ")
            date_info = tokens[0].split("-")
            time_info = tokens[1].split(":")
            timestamp = datetime.datetime(int(date_info[0]), int(date_info[1]), int(date_info[2]), int(time_info[0]))
            if timestamp < start:
                continue
            if timestamp > end:
                break
            print(row)

    end_time = time.perf_counter_ns()
    print(f"Elapsed time: {end_time-start_time} ns\t {(end_time-start_time)/1000000} ms \t {(end_time-start_time)/1000000000} s")

def read_n_rows(rows : int):
    start_time = time.perf_counter_ns()

    with open(path, "r", encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        header_indices = {header: index for index, header in enumerate(headers)}
        for i, row in enumerate(csv_reader):
            if i > rows:
                break
            print(row)

    end_time = time.perf_counter_ns()
    print(f"Elapsed time: {end_time-start_time} ns\t {(end_time-start_time)/1000000} ms \t {(end_time-start_time)/1000000000} s")

if __name__ == '__main__':
    if len(sys.argv) > 3:
        start = arg_to_datetime(sys.argv[1], sys.argv[2])
        end = arg_to_datetime(sys.argv[3], sys.argv[4])
        if end > start:
            most_placed_color_and_pixel(start, end)
            #read_n_rows(10000000)
