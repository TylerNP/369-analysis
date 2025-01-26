import csv
import time

path = r"../data/2022_place_canvas_history.csv"
write_path = r"../data/cleaned_2022_place_history.csv"

min_date = '2022-04-01 12:00:00'

def hour_delta(min_date : str, curr_date : str):
    min_tokens = min_date.split(' ')
    curr_tokens = curr_date.split(' ')
    # Get Hour Difference
    hour_diff = int((curr_tokens[1].split(':'))[0]) - int((min_tokens[1].split(':'))[0])
    # Get Day Difference
    day_hour_diff = int((curr_tokens[0].split('-'))[2]) - int((min_tokens[0].split('-'))[2])
    hour_delta = hour_diff + day_hour_diff
    return hour_delta

def clean_CSV() -> None:
    start_time = time.perf_counter_ns()

    read = []

    with open(path, "r", encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        header_indices = {header: index for index, header in enumerate(headers)}
        for row in csv_reader:
            timestamp = row[header_indices['timestamp']]
            timedelta = hour_delta(min_date, timestamp)
            
            color = row[header_indices['pixel_color']]
            coord = row[header_indices['coordinate']].split(',')
            read.append({"timedelta": timedelta, "color": int(color[1:], 16), "x": int(coord[0]), "y": int(coord[1])})

    read = sorted(read, key= lambda x: x['timestamp'])

    header = ["timedelta", "color", "x", "y"]
    with open(write_path, "w", encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()
        writer.writerows(read)

    end_time = time.perf_counter_ns()
    print(f"Cleaning time: {end_time-start_time} ns\t {(end_time-start_time)/1000000} ms \t {(end_time-start_time)/1000000000} s")

if __name__ == '__main__':
    clean_CSV()