import csv
import time

path = r"../data/2022_place_canvas_history.csv"
write_path = r"../data/user_2022_place_history.csv"

min_date = '2022-04-01 12:00:00'

def minute_delta(min_date : str, curr_date : str):
    min_tokens = min_date.split(' ')
    curr_tokens = curr_date.split(' ')
    minute_diff = int((curr_tokens[1].split(':'))[1]) - int((min_tokens[1].split(':'))[1])
    hour_minute_diff = 60 * (int((curr_tokens[1].split(':'))[0]) - int((min_tokens[1].split(':'))[0]))
    day_minute_diff = 24 * 60 * (int((curr_tokens[0].split('-'))[2]) - int((min_tokens[0].split('-'))[2]))
    minute_delta = minute_diff+hour_minute_diff+day_minute_diff
    return minute_delta

def clean_CSV() -> None:
    start_time = time.perf_counter_ns()

    read = []
    unique_users = {}
    unique_id_counter = 1

    with open(path, "r", encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        header_indices = {header: index for index, header in enumerate(headers)}
        for row in csv_reader:
            timestamp = row[header_indices['timestamp']]
            user = row[header_indices['user_id']]
            timedelta = minute_delta(min_date, timestamp)
            
            color = row[header_indices['pixel_color']]
            color_int = int(color[1:], 16)
            if user in unique_users:
                user_id = unique_users[user]
            else:
                unique_users[user] = unique_id_counter
                user_id = unique_id_counter
                unique_id_counter += 1

            read.append({"timedelta": timedelta, "color": color_int, "user_id": user_id})

    read = sorted(read, key= lambda x: x['timedelta'])

    header = ["timedelta", "color", "user_id"]
    with open(write_path, "w", encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()
        writer.writerows(read)

    end_time = time.perf_counter_ns()
    print(f"Cleaning time: {end_time-start_time} ns\t {(end_time-start_time)/1000000} ms \t {(end_time-start_time)/1000000000} s")

if __name__ == '__main__':
    clean_CSV()