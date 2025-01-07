import csv 
import sys
import time

path = r"Data\2022_place_canvas_history.csv\2022_place_canvas_history.csv"

class DateHour:
    year : int
    month : int
    day : int
    hour : int

    def __init__(self, year : int, month : int, day : int, hour : int) -> "DateHour":
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour

    def cmp(self, other : "DateHour") -> bool:
        return (self.year, self.month, self.day, self.hour) > (other.year, other.month, other.day, other.hour)

    def print(self) -> None:
        print(f"{self.year}-{self.month}-{self.day} {self.hour}")

def str_to_datehour(date : str, time : str) -> DateHour:
    date_info = date.split("-")
    return DateHour(date_info[0], date_info[1], date_info[2], time)

def most_placed_color_and_pixel(start : DateHour, end : DateHour) -> None:
    pixels = {}
    coords = {}
    with open(path, "r", encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        header_indices = {header: index for index, header in enumerate(headers)}
        print(headers) 
        for i, row in enumerate(csv_reader):
            tokens = row[header_indices['timestamp']].split(" ")
            #color = row[header_indices['pixel_color']]
            coord = row[header_indices['coordinate']]
            date_info = tokens[0].split("-")
            time_info = tokens[1].split(":")
            timestamp = DateHour(date_info[0], date_info[1], date_info[2], time_info[0])
            if timestamp.cmp(start) and coord == "73,1944":
                #timestamp.print()
                if i % 10 == 0:
                    time.sleep(1)
                print(tokens, coord)
                print(i)
            if timestamp.cmp(end):
                break
            
            #timestamp.print()
            #timestamp = datetime.datetime.strptime(temp, "%y-%m-%d %H:%M:%S")
            #print(timestamp)
            #if timestamp >= start and timestamp <= end: 
                #print("HI")
            if i > 100000:
                break
            #print(f"{i}). {row}")

if __name__ == '__main__':
    if len(sys.argv) > 3:
        start = str_to_datehour(sys.argv[1], sys.argv[2])
        end = str_to_datehour(sys.argv[3], sys.argv[4])
        if end.cmp(start):
            most_placed_color_and_pixel(start, end)
