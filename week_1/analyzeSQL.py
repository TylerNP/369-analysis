import sqlite3
import sys
import datetime
import time
import warnings

# Suppress DeprecationWarnings
# Default datetime is fine for this datetime instance
warnings.filterwarnings("ignore", category=DeprecationWarning)

db_path = r"../data/2022_place_canvas_history.db"

def arg_to_datetime(date : str, time : str) -> datetime:
    date_info = date.split("-")
    return datetime.datetime(int(date_info[0]), int(date_info[1]), int(date_info[2]), int(time))

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Not matching arguments <filename>.py YYYY-MM-DD HH YYYY-MM-DD HH")
        exit()
    start_time = time.perf_counter_ns()
    start = arg_to_datetime(sys.argv[1], sys.argv[2])
    end = arg_to_datetime(sys.argv[3], sys.argv[4])
        
    #print(start, end)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    result = c.execute('''
        WITH filtered_pixels AS (
            SELECT 
                color,
                COUNT(1) AS num
            FROM 
                pixels
            WHERE
                timestamp >= ?
                AND timestamp <= ?
            GROUP BY
                color
            ORDER BY
                num DESC
        )
        SELECT 
            color,
            num
        FROM 
            filtered_pixels
        LIMIT 
            1
    ''',
    (start, end)
    )
    color, colorCount = result.fetchone()
    result = c.execute('''
        WITH filtered_pixels AS (
            SELECT 
                x,
                y,
                COUNT(1) as num
            FROM 
                pixels
            WHERE
                timestamp >= ?
                AND timestamp <= ?
            GROUP BY
                x,
                y
            ORDER BY
                num DESC
        )
        SELECT 
            x || ', ' || y AS coord,
            num
        FROM 
            filtered_pixels
        LIMIT 
            1
    ''',
    (start, end)
    )
    coord, coordCount = result.fetchone()
    print(f"Most used color: {color} with {colorCount}")
    print(f"Most used coordinate: {coord} with {coordCount}")

    conn.commit()
    conn.close()
    end_time = time.perf_counter_ns()
    print(f"Time elapsed: {(end_time-start_time)/1000000} ms")