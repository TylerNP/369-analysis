import util
import time
import sys
import pandas as pd

def countMostCommon(min : int, max : int) -> None:
    df = pd.read_parquet(util.db_path, engine='pyarrow')
    df = df[(df['timedelta'] >= start) & (df['timedelta'] < end)]
    x, y= df.groupby(['x', 'y']).size().idxmax()
    print(f"Most used color: #{(df['color'].value_counts().idxmax()):06x} ")
    print(f"Most used coord: ({x}, {y})")

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Not matching arguments <filename>.py YYYY-MM-DD HH YYYY-MM-DD HH")
        exit()
    start_time = time.perf_counter_ns()
    start = util.min_hour_delta(sys.argv[1], sys.argv[2])
    end = util.min_hour_delta(sys.argv[3], sys.argv[4])

    countMostCommon(start, end)
    end_time = time.perf_counter_ns()
    print(f"Time elapsed: {(end_time-start_time)/1000000} ms")