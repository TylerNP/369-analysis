import util
import polars
import time
import sys

def countMostCommon(min : int, max : int) -> None:
    db = polars.scan_parquet(util.db_path).filter(
            (polars.col("timedelta") >= start) & (polars.col("timedelta") < end)
        ).select(["color", "x", "y"])
    color = db.select("color").group_by("color").agg(polars.len().alias("count")).top_k(k=1, by="count").collect()
    coord = db.select("x", "y").group_by("x", "y").agg(polars.len().alias("count")).top_k(k=1, by="count").collect()
    
    print(f"Most used color: #{(color["color"][0]):06x} ")
    print(f"Most used coord: {coord["x"][0], coord["y"][0]}")

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