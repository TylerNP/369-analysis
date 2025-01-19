import util
import duckdb
import time
import sys

def countMostCommon(min : int, max : int) -> None:
    filtered_db = duckdb.sql(f"""
            SELECT color, x, y FROM read_parquet('{util.db_path}')
            WHERE timedelta >= {min} AND timedelta < {end} 
        """)
    
    color = duckdb.sql("SELECT color FROM filtered_db GROUP BY color ORDER BY COUNT(1) DESC LIMIT 1").fetchone()
    coord = duckdb.sql("SELECT x, y FROM filtered_db GROUP BY x, y ORDER BY COUNT(1) DESC LIMIT 1").fetchone()
    
    print(f"Most used color: #{(color[0]):06x} ")
    print(f"Most used coord: {coord}")

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