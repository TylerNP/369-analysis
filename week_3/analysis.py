import duckdb
import time
import sys

db_path = r"../data/user_2022_place_history.parquet"

min_date = '2022-04-01 12:00:00'

color_name = {
    16729344: 'red', 
    0: 'black', 
    6970623: 'periwinkle', 
    7143450: 'burgandy', 
    16775352: 'pale yellow', 
    2379940: 'dark blue', 
    8461983: 'dark purple', 
    16754688: 'orange', 
    40618: 'teal', 
    52416: 'light teal', 
    8318294: 'light green', 
    12451897: 'dark red', 
    16757872: 'beige', 
    9745407: 'lavendar', 
    16777215: 'white', 
    11815616: 'purple', 
    41832: 'dark green', 
    10250534: 'brown', 
    14986239: 'pale purple', 
    16766517: 'yellow', 
    3576042: 'blue', 
    16751018: 'light pink', 
    5368308: 'light blue', 
    9014672: 'gray', 
    4799169: 'indigo', 
    30063: 'dark teal', 
    13948889: 'light gray', 
    5329490: 'dark gray', 
    52344: 'green', 
    16726145: 'pink', 
    7161903: 'dark brown', 
    14553215: 'magenta'
}

def min_minute_delta(curr_date: str, curr_hour: str):
    min_tokens = min_date.split(' ')
    hour_minute_diff = 60 * (int(curr_hour) - int((min_tokens[1].split(':'))[0]))
    day_minute_diff = 24 * 60 * (int((curr_date.split('-'))[2]) - int((min_tokens[0].split('-'))[2]))
    minute_delta = hour_minute_diff+day_minute_diff
    return minute_delta

def countMostCommon(min : int, max : int) -> None:
    filtered_db = duckdb.sql(f"""
        SELECT color, user_id, timedelta FROM read_parquet('{db_path}')
        WHERE timedelta >= {min} AND timedelta < {max} 
    """)
    
    # Get most Used Color By Unique Users
    color_unique_user = duckdb.sql(f"""
        SELECT color, COUNT(DISTINCT user_id) AS val 
        FROM filtered_db 
        GROUP BY color 
        ORDER BY val DESC, color DESC
    """).fetchall()

    # Calculate Session Length
    session_length = duckdb.sql(f"""
    WITH session_gaps AS (
        SELECT 
            user_id, 
            timedelta,
            (timedelta - LAG(timedelta) OVER (PARTITION BY user_id ORDER BY timedelta)) AS gap, 
        FROM 
            filtered_db
    ), session_marker AS (
        SELECT
            user_id,
            gap,
            SUM(CASE WHEN gap >= 15 THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY timedelta) AS id 
        FROM 
            session_gaps
    ), session_time AS (
        SELECT 
            user_id, 
            COUNT(1) AS actions,
            SUM(CASE WHEN gap < 15 THEN gap ELSE 0 END) AS session_length 
        FROM 
            session_marker
        GROUP BY 
            user_id,
            id
        HAVING 
            COUNT(1) > 1
    ), session_avg AS (
        SELECT
            user_id,
            (60*SUM(session_length)) / COUNT(1) AS avg_length
        FROM
            session_time
        GROUP BY
            user_id
    )

    SELECT 
        AVG(avg_length)
    FROM 
        session_avg
    """).fetchone()

    # Pixel Percentiles
    pixel_percentile = duckdb.sql(f"""
        WITH pixel_placements AS (
            SELECT 
                COUNT(1) AS pixel_placed
            FROM 
                filtered_db
            GROUP BY 
                user_id
        )

        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pixel_placed) AS p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pixel_placed) AS p75,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY pixel_placed) AS p90,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pixel_placed) AS p99
        FROM pixel_placements
    """).fetchone()

    # First Time Users Count
    first_time_users = duckdb.sql(f"""
        SELECT SUM(new_user) FROM (
            SELECT 1 AS new_user 
            FROM filtered_db 
            GROUP BY user_id 
            HAVING MIN(timedelta) >= {min} AND MIN(timedelta) < {max}
        )
        """).fetchone()

    # Print All Results
    print("\nRank Colors By Unique Users\n")
    i = 1
    for result in color_unique_user:
        print(f"{i}. {color_name[result[0]]}: {result[1]} users")
        i += 1
    print("\nAverage Session Length\n")
    print(f"{session_length[0]} s")
    print("\nPixel Placement By Percentile\n")
    print(f"50th Percentile: {pixel_percentile[0]}")
    print(f"75th Percentile: {pixel_percentile[1]}")
    print(f"90th Percentile: {pixel_percentile[2]}")
    print(f"99th Percentile: {pixel_percentile[3]}")
    print("\nCount Of First-time Users\n")
    print(f"{first_time_users[0]} users\n")    

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Not matching arguments <filename>.py YYYY-MM-DD HH YYYY-MM-DD HH")
        exit()
    start_time = time.perf_counter_ns()
    start = min_minute_delta(sys.argv[1], sys.argv[2])
    end = min_minute_delta(sys.argv[3], sys.argv[4])

    print(f"\nFrom {sys.argv[1]} {sys.argv[2]} to {sys.argv[3]} {sys.argv[4]}")
    countMostCommon(start, end)
    end_time = time.perf_counter_ns()
    print(f"Time elapsed: {(end_time-start_time)/1000000} ms\n")