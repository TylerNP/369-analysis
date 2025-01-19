db_path = r"../data/lossy_2022_place_history.parquet"

min_date = '2022-04-01 12'

def min_hour_delta(curr_date : str, curr_hour : str):
    min_tokens = min_date.split(' ')
    # Get Hour Difference
    hour_diff = int(curr_hour) - int(min_tokens[1])
    # Get Day Difference
    day_hour_diff = 24 * (int((curr_date.split('-'))[2]) - int((min_tokens[0].split('-'))[2]))
    hour_delta = hour_diff + day_hour_diff
    return hour_delta