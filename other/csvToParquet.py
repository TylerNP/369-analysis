import pyarrow.csv as pc
import pyarrow.parquet as pq

path = r"../data/user_2022_place_history.csv"
write = r"../data/user_2022_place_history.parquet"

table = pc.read_csv(path)
pq.write_table(table, write)