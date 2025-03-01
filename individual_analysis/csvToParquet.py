import pyarrow.csv as pc
import pyarrow.parquet as pq
import sys
import os

if len(sys.argv) != 2:
    print("Must include a csv filename do not include file extension")
    exit()

path = f"{os.path.dirname(os.getcwd())}/data/{sys.argv[1]}.csv"
write = f"{os.path.dirname(os.getcwd())}/data/{sys.argv[1]}.parquet"

table = pc.read_csv(path, read_options=pc.ReadOptions(block_size=256 * 1024 * 1024))
pq.write_table(table, write)