import pyarrow.csv as pc
import pyarrow.parquet as pq
import sys

if len(sys.argv) != 2:
    print("Must include a csv filename do not include file extension")
    exit()

path = f"../data/{sys.argv[1]}.csv"
write = f"../data/{sys.argv[1]}.parquet"

table = pc.read_csv(path)
pq.write_table(table, write)