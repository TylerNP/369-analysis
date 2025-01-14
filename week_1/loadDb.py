import sqlite3
import csv
import sys
import time

db_path = r"../data/2022_place_canvas_history.db"
path = r"../data/2022_place_canvas_history.csv"

conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS pixels (
        x INTEGER,
        y INTEGER,
        color TEXT,
        timestamp DATETIME
    )
    ''')

c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON pixels (timestamp)')

start_time = time.perf_counter_ns()
chunk_size = 1000
with open(path, "r", encoding='utf-8') as file:
    csv_reader = csv.reader(file)
    headers = next(csv_reader)
    header_indices = {header: index for index, header in enumerate(headers)}
    chunk = []
    for i, row in enumerate(csv_reader):
        timestamp = row[header_indices['timestamp']]
        color = row[header_indices['pixel_color']]
        coord = row[header_indices['coordinate']].split(',')
        chunk.append((coord[0], coord[1], color, timestamp))
        if (i%chunk_size == 0):
            #print(chunk)
            c.executemany('''
                INSERT INTO pixels (x, y, color, timestamp)
                    VALUES (?, ?, ?, ?)
                ''',
                chunk
            )
            conn.commit()
            chunk = []

end_time = time.perf_counter_ns()

print(f"Seconds elapsed: {(end_time-start_time)/1000000000}")

conn.commit()
conn.close()