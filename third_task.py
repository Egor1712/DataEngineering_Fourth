import csv
import pickle

from first_task import open_connection
from second_task import execute_query_and_save_result


def get_pickle_data(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def get_csv_data(filename):
    items = []
    with open(filename, 'r+', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        reader.__next__()
        for row in reader:
            if len(row) == 0:
                continue
            items.append({
                'artist': row[0],
                'song': row[1],
                'duration_ms': int(row[2]),
                'year': int(row[3]),
                'tempo': float(row[4]),
                'genre': row[5],
                'energy': float(row[6]),
                'key': int(row[7]),
                'loudness': float(row[8]),
            })
    return items


def create_table(db):
    cursor = db.cursor()
    cursor.execute("""drop table if exists song;""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS songs 
    (id INTEGER PRIMARY KEY,
    artist text,
    song text,
    duration_ms INTEGER,
    year INTEGER,
    tempo real,
    genre text,
    energy real,
    popularity integer,
    acousticness real,
    key integer,
    loudness real
    )""")


def insert_data(db, data, columns):
    cursor = db.cursor()
    cursor.executemany(f"""
    INSERT INTO songs ({', '.join(columns)}) values({' ,'.join([f':{x}' for x in columns])})""", data)
    db.commit()


pickle_data = get_pickle_data('./58/3/_part_1.pkl')
csv_data = get_csv_data('./58/3/_part_2.csv')

db = open_connection()
create_table(db)
insert_data(db, pickle_data, list(pickle_data[0].keys()))
insert_data(db, csv_data, list(csv_data[0].keys()))

execute_query_and_save_result(db,
                              """
                              select * from songs
                              where duration_ms > 240000""",
                              './results/3/third_task_filtered_by.json')

execute_query_and_save_result(db,
                              """
                              select 
                              sum(popularity) as sum_popularity,
                              avg(popularity) as avg_popularity,
                              min(popularity) as min_popularity,
                              max(popularity) as max_popularity
                              from songs""",
                              './results/3/third_task_characteristics.json')

execute_query_and_save_result(db,
                              """
                              select genre, count(*) as count from songs
                              group by genre
""",
                              './results/3/third_task_frequency.json')

execute_query_and_save_result(db,
                              """
                              select * from songs
                              where tempo > 130
                              order by artist 
""",
                              './results/3/third_task_ordered_and_filtered.json')
