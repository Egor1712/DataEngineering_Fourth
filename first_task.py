import json
import sqlite3

def open_connection():
    connection = sqlite3.connect('./results/all_tasks.db')
    connection.row_factory = sqlite3.Row
    return connection


def get_file_data():
    with open('./58/1-2/item.json', 'r+', encoding='utf-8') as file:
        data = json.load(file)
    return data


def create_table(db):
    cursor = db.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS items (id integer PRIMARY KEY, 
    name text,
    city text,
    begin text,
    system text,
    tours_count integer,
    min_rating real,
    time_on_game integer)""")


def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
    INSERT or ignore INTO items (id, name, city, begin, system, tours_count, min_rating, time_on_game)
    VALUES (:id, :name, :city, :begin, :system, :tours_count, :min_rating, :time_on_game)""",
                       items)
    db.commit()


def get_ordered_by(db, table_name, column_name, limit=50):
    cursor = db.cursor()
    result = cursor.execute(f"""
    select * from {table_name} order by {column_name} limit {limit}""")
    items = []
    for row in result.fetchall():
        items.append(dict(row))

    return items


def count_characteristics_by_filed(db, table_name, column_name):
    cursor = db.cursor()
    result = cursor.execute(f"""
    select 
    sum({column_name}) as sum,
    min({column_name}) as min,
    max({column_name}) as max,
    avg({column_name}) as avg
    from {table_name}
    """)
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def count_frequency_by_column(db, table_name, column_name):
    cursor = db.cursor()
    results = cursor.execute(f"""
    select {column_name}, count(*) as count from {table_name} group by {column_name}""")
    items = []
    for row in results.fetchall():
        items.append(dict(row))
    return items


def filter_and_order_by(db, table_name, filter_column_name, filter_predicate, order_column_name):
    cursor = db.cursor()
    results = cursor.execute(f"""
    select * from {table_name}
    where {filter_column_name} {filter_predicate}
    order by {order_column_name}
    """)
    items = []
    for row in results.fetchall():
        items.append(dict(row))
    return items


def write_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False)


data = get_file_data()
db = open_connection()
create_table(db)
insert_data(db, data)
write_items('./results/1/first_task_ordered.json', get_ordered_by(db, 'items', 'city'))
write_items('./results/1/first_task_characteristics.json', count_characteristics_by_filed(db, 'items', 'time_on_game'))
write_items('./results/1/first_task_frequency.json', count_frequency_by_column(db, 'items', 'system'))
write_items('./results/1/first_task_filter_and_order_by.json',
            filter_and_order_by(db, 'items', 'system', "='Olympic'", 'time_on_game'))
