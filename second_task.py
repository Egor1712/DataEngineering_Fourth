import csv
import json

from first_task import open_connection


def get_items(filename):
    items = []
    with open(filename, 'r+', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=';')
        reader.__next__()
        for i, row in enumerate(reader):
            if len(row) == 0:
                continue
            items.append({
                'id': i,
                'name': row[0],
                'place': row[1],
                'price': row[2]
            })
        return items


def create_table(db):
    cursor = db.cursor()
    cursor.execute("""
    create table if not exists prices (
    id integer primary key,
    name text references items(name),
    place integer,
    price integer)
    """)


def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
    insert or ignore into prices (id, name, place, price)
    values (:id, :name, :place, :price)""", data)
    db.commit()


def execute_query_and_save_result(db, query, filename):
    cursor = db.cursor()
    results = cursor.execute(query)
    items = []
    for row in results.fetchall():
        items.append(dict(row))
    with open(filename, 'w+', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False)


items = get_items('./58/1-2/subitem.csv')
db = open_connection()
create_table(db)
insert_data(db, items)

execute_query_and_save_result(
    db,
    """SELECT * FROM items 
    JOIN prices on items.name = prices.name 
    where items.name='Тилбург 1974' 
    ORDER BY prices.price """,
    './results/2/second_task_first_query.json')

execute_query_and_save_result(
    db,
    """SELECT items.system, 
    count(*) as count, 
    sum(prices.price) as sum_price, 
    avg(prices.price) as avg_price
     FROM items JOIN prices on items.name = prices.name 
    group by items.system""",
    './results/2/second_task_second_query.json')

execute_query_and_save_result(
    db,
    """SELECT items.city, 
    count(*) as count, 
    sum(prices.price) as sum_price, 
    avg(prices.price) as avg_price,
    min(prices.price) as min_price,
    max(prices.price) as max_price
    FROM items JOIN prices on items.name = prices.name 
    group by items.city""",
    './results/2/second_task_third_query.json')
