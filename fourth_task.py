import csv
from os.path import curdir

from first_task import open_connection
from second_task import execute_query_and_save_result


def read_items(filename):
    with open(filename, 'r+', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=';')
        reader.__next__()
        items = []
        for row in reader:
            if len(row) == 0:
                continue
            if len(row) == 7:
                item = {
                    'name': row[0],
                    'price': float(row[1]),
                    'quantity': int(row[2]),
                    'fromCity': row[4],
                    'category': row[3],
                    'isAvailable': bool(row[5]),
                    'views': int(row[6])
                }
            else:
                item = {
                    'name': row[0],
                    'price': float(row[1]),
                    'quantity': int(row[2]),
                    'fromCity': row[3],
                    'isAvailable': bool(row[4]),
                    'views': int(row[5]),
                    'category': None
                }
            items.append(item)

        return items


def create_table(db):
    cursor = db.cursor()
    cursor.execute("""drop table if exists products""")
    cursor.execute("""
    create table if not exists products (
    id integer primary key,
   name string not null,
   price real not null check(price > 0),
   quantity integer not null check(quantity >= 0),
   fromCity text,
   isAvailable boolean not null,
   views integer not null,
   category text null,
   updates_count integer default 0)
    """)


def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
    insert into products (name, price, quantity, fromCity, isAvailable, views, category)
    values (:name, :price, :quantity, :fromCity, :isAvailable, :views, :category)
    """, data)
    db.commit()


def get_updates(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        items = []
        data = {}
        for line in file:
            line = line.strip()
            if line == "=====":
                items.append(data)
                data = {}
                continue
            split = line.split("::")
            key = split[0].strip()
            value = split[1].strip() if len(split) > 1 else None
            data[key.strip()] = value
        return items


def execute_sql_query(db, queries):
    db.isolation_level = None
    cursor = db.cursor()
    cursor.execute('begin')  # sql lite isolation level is serializable by default
    try:
        for query in queries:
            cursor.execute(query)
        cursor.execute('commit')
    except Exception as e:
        print(f'{e}, so rollback transaction')
        cursor.execute('rollback')


def commit_all_updates(db, updates):
    for update in updates:
        method = update['method']
        name = update['name']
        param = update['param']
        main_sql_query = sql_factories[method](name, param)
        sql_queries = [
            main_sql_query,
            f"""update products set updates_count = updates_count + 1 where name = '{name}'"""]
        execute_sql_query(db, sql_queries)


sql_factories = {
    'price_abs': lambda name, param: f"""update products set price = ABS({param}) where name = '{name}'""",
    # i have no idea what price_abs means
    'available': lambda name, param: f"""update products set isAvailable = {param} where name = '{name}'""",
    'quantity_add': lambda name, param: f"""update products set quantity = quantity + {param} where name = '{name}'""",
    'remove': lambda name, param: f"""delete from products where  name = '{name}'""",
    'price_percent': lambda name, param: f"""update products set price = price*(1+({param})) where name = '{name}'""",
    'quantity_sub': lambda name,
                           param: f"""update products set quantity = quantity - ABS({param}) where name = '{name}'""",

}

items = read_items('./58/4/_product_data.csv')

db = open_connection()
create_table(db)
insert_data(db, items)

updates = get_updates('./58/4/_update_data.text')
commit_all_updates(db, updates)

execute_query_and_save_result(db, """
select * from products
order by updates_count desc
limit 10""", './results/4/fourth_task_most_updatable_items.json')

execute_query_and_save_result(db, """
select 
category,
sum(price) as sum_price,
min(price) as min_price,
max(price) as max_price,
avg(price) as avg_price
 from products
group by category""",
                              './results/4/fourth_task_category_characteristics.json')

execute_query_and_save_result(db, """
select 
category,
count(*) as count,
sum(quantity) as sum_quantity,
min(quantity) as min_quantity,
max(quantity) as max_quantity,
avg(quantity) as avg_quantity
 from products
group by category""",
                              './results/4/fourth_task_quantity_characteristics.json')

execute_query_and_save_result(db, """
select 
fromCity,
count(*) as count,
sum(quantity) as sum_quantity,
min(quantity) as min_quantity,
max(quantity) as max_quantity,
avg(quantity) as avg_quantity
 from products
group by fromCity
having count(*) > 1""",
                              './results/4/fourth_task_quantity_characteristics_for_city.json')
