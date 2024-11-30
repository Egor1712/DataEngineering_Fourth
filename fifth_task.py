import csv
import json
import pickle

from first_task import open_connection
from fourth_task import execute_sql_query
from second_task import execute_query_and_save_result


def create_movies_table(db, filename):
    with open(filename) as file:
        reader = csv.reader(file)
        movies = []
        reader.__next__()
        for row in reader:
            movies.append({
                'movieId': int(row[0]),
                'title': row[1],
                'genres': row[2].strip(),
            })
        cursor = db.cursor()
        cursor.execute("""drop table if exists movies""")
        cursor.execute('create table if not exists movies (movieId integer primary key, title integer, genres text)')
        cursor.executemany('insert into movies (movieId, title, genres) values (:movieId, :title, :genres)', movies)
        db.commit()
        return movies


def create_ratings_table(db, filename):
    with open(filename) as file:
        ratings = json.load(file)
        cursor = db.cursor()
        cursor.execute("""drop table if exists ratings""")
        cursor.execute("""create table if not exists ratings (userId integer, movieId integer references movies(movieId), rating real, timestamp integer)""")
        cursor.executemany("""insert into ratings (userId, movieId, rating, timestamp) values (:userId, :movieId, :rating, :timestamp)""", ratings)
        db.commit()
        return ratings

def create_tags_table(db, filename):
    with open(filename, 'rb') as file:
        tags = pickle.load(file)
        cursor = db.cursor()
        cursor.execute("""drop table if exists tags""")
        cursor.execute("""create table if not exists tags (userId integer references ratings(userId), movieId integer references movies(movieId), tag text, timestamp integer)""")
        cursor.executemany("""insert into tags (userId, movieId, tag, timestamp) values (:userId, :movieId, :tag, :timestamp)""", tags)
        db.commit()
        return tags


db = open_connection()
movies = create_movies_table(db, './58/5/movies.csv')
ratings = create_ratings_table(db, './58/5/ratings.json')
tags = create_tags_table(db, './58/5/tags.pkl')

execute_query_and_save_result(db, """
select * 
from movies  join ratings  on (movies.movieId = ratings.movieId)
join tags  on (movies.movieId = tags.movieId)
where genres like '%Comedy%'
order by title
limit 50
""", './results/5/fifth_task_filtered_by_comedy.json')


execute_query_and_save_result(db, """
select
tags.tag as tag,
count(*) as count,
min(ratings.rating) as min_rating,
max(ratings.rating) as msx_rating,
avg(ratings.rating) as avg_rating 
from movies
join ratings  on (movies.movieId = ratings.movieId)
join tags  on (movies.movieId = tags.movieId)
group by tags.tag
""", './results/5/fifth_task_characteristics_by_tags.json')

execute_query_and_save_result(db, """
select ratings.rating as rating, tags.tag as tag, count(*) as count  from movies
join ratings  on (movies.movieId = ratings.movieId)
join tags  on (movies.movieId = tags.movieId)
group by ratings.rating, tags.tag
""", './results/5/fifth_task_grouping_by_rating_and_tag.json')


update_sql_queries = [
    """update ratings set rating = rating + 1 where rating <= 1""",
    """delete from tags where tag = 'drugs' """
]

execute_sql_query(db, update_sql_queries)




