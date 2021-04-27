import connections as con
from pyspark.sql import functions as f
from datetime import datetime

titles_info = con.read_tsv('title.basics.tsv') \
    .select('tconst', 'primaryTitle', 'startYear') \
    .where(f.col('titleType') == 'movie')

rating_info = con.read_tsv('title.ratings.tsv') \
    .select('tconst', 'numVotes', 'averageRating') \
    .where((f.col('numVotes') >= 100000))

title_rating_info = titles_info.join(rating_info,
                                     titles_info.tconst == rating_info.tconst, 'inner') \
    .drop(titles_info.tconst) \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())


def top_films_of_all_times():
    '''
    Find the best films of all times
    '''

    top_films_of_all_times = title_rating_info.limit(100)

    return top_films_of_all_times


def top_films_of_last_10years():
    '''
    Find the best films of last 10 years
    '''
    current_date = datetime.now().year

    top_films_of_last_10years = title_rating_info \
        .where(f.col('startYear') >= current_date - 10) \
        .limit(100)

    return top_films_of_last_10years


def top_films_of_60s():
    '''
    Find the best films of 60`s
    '''

    top_films_of_60s = title_rating_info \
        .where(f.col('startYear').between(1960, 1969)) \
        .limit(100)

    return top_films_of_60s
