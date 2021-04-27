from pyspark.sql import functions as f
from pyspark.sql.window import Window

import connections as con

titles_info = con.read_tsv('title.basics.tsv')

rating_info = con.read_tsv('title.ratings.tsv')

window_genres = Window.partitionBy('genre') \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

window_decade = Window.partitionBy('decade') \
    .orderBy(f.col('decade').desc())

title_rating_info = titles_info.join(rating_info,
                                     titles_info.tconst == rating_info.tconst, 'inner') \
    .withColumn('genre', f.explode(f.split('genres', ','))) \
    .withColumn('decade', (f.floor(f.col('startYear') / 10) * 10)) \
    .where((f.col('titleType') == 'movie')
           & (f.col('numVotes') >= 100000)) \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc()) \
    .withColumn('rank_genres', f.dense_rank().over(window_genres)) \
    .withColumn('rank_decade', f.dense_rank().over(window_decade)) \
    .drop(titles_info.tconst)


def top_films_by_genres_decades():
    """
    Find the best films by genres and by years
    """
    top_films_by_genres = title_rating_info.select(
        'tconst', 'primaryTitle', 'startYear',
        'genre', 'averageRating', 'numVotes', 'decade') \
        .where(title_rating_info.rank_genres <= 10) \
        .orderBy(f.col('decade').desc(), f.col('genre'),
                 f.col('rank_genres'))

    return top_films_by_genres
