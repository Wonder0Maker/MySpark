from pyspark.sql import functions as f
from pyspark.sql import Window

from connections import read_tsv

titles_info = read_tsv('title.basics.tsv')

rating_info = read_tsv('title.ratings.tsv')

window_genre = Window.partitionBy('genre') \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

title_rating_info = titles_info.join(rating_info,
                                     titles_info.tconst == rating_info.tconst, 'inner') \
    .withColumn('genre', f.explode(f.split('genres', ','))) \
    .where((f.col('titleType') == 'movie')
           & (f.col('numVotes') >= 100000)) \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc()) \
    .withColumn('rank_genre', f.dense_rank().over(window_genre)) \
    .drop(titles_info.tconst)


def top_films_by_genres():
    """
    Find the best films by genres
    """

    top_films_by_genres = title_rating_info.select(
        'tconst', 'primaryTitle', 'startYear',
        'genre', 'averageRating', 'numVotes') \
        .where(title_rating_info.rank_genre <= 10)

    return top_films_by_genres
