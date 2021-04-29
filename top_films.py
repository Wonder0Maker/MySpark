from datetime import datetime

from pyspark.sql import functions as f
from pyspark.sql.window import Window


def title_rating_info(data_frame1, data_frame2):
    """
    Create data frame by joining
    """
    title_rating_info = data_frame1.join(data_frame2, data_frame1.tconst == data_frame2.tconst, 'inner') \
        .drop(data_frame1.tconst) \
        .where((f.col('titleType') == 'movie') & (f.col('numVotes') >= 100000)) \
        .orderBy(f.col('averageRating').desc(), f.col('numVotes').desc())

    return title_rating_info


def top_films_of_all_times(data_frame):
    """
    Find the best films of all times
    """
    top_films_of_all_times = data_frame \
        .select('tconst', 'primaryTitle', 'numVotes',
                'averageRating', 'startYear')

    return top_films_of_all_times.limit(100)


def top_films_of_last_10years(data_frame):
    """
    Find the best films of last 10 years
    """
    current_date = datetime.now().year

    top_films_of_last_10years = data_frame \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .where(f.col('startYear') >= current_date - 10)

    return top_films_of_last_10years.limit(100)


def top_films_of_60s(data_frame):
    """
    Find the best films of 60`s
    """
    top_films_of_60s = data_frame \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .where(f.col('startYear').between(1960, 1969))
    return top_films_of_60s.limit(100)


def top_films_by_genres(data_frame):
    """
    Find the best films by genres
    """
    window_genre = Window.partitionBy('genre').orderBy(f.col('averageRating').desc(), f.col('numVotes').desc())

    title_rating_info = data_frame \
        .withColumn('genre', f.explode(f.split('genres', ','))) \
        .withColumn('rank_genre', f.dense_rank().over(window_genre))

    top_films_by_genres = title_rating_info \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes') \
        .where(title_rating_info.rank_genre <= 10)

    return top_films_by_genres


def top_films_by_genres_decades(data_frame):
    """
    Find the best films by genres and by years
    """
    window_decade = Window.partitionBy('decade').orderBy(f.col('decade').desc())

    title_rating_info = data_frame \
        .withColumn('decade', (f.floor(f.col('startYear') / 10) * 10)) \
        .withColumn('rank_decade', f.dense_rank().over(window_decade))

    top_films_by_genres_decades = title_rating_info \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes', 'decade') \
        .orderBy(f.col('decade').desc(), f.col('genre'), f.col('rank_genre'))

    return top_films_by_genres_decades


def title_cast_info(data_frame1, data_frame2):
    """
    Create data frame by joining
    """
    title_cast_info = data_frame1.join(data_frame2, data_frame1.tconst == data_frame2.tconst, 'inner') \
        .drop(data_frame2.tconst)

    return title_cast_info


def title_cast_person(data_frame1, data_frame2):
    """
    Create data frame by joining
    """
    title_cast_person = data_frame1.join(data_frame2, data_frame1.nconst == data_frame2.nconst, 'inner') \
        .drop(data_frame2.nconst) \
        .where(f.col('category').like('act%'))

    return title_cast_person


def top_actors(dataframe):
    """
    Find the best actors and actresses in the best films
    """

    top_actors = dataframe \
        .groupby('nconst', 'primaryName').count() \
        .select('primaryName') \
        .orderBy(f.col('count').desc())

    return top_actors


def title_crew_info(data_frame1, data_frame2):
    """
    Create data frame by joining
    """
    title_crew_info = data_frame1.join(data_frame2, data_frame1.tconst == data_frame2.tconst, 'inner') \
        .drop(data_frame2.tconst) \
        .withColumn('director', f.explode(f.split('directors', ',')))

    return title_crew_info


def title_crew_person(data_frame1, data_frame2):
    """
    Create data frame by joining
    """
    window_director = Window.partitionBy('director').orderBy(f.col('averageRating').desc(), f.col('numVotes').desc())

    title_crew_person = data_frame1.join(data_frame2, data_frame1.director == data_frame2.nconst, 'inner') \
        .drop(data_frame2.nconst) \
        .withColumn('rank_films', f.dense_rank().over(window_director))

    return title_crew_person


def top_films_director(data_frame):
    """
    Find the best films of the directors
    """
    top_films_director = data_frame \
        .select('primaryName', 'primaryTitle', 'startYear', 'averageRating', 'numVotes') \
        .where(f.col('rank_films') <= 5) \
        .orderBy(f.col('director'))

    return top_films_director
