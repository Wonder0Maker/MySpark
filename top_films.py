from datetime import datetime

from pyspark.sql import functions as f
from pyspark.sql.window import Window


def join_dataframes(left_df, right_df, condition):
    """
    Create dataframe by joining two datasets and condition for joining
    """
    return left_df.join(right_df, on=condition, how='inner')


def prepare_rating_info(left_df, right_df):
    """
    Create dataframe about film`s title and rating
    """
    df = join_dataframes(left_df, right_df, 'tconst') \
        .where((f.col('titleType') == 'movie') & (f.col('numVotes') >= 100000)) \
        .orderBy(f.col('averageRating').desc())

    return df


def top_films_of_all_times(df):
    """
    Find the best films of all times
    """
    return df.select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear').limit(100)


def top_films_of_last_10years(df):
    """
    Find the best films of last 10 years
    """
    current_year = datetime.now().year

    df = df.select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .where(f.col('startYear') >= current_year - 10)

    return df.limit(100)


def top_films_of_60s(df):
    """
    Find the best films of 60`s
    """
    df = df.select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .where(f.col('startYear').between(1960, 1969))
    return df.limit(100)


def explode_by_genres(df):
    """
    Create a ranking dataframe by dividing genres
    """
    window_genre = Window.partitionBy('genres').orderBy(f.col('averageRating').desc())
    df = df.withColumn('genres', f.explode(f.split('genres', ','))) \
        .withColumn('rank_genres', f.row_number().over(window_genre))
    return df


def top_films_by_genres(df):
    """
    Find the best films by genres
    """
    df = df.where(f.col('rank_genres') <= 10) \
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes')

    return df


def top_films_by_genres_decades(df):
    """
    Find the best films by genres and by years
    """
    window_decade = Window.partitionBy('decade').orderBy(f.col('decade').desc())
    df = df.withColumn('decade', f.concat((f.floor(f.col('startYear') / 10) * 10), f.lit('-'),
                                          (f.floor(f.col('startYear') / 10) * 10) + 10)) \
        .withColumn('rank_decade', f.row_number().over(window_decade))

    df = df.orderBy(f.col('decade').desc(), f.col('genres'), f.col('rank_genres')) \
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes', 'decade')

    return df


def prepare_actors(df1, df2, df3):
    """
    Create a dataframe by joining three datasets to find the best actors
    """
    df = join_dataframes(df1, df2, 'tconst')
    df = join_dataframes(df, df3, 'nconst')

    return df


def prepare_directors(df1, df2, df3):
    """
    Create a data frame by combining three datasets to find the best directors
    """
    df = join_dataframes(df1, df2, 'tconst') \
        .withColumn('directors', f.explode(f.split('directors', ',')))
    df = join_dataframes(df, df3, f.col('directors') == f.col('nconst'))

    return df


def top_actors(df):
    """
    Find the best actors and actresses in the best films
    """

    df = df.where(f.col('category').like('act%')) \
        .groupby('nconst', 'primaryName').count() \
        .orderBy(f.col('count').desc()) \
        .select('primaryName')

    return df


def top_films_director(df):
    """
    Find the best films of the directors
    """
    window_director = Window.partitionBy('directors').orderBy(f.col('averageRating').desc())

    df = df.withColumn('rank_films', f.row_number().over(window_director)) \
        .where(f.col('rank_films') <= 5) \
        .orderBy(f.col('directors')) \
        .select('primaryName', 'primaryTitle', 'startYear', 'averageRating', 'numVotes')

    return df
