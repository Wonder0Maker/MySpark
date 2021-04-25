import connections as con

title_rating_info = con.titles_info.join(con.rating_info,
                                         con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
    .withColumn('numVotes', con.rating_info.numVotes.cast('integer')) \
    .withColumn('startYear', con.titles_info.startYear.cast('integer')) \
    .withColumn('genres', con.split('genres', ',\s*').cast('array<string>')) \
    .withColumn('genres', con.explode('genres')) \
    .withColumn('decade', (con.floor(con.col('startYear') / 10) * 10)) \
    .drop(con.titles_info.tconst)

window_genres = con.Window.partitionBy('genres') \
    .orderBy(con.col('averageRating').desc(), con.col('numVotes').desc())

window_decade = con.Window.partitionBy('decade') \
    .orderBy(con.col('decade').desc())

title_rating_info = title_rating_info \
    .where((title_rating_info.numVotes >= 100000)
           & (title_rating_info.titleType == 'movie')) \
    .orderBy(title_rating_info.averageRating.desc(),
             title_rating_info.numVotes.desc()) \
    .limit(300) \
    .withColumn('rank_genres', con.dense_rank().over(window_genres)) \
    .withColumn('rank_decade', con.dense_rank().over(window_decade))


def top_films_by_genres_decades():
    """ Function for find the best films by genres and by years"""
    top_films_by_genres_decades = title_rating_info.select(
        title_rating_info.tconst, title_rating_info.primaryTitle,
        title_rating_info.startYear, title_rating_info.genres,
        title_rating_info.averageRating, title_rating_info.numVotes,
        title_rating_info.decade) \
        .where(title_rating_info.rank_genres <= 10) \
        .orderBy(title_rating_info.decade.desc(),
                 title_rating_info.genres,
                 title_rating_info.rank_genres)

    con.write_csv(top_films_by_genres_decades, 'top_films_by_genres_decades')
