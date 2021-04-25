import connections as con

title_rating_info = con.titles_info.join(con.rating_info,
                                         con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
    .withColumn('numVotes', con.rating_info.numVotes.cast('integer')) \
    .withColumn('genres', con.split('genres', ',\s*').cast('array<string>')) \
    .withColumn('genres', con.explode('genres')) \
    .drop(con.titles_info.tconst)

window = con.Window.partitionBy('genres').orderBy(con.col('averageRating').desc(), con.col('numVotes').desc())


def top_films_by_genres():
    """ Function for find the best films by genres"""
    top_films_by_genres = title_rating_info.select(
        title_rating_info.tconst, title_rating_info.primaryTitle,
        title_rating_info.startYear, title_rating_info.genres,
        title_rating_info.averageRating, title_rating_info.numVotes) \
        .where((title_rating_info.numVotes >= 100000)
               & (title_rating_info.titleType == 'movie')) \
        .orderBy(title_rating_info.averageRating.desc(),
                 title_rating_info.numVotes.desc()) \
        .limit(100)
    top_films_by_genres = top_films_by_genres.withColumn('rank', con.dense_rank().over(window))
    top_films_by_genres = top_films_by_genres.select(
        top_films_by_genres.tconst, top_films_by_genres.primaryTitle,
        top_films_by_genres.startYear, top_films_by_genres.genres,
        top_films_by_genres.averageRating, top_films_by_genres.numVotes) \
        .where(top_films_by_genres.rank <= 10) \
        .orderBy(top_films_by_genres.genres, top_films_by_genres.rank)

    con.write_csv(top_films_by_genres, 'TopFilmsByGenres')
