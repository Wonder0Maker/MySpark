import connections as con

title_rating_info = con.titles_info.join(con.rating_info,
                con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
        .withColumn('numVotes', con.rating_info.numVotes.cast('integer'))\
        .withColumn('genres',  con.split('genres', ",\s*").cast('array<string>'))\
        .withColumn('genres',  con.explode('genres'))\
        .drop(con.titles_info.tconst)\

window = con.Window.partitionBy('genres').orderBy(con.col('averageRating').desc(), con.col('numVotes').desc())

def top_films_by_genres_f(tri):
    """ Function for find the best films by genres"""
    top_films_by_genres = tri.select(
            tri.tconst, tri.primaryTitle,
            tri.startYear, tri.genres,
            tri.averageRating, tri.numVotes) \
        .where((tri.numVotes >= 100000)
               & (tri.titleType == 'movie'))\
        .orderBy(tri.averageRating.desc(), tri.numVotes.desc()) \
        .limit(100)
    top_films_by_genres = top_films_by_genres.withColumn('rank',con.dense_rank().over(window))
    top_films_by_genres = top_films_by_genres.select(
            top_films_by_genres.tconst, top_films_by_genres.primaryTitle,
            top_films_by_genres.startYear, top_films_by_genres.genres,
            top_films_by_genres.averageRating, top_films_by_genres.numVotes)\
        .where(top_films_by_genres.rank <= 10)\
        .orderBy(top_films_by_genres.genres, top_films_by_genres.rank)

    con.write_csv(top_films_by_genres, 'TopFilmsByGenres')

def top_films_by_genres_exec():
    """ Function for execute top_films_by_genres in main"""
    top_films_by_genres_f(title_rating_info)
