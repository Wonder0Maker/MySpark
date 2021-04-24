import connections as con

title_rating_info = con.titles_info.join(con.rating_info,
                con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
        .withColumn('numVotes', con.rating_info.numVotes.cast('integer'))\
        .withColumn('startYear', con.titles_info.startYear.cast('integer'))\
        .withColumn('genres',  con.split('genres', ",\s*").cast('array<string>'))\
        .withColumn('genres',  con.explode('genres'))\
        .withColumn('decade', (con.floor(con.col('startYear')/10)*10))\
        .drop(con.titles_info.tconst)\

window = con.Window.partitionBy('genres').orderBy(con.col('averageRating').desc(), con.col('numVotes').desc())
window1 = con.Window.partitionBy(con.floor(con.col('startYear')/10)*10)\
        .orderBy(con.col('startYear').desc())

def top_films_by_genres_f(tri):
    """ Function for find the best films by genres"""
    top_films_by_genres = tri.select(
            tri.tconst, tri.primaryTitle,
            tri.startYear, tri.genres,
            tri.averageRating, tri.numVotes, tri) \
        .where((tri.numVotes >= 100000)
               & (tri.titleType == 'movie'))\
        .orderBy(tri.averageRating.desc(), tri.numVotes.desc()) \
        .limit(100)
    top_films_by_genres = top_films_by_genres\
            .withColumn('rank',con.dense_rank().over(window)) \
            .withColumn('decade', con.dense_rank().over(window1))

    top_films_by_genres = top_films_by_genres.select(
            top_films_by_genres.tconst, top_films_by_genres.primaryTitle,
            top_films_by_genres.startYear, top_films_by_genres.genres,
            top_films_by_genres.averageRating, top_films_by_genres.numVotes,
            top_films_by_genres.decade)\
        .where(top_films_by_genres.rank <= 10)\
        .orderBy( top_films_by_genres.genres, top_films_by_genres.rank)
    top_films_by_genres.show(30)
    top_films_by_genres.printSchema()

    #con.write_csv(top_films_by_genres, 'TopFilmsByGenres')

def top_films_by_genres_exec():
    """ Function for execute top_films_by_genres in main"""
    top_films_by_genres_f(title_rating_info)
