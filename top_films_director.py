import connections as con

title_rating_info = con.titles_info.join(con.rating_info,
                                         con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
    .withColumn('numVotes', con.rating_info.numVotes.cast('integer')) \
    .drop(con.titles_info.tconst)

title_crew_info = title_rating_info.join(con.crew_info,
                                         title_rating_info.tconst == con.crew_info.tconst.alias('tconst'), 'inner') \
    .withColumn('director', con.split(con.crew_info.directors, ',\s*').cast('array<string>')) \
    .withColumn('director', con.explode('director')) \
    .drop(con.crew_info.tconst)

title_crew_name = title_crew_info.join(con.crew_cast_personal_info,
                                       title_crew_info.director == con.crew_cast_personal_info.nconst \
                                       .alias('nconst'), 'inner') \
    .drop(con.crew_cast_personal_info.nconst)

window_director = con.Window.partitionBy('director') \
    .orderBy(con.col('averageRating').desc(), con.col('numVotes').desc())

title_crew_name = title_crew_name.withColumn('rank_films', con.dense_rank().over(window_director))


def director_top_film():
    """ Function for find the best films of the directors"""
    top_films_director = title_crew_name.select(
        title_crew_name.primaryName, title_crew_name.primaryTitle,
        title_crew_name.startYear, title_crew_name.averageRating,
        title_crew_name.numVotes) \
        .where((title_crew_name.rank_films <= 5)
               & (title_crew_name.titleType == 'movie')
               & (title_crew_name.numVotes >= 100000)) \
        .orderBy(title_crew_name.primaryName, title_crew_name.rank_films)

    con.write_csv(top_films_director, 'top_films_director')
