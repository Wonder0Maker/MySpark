import connections as con

title_rating_info = con.titles_info.join(con.rating_info,
                                         con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
    .withColumn('numVotes', con.rating_info.numVotes.cast('integer')) \
    .drop(con.titles_info.tconst)

title_cast_info = title_rating_info.join(con.cast_info,
                                         title_rating_info.tconst == con.cast_info.tconst.alias('tconst'), 'inner') \
    .drop(con.cast_info.tconst)

title_cast_person = title_cast_info.join(con.crew_cast_personal_info,
                                         title_cast_info.nconst == con.crew_cast_personal_info.nconst
                                         .alias('nconst'), 'inner') \
    .drop(con.crew_cast_personal_info.nconst)


def top_actors():
    """ Function for find the best actors in the best films"""
    top_actors = title_cast_person.select('nconst', 'primaryName') \
        .where((title_cast_person.numVotes >= 100000)
               & (title_cast_person.titleType == 'movie')) \
        .orderBy(title_rating_info.averageRating.desc(), title_rating_info.numVotes.desc())

    top_actors = top_actors.groupby('primaryName').count() \
        .select('primaryName').orderBy(con.col('count').desc()).limit(100)

    con.write_csv(top_actors, 'top_actors')
