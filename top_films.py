import connections as con

title_rating_info = con.titles_info.join(con.rating_info,
                con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
        .withColumn('numVotes', con.rating_info.numVotes.cast('integer'))\
        .drop(con.titles_info.tconst)


def top_films_of_all_times_f():
    """ Function for find the best films of all times"""
    top_films_of_all_times = title_rating_info.select(
                title_rating_info.tconst, title_rating_info.primaryTitle,
                title_rating_info.numVotes, title_rating_info.averageRating,
                title_rating_info.startYear) \
        .where((title_rating_info.numVotes >= 100000)
               & (title_rating_info.titleType == 'movie')) \
        .orderBy(title_rating_info.averageRating.desc(), title_rating_info.numVotes.desc()) \
        .limit(100)

    con.write_csv(top_films_of_all_times, 'TopFilmsForAllTimes')



def top_films_of_last_10years_f():
    """ Function for find the best films of last 10 years"""
    top_films_of_last_10years = title_rating_info.select(
                title_rating_info.tconst, title_rating_info.primaryTitle,
                title_rating_info.numVotes, title_rating_info.averageRating,
                title_rating_info.startYear) \
        .where((title_rating_info.numVotes >= 100000)
               & (title_rating_info.titleType == 'movie')
               & (title_rating_info.startYear >= 2001)) \
        .orderBy(title_rating_info.averageRating.desc(),
                 title_rating_info.numVotes.desc()) \
        .limit(100)

    con.write_csv(top_films_of_last_10years, 'TopFilmsForLast10Years')


def top_films_of_60s_f():
    """ Function for find the best films of 60`s"""
    top_films_of_60s = title_rating_info.select(
                title_rating_info.tconst, title_rating_info.primaryTitle,
                title_rating_info.numVotes, title_rating_info.averageRating,
                title_rating_info.startYear) \
        .where((title_rating_info.numVotes >= 100000)
               & (title_rating_info.titleType == 'movie')
               & (title_rating_info.startYear.like('196%')))\
        .orderBy(title_rating_info.averageRating.desc(),
                 title_rating_info.numVotes.desc()) \
        .limit(100)

    con.write_csv(top_films_of_60s, 'TopFilmsFor60s')

def top_films():
    """ Function for execute top_films_of_all_times_f,
        top_films_of_last_10years_f, top_films_of_60s_f in main"""
    top_films_of_all_times_f()
    top_films_of_last_10years_f()
    top_films_of_60s_f()
