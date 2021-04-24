import connections as con

title_rating_info = con.titles_info.join(con.rating_info,
                con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
        .withColumn('numVotes', con.rating_info.numVotes.cast('integer'))\
        .drop(con.titles_info.tconst)


def top_films_of_all_times_f(tri):
    """ Function for find the best films of all times"""
    top_films_of_all_times = tri.select(
                tri.tconst, tri.primaryTitle,
                tri.numVotes, tri.averageRating, tri.startYear) \
        .where((tri.numVotes >= 100000)
               & (tri.titleType == 'movie')) \
        .orderBy(tri.averageRating.desc(), tri.numVotes.desc()) \
        .limit(100)

    con.write_csv(top_films_of_all_times, 'TopFilmsForAllTimes')



def top_films_of_last_10years_f(tri):
    """ Function for find the best films of last 10 years"""
    top_films_of_last_10years = tri.select(
                tri.tconst, tri.primaryTitle,
                tri.numVotes, tri.averageRating, tri.startYear) \
        .where((tri.numVotes >= 100000)
               & (tri.titleType == 'movie')
               & (tri.startYear >= 2001)) \
        .orderBy(tri.averageRating.desc(), tri.numVotes.desc()) \
        .limit(100)

    con.write_csv(top_films_of_last_10years, 'TopFilmsForLast10Years')


def top_films_of_60s_f(tri):
    """ Function for find the best films of 60`s"""
    top_films_of_60s = tri.select(
                con.rating_info.tconst, tri.primaryTitle,
                tri.numVotes, tri.averageRating, tri.startYear) \
        .where((tri.numVotes >= 100000)
               & (tri.titleType == 'movie')
               & (tri.startYear.like('196%')))\
        .orderBy(tri.averageRating.desc(), tri.numVotes.desc()) \
        .limit(100)

    con.write_csv(top_films_of_60s, 'TopFilmsFor60s')

def top_films():
    """ Function for execute top_films_of_all_times_f,
        top_films_of_last_10years_f, top_films_of_60s_f in main"""
    #top_films_of_all_times_f(title_rating_info)
    #top_films_of_last_10years_f(title_rating_info)
    top_films_of_60s_f(title_rating_info)
