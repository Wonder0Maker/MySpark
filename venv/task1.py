import connections as con

newDF = con.titles_info.join(con.rating_info,
                con.titles_info.tconst == con.rating_info.tconst.alias('tconst'), 'inner') \
        .withColumn('numVotes', con.rating_info.numVotes.cast('integer'))\
        .drop(con.titles_info.tconst)\


def TopFilmsForAllTimes():
    topFilmsForAllTimes = newDF.select(
                newDF.tconst, newDF.primaryTitle,
                newDF.numVotes, newDF.averageRating, newDF.startYear) \
        .where((newDF.numVotes >= 100000)
               & (newDF.titleType == 'movie')) \
        .orderBy(newDF.averageRating.desc(), newDF.numVotes.desc()) \
        .limit(100)

    topFilmsForAllTimes.write.format('csv')\
        .option("header", True).mode('overwrite')\
        .save("D:\pythonProject1\outputs\TopFilmsForAllTimes")


def TopFilmsForLast10Years():
    topFilmsForLast10Years = newDF.select(
                newDF.tconst, newDF.primaryTitle,
                newDF.numVotes, newDF.averageRating, newDF.startYear) \
        .where((newDF.numVotes >= 100000)
               & (newDF.titleType == 'movie')
               & (newDF.startYear >= 2001)) \
        .orderBy(newDF.averageRating.desc(), newDF.numVotes.desc()) \
        .limit(100)

    topFilmsForLast10Years.write.format('csv')\
        .option("header", True).mode('overwrite') \
        .save("D:\pythonProject1\outputs\TopFilmsForLast10Years")


def TopFilmsFor60s():
    TopFilmsFor60s = newDF.select(
                con.rating_info.tconst, newDF.primaryTitle,
                newDF.numVotes, newDF.averageRating, newDF.startYear) \
        .where((newDF.numVotes >= 100000)
               & (newDF.titleType == 'movie')
               & (newDF.startYear.like('196%')))\
        .orderBy(newDF.averageRating.desc(), newDF.numVotes.desc()) \
        .limit(100)

    TopFilmsFor60s.write.format('csv')\
        .option("header", True).mode('overwrite') \
        .save("D:\pythonProject1\outputs\TopFilmsFor60s")

def TopFilms():
    TopFilmsForAllTimes()
    TopFilmsForLast10Years()
    TopFilmsFor60s()
