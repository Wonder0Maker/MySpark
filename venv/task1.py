import connections as con

rb = con.basics.join(con.ratings,
                    con.basics.tconst == con.ratings.tconst, "inner") \
        .withColumn('numVotes', con.ratings.numVotes.cast('integer'))

def task1():
    rb.select(con.ratings.tconst, rb.primaryTitle,
              rb.numVotes, rb.averageRating, rb.startYear) \
        .where((rb.numVotes >= 100000)
               & (rb.titleType == 'movie')) \
        .orderBy(rb.averageRating.desc(), rb.numVotes.desc()) \
        .show(100)


def task2():
    rb.select(con.ratings.tconst, rb.primaryTitle,
              rb.numVotes, rb.averageRating, rb.startYear) \
        .where((rb.numVotes >= 100000)
               & (rb.titleType == 'movie')
               & (rb.startYear >= 2001)) \
        .orderBy(rb.averageRating.desc(), rb.numVotes.desc()) \
        .show(100)


def task3():
    rb.select(con.ratings.tconst, rb.primaryTitle,
              rb.numVotes, rb.averageRating, rb.startYear)\
        .where((rb.numVotes >= 100000)
               & (rb.titleType == 'movie')
               & (rb.startYear.like('196%')))\
        .orderBy(rb.averageRating.desc(), rb.numVotes.desc())\
        .show(100)

def task():
    task1()
    task2()
    task3()
