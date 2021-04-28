from pyspark.sql import functions as f
from pyspark.sql import Window

from connections import read_tsv

titles_info = read_tsv('title.basics.tsv')

rating_info = read_tsv('title.ratings.tsv')

crew_info = read_tsv('title.crew.tsv')

crew_cast_personal_info = read_tsv('name.basics.tsv')

window_director = Window.partitionBy('director') \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

title_rating_info = titles_info \
    .join(rating_info,
          titles_info.tconst == rating_info.tconst,
          'inner') \
    .drop(titles_info.tconst) \
    .where((f.col('titleType') == 'movie')
           & (f.col('numVotes') >= 100000)) \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

title_crew_info = title_rating_info \
    .join(crew_info,
          title_rating_info.tconst == crew_info.tconst,
          'inner') \
    .drop(crew_info.tconst) \
    .withColumn('director', f.explode(f.split('directors', ',')))

title_crew_person = title_crew_info \
    .join(crew_cast_personal_info,
          title_crew_info.director == crew_cast_personal_info.nconst,
          'inner') \
    .drop(crew_cast_personal_info.nconst) \
    .withColumn('rank_films', f.dense_rank().over(window_director))


def top_films_director():
    """
    Find the best films of the directors
    """
    top_films_director = title_crew_person.select(
        'primaryName', 'primaryTitle', 'startYear',
        'averageRating', 'numVotes') \
        .where(f.col('rank_films') <= 5) \
        .orderBy(f.col('director'))

    return top_films_director
