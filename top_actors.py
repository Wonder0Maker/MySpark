from pyspark.sql import functions as f

from connections import read_tsv

titles_info = read_tsv('title.basics.tsv')

rating_info = read_tsv('title.ratings.tsv')

cast_info = read_tsv('title.principals.tsv')

crew_cast_personal_info = read_tsv('name.basics.tsv')

title_rating_info = titles_info \
    .join(rating_info,
          titles_info.tconst == rating_info.tconst,
          'inner') \
    .drop(titles_info.tconst) \
    .where((f.col('titleType') == 'movie')
           & (f.col('numVotes') >= 100000)) \
    .orderBy(f.col('averageRating').desc(),
             f.col('numVotes').desc())

title_cast_info = title_rating_info \
    .join(cast_info,
          title_rating_info.tconst == cast_info.tconst,
          'inner') \
    .drop(cast_info.tconst)

title_cast_person = title_cast_info \
    .join(crew_cast_personal_info,
          title_cast_info.nconst == crew_cast_personal_info.nconst,
          'inner') \
    .drop(crew_cast_personal_info.nconst) \
    .where(f.col('category').like('act%'))


def top_actors():
    """
    Find the best actors and actresses in the best films
    """
    top_films_by_genres = title_cast_person \
        .groupby('nconst', 'primaryName').count() \
        .select('primaryName') \
        .orderBy(f.col('count').desc())

    return top_films_by_genres
