from top_films import (prepare_rating_info, top_films_of_all_times, top_films_of_last_10years, prepare_directors,
                       top_films_of_60s, top_films_by_genres, top_films_by_genres_decades,
                       prepare_actors, explode_by_genres, top_actors, top_films_director)
from connections import write_csv, read_tsv

if __name__ == '__main__':
    titles_info = read_tsv('title.basics.tsv')
    rating_info = read_tsv('title.ratings.tsv')
    cast_info = read_tsv('title.principals.tsv')
    crew_info = read_tsv('title.crew.tsv')
    crew_cast_personal_info = read_tsv('name.basics.tsv')

    title_rating_info = prepare_rating_info(titles_info, rating_info)
    exploded_by_genres = explode_by_genres(title_rating_info)
    prepared_actors = prepare_actors(title_rating_info, cast_info, crew_cast_personal_info)
    prepared_directors = prepare_directors(title_rating_info, crew_info, crew_cast_personal_info)

    write_csv(top_films_of_all_times(title_rating_info), 'top_films_of_all_times')
    write_csv(top_films_of_last_10years(title_rating_info), 'top_films_of_last_10years')
    write_csv(top_films_of_60s(title_rating_info), 'top_films_of_60s')
    write_csv(top_films_by_genres(exploded_by_genres), 'top_films_by_genres')
    write_csv(top_films_by_genres_decades(exploded_by_genres), 'top_films_by_genres_decades')
    write_csv(top_actors(prepared_actors), 'top_actors')
    write_csv(top_films_director(prepared_directors), 'top_films_director')
