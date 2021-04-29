import top_films as tf
from connections import write_csv, read_tsv

if __name__ == '__main__':
    titles_info = read_tsv('title.basics.tsv')
    rating_info = read_tsv('title.ratings.tsv')
    cast_info = read_tsv('title.principals.tsv')
    crew_info = read_tsv('title.crew.tsv')
    crew_cast_personal_info = read_tsv('name.basics.tsv')

    title_rating_info = tf.title_rating_info(titles_info, rating_info)
    top_films_by_genres = tf.top_films_by_genres(title_rating_info)
    title_cast_info = tf.title_cast_info(title_rating_info, cast_info)
    title_cast_person = tf.title_cast_person(title_cast_info, crew_cast_personal_info)
    title_crew_info = tf.title_crew_info(title_rating_info, crew_info)
    title_crew_person = tf.title_crew_person(title_crew_info, crew_cast_personal_info)

    write_csv(tf.top_films_of_all_times(title_rating_info), 'top_films_of_all_times')
    write_csv(tf.top_films_of_last_10years(title_rating_info), 'top_films_of_last_10years')
    write_csv(tf.top_films_of_60s(title_rating_info), 'top_films_of_60s')
    write_csv(tf.top_films_by_genres(title_rating_info), 'top_films_by_genres')
    write_csv(tf.top_films_by_genres_decades(top_films_by_genres), 'top_films_by_genres_decades')
    write_csv(tf.top_actors(title_cast_person), 'top_actors')
    write_csv(tf.top_films_director(title_crew_person), 'top_films_director')
