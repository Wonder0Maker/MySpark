from top_films_by_genres_decades import top_films_by_genres_decades
from connections import write_csv

if __name__ == '__main__':
    write_csv(top_films_by_genres_decades(), 'top_films_by_genres')

