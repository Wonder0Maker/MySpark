from top_films_by_genres import top_films_by_genres
from connections import write_csv

if __name__ == '__main__':
    write_csv(top_films_by_genres(), 'top_films_by_genres')
