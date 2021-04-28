from top_films_director import top_films_director
from connections import write_csv

if __name__ == '__main__':
    write_csv(top_films_director(), 'top_films_director')
