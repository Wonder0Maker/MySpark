from top_films import top_films_of_all_times, \
    top_films_of_last_10years, top_films_of_60s
from connections import write_csv

if __name__ == '__main__':
    write_csv(top_films_of_all_times(), 'top_films_of_all_times')
    write_csv(top_films_of_last_10years(), 'top_films_of_last_10years')
    write_csv(top_films_of_60s(), 'top_films_of_60s')
