import top_films
import connections as con

if __name__ == "__main__":
    con.write_csv(top_films.top_films_of_all_times(), 'top_films_of_all_times')
    con.write_csv(top_films.top_films_of_last_10years(), 'top_films_of_last_10years')
    con.write_csv(top_films.top_films_of_60s(), 'top_films_of_60s')
