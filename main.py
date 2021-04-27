import top_films_by_genres_decades
import connections as con

if __name__ == "__main__":
    con.write_csv(top_films_by_genres_decades.top_films_by_genres_decades(),
                  'top_films_by_genres_decades')
