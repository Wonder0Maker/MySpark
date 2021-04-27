import top_films_by_genres
import connections as con

if __name__ == "__main__":
    con.write_csv(top_films_by_genres.top_films_by_genres(), 'top_films_by_genres')
