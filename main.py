from top_actors import top_actors
from connections import write_csv

if __name__ == '__main__':
    write_csv(top_actors(), 'top_actors')
