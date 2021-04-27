from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, array_contains, explode, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('MyProject') \
    .getOrCreate()


def read_tsv(file_name):
    dataset = spark.read.csv(('dataset/' + file_name + '/data.tsv'), sep=r'\t', header=True)
    return dataset


def write_csv(data_frame, file_name):
    """ Function for write data frame into csv file"""
    data_frame.write.format('csv') \
        .option('inferSchema', True).mode('overwrite') \
        .save('outputs\outputs' + file_name)


titles_clue = read_tsv('title.akas.tsv')
titles_info = read_tsv('title.basics.tsv')
crew_info = read_tsv('title.crew.tsv')
episode_info = read_tsv('title.episode.tsv')
cast_info = read_tsv('title.principals.tsv')
rating_info = read_tsv('title.ratings.tsv')
crew_cast_personal_info = read_tsv('name.basics.tsv')
