from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, array_contains, explode, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('MyProject') \
    .getOrCreate()

titles_clue = spark.read.csv('venv/dataset/title.akas.tsv/data.tsv', sep=r'\t', header=True)

titles_info = spark.read.csv('venv/dataset/title.basics.tsv/data.tsv', sep=r'\t', header=True)

crew_info = spark.read.csv('venv/dataset/title.crew.tsv/data.tsv', sep=r'\t', header=True)

episode_info = spark.read.csv('venv/dataset/title.episode.tsv/data.tsv', sep=r'\t', header=True)

cast_info = spark.read.csv('venv/dataset/title.principals.tsv/data.tsv', sep=r'\t', header=True)

rating_info = spark.read.csv('venv/dataset/title.ratings.tsv/data.tsv', sep=r'\t', header=True)

crew_cast_personal_info = spark.read.csv('venv/dataset/name.basics.tsv/data.tsv', sep=r'\t', header=True)


def write_csv(data_frame, file_name):
    """ Function for write data frame into csv file"""
    data_frame.write.format('csv') \
        .option('header', True).mode('overwrite') \
        .save('D:\pythonProject1\outputs\outputs' + file_name)
