from pyspark.sql import SparkSession
from pyspark.sql import *

spark = SparkSession.builder.appName('MyProject')\
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

titles_clue = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.akas.tsv/data.tsv',
                      sep=r'\t', header=True)

titles_info = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.basics.tsv/data.tsv',
                        sep=r'\t', header=True)

crew_info = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.crew.tsv/data.tsv',
                      sep=r'\t', header=True)

episode_info = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.episode.tsv/data.tsv',
                         sep=r'\t', header=True)

cast_info = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.principals.tsv/data.tsv',
                            sep=r'\t', header=True)

rating_info = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.ratings.tsv/data.tsv',
                         sep=r'\t', header=True)

crew_cast_personal_info = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\name.basics.tsv/data.tsv',
                         sep=r'\t', header=True)
