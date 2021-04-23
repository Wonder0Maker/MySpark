from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('MyProject').getOrCreate()

akas = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.akas.tsv/data.tsv',
                      sep=r'\t', header=True)

basics = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.basics.tsv/data.tsv',
                        sep=r'\t', header=True)

crew = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.crew.tsv/data.tsv',
                      sep=r'\t', header=True)

episode = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.episode.tsv/data.tsv',
                         sep=r'\t', header=True)

principals = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.principals.tsv/data.tsv',
                            sep=r'\t', header=True)

ratings = spark.read.csv('D:\\pythonProject1\\venv\\dataset\\title.ratings.tsv/data.tsv',
                         sep=r'\t', header=True)
