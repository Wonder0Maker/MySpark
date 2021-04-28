from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('MyProject') \
    .master('local[*]') \
    .getOrCreate()


def read_tsv(file_name):
    """
    Read data frame from tsv file
    """
    dataset = spark.read.load(('dataset/{}/data.tsv'.format(file_name)),
                              format='csv',
                              header='true',
                              sep=r'\t',
                              inferSchema='true')

    return dataset


def write_csv(data_frame, file_name):
    """
    Write data frame into csv file
    """
    data_frame = data_frame.coalesce(1)
    data_frame.write.format('csv') \
        .option('header', True).mode('overwrite') \
        .save('outputs\outputs{}'.format(file_name))
