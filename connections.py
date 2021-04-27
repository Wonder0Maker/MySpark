from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('MyProject') \
    .master('local[*]') \
    .getOrCreate()


def read_tsv(file_name):
    '''
    Read data frame from tsv file
    '''
    dataset = spark.read.load(('dataset/' + file_name + '/data.tsv'),
                              format='csv',
                              header='true',
                              sep=r'\t',
                              inferSchema='true')

    return dataset


def write_csv(data_frame, file_name):
    '''
    Write data frame into csv file
    '''
    data_frame = data_frame.coalesce(1)
    data_frame.write.format('csv') \
        .option('inferSchema', True).mode('overwrite') \
        .save('outputs\outputs' + file_name)
