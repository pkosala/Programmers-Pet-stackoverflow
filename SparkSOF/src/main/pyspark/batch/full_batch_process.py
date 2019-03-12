from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer
from databaseutil import DatabaseOperations

from util import get_tags, get_description, generate_shingles,read_filenames, numHashes, nextPrime, lemmatize, find_lsh_buckets
import config
import boto3
import time

resource = boto3.resource('s3')
bucket = resource.Bucket(config.S3_BUCKET_BATCH_RAW)


start_time = time.time()


conf = (SparkConf().setMaster(config.SPARK_MASTER_URL).setAppName(config.SPARK_APP_NAME))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)


def generate_minhash_signatures(shingles):
    # TODO: make it generic to work for any no. of hash functions

    signature = []
    for i in range(0, numHashes):
        minHashCode = nextPrime + 1
        for shingleID in shingles:
            hashCode = (coeffA.value[i] * shingleID + coeffB.value[i]) % nextPrime
            if hashCode < minHashCode:
                minHashCode = hashCode

        signature.append(minHashCode)
    return signature


db_connector = DatabaseOperations()

coeff_df = db_connector.db_read("coefficients_post", sqlContext)
coeffA = sc.broadcast([int(row.coeffA) for row in coeff_df.select("coeffA").collect()])
coeffB = sc.broadcast([int(row.coeffB) for row in coeff_df.select("coeffB").collect()])

file_list = read_filenames(bucket, "FinalData/Questions/")
file_list = ["s3a://" + config.S3_BUCKET_BATCH_RAW + "/" + each_file for each_file in file_list]
N = 4
subList = [file_list[n:n+N] for n in range(0, len(file_list), N)]

for sub in subList:
    multi_path = ', '.join('"{0}"'.format(path) for path in sub)
    print("===========================  " + multi_path + " ==================================")

    df = sqlContext.read.json(sub)
    df = df.withColumn("id", df["id"].cast(LongType()))
    df.show()

    udf_get_tag_array = udf(get_tags, ArrayType(StringType()))
    df = df.withColumn("tags", udf_get_tag_array("tags"))

    df.show()

    # udf_getCode = udf(get_code, StringType())
    # df_with_code = df.withColumn("code", udf_getCode("body"))
    # df_with_code.show()

    udf_getDesc = udf(get_description, StringType())
    df = df.withColumn("desc", udf_getDesc("body", "title"))
    df = df.select('id', 'desc', 'title', 'tags')
    df.show()

    tokenizer = Tokenizer(inputCol="desc", outputCol="desc_tokens")
    df = tokenizer.transform(df)

    stop_words_remover = StopWordsRemover(inputCol="desc_tokens", outputCol="desc_stop_words_removed")
    df = stop_words_remover.transform(df)

    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    df = df.withColumn("desc_stemmed", stem("desc_stop_words_removed"))
    df.show()

    udf_shingle = udf(generate_shingles, ArrayType(LongType()))
    df = df.withColumn('shingles', udf_shingle("desc_stop_words_removed"))
    df = df.select('id', 'shingles', 'title', 'tags')
    df.show()

    minhash_udf = udf(generate_minhash_signatures, ArrayType(LongType()))
    df = df.withColumn('minhash', minhash_udf("shingles"))
    df = df.select('id', 'minhash', 'title', 'tags')

    lsh_udf = udf(find_lsh_buckets, ArrayType(LongType()))
    df = df.withColumn('lsh', lsh_udf("minshash"))
    df = df.select('id', 'minhash', 'title', 'tags', 'lsh')

    df.show()

    db_connector.db_write(df, "Post_batch", "append")
    print("--- %s seconds ---" % (time.time() - start_time))

print("Total Time --- %s seconds ---" % (time.time() - start_time))

