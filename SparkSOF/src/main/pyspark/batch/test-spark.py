from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
import pyspark.sql.functions as fn
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re
import string
import binascii
import random
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer
from pyspark.sql import DataFrameReader

import nltk

nltk.download("wordnet")
from nltk.stem import WordNetLemmatizer
from nltk import ngrams

import boto3

import time
start_time = time.time()


client = boto3.client('s3')
resource = boto3.resource('s3')
bucket_name = 'insightstackoverflowsample'
bucket = resource.Bucket(bucket_name)

conf = (SparkConf().setMaster("local").setAppName("SFO_Search"))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

# sfo_bucket = "s3a://insightstackoverflowsample/FinalData/Questions/Q1_000000000000.json"
numHashes = 10
maxShingleID = 2 ** 32 - 1
nextPrime = 4294967311


def pick_random_coeffs(k):
    #  TODO: Move this to initialize DB
    randList = []
    while k > 0:
        randIndex = random.randint(0, maxShingleID)

        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID)

        randList.append(randIndex)
        k = k - 1

    return randList


def get_code(body):
    if body is None or body == '':
        return None
    code = re.findall(r'<code>(.*?)</code>', body, re.DOTALL) # get only code from the body
    code = re.sub('\s+', ' ', ' '.join(code)) # replace all new lines and multiple spaces with single space
    # code = re.sub(r'[^\x00-\x7f]', r'', code) # remove all non-ascii characters
    # return code.lower()
    code = re.sub(r'<[^>]+>', ' ', code, flags=re.DOTALL)  # remove all tags
    code = re.sub(r'\s+', ' ', code, flags=re.DOTALL)  # replace all new lines and multiple spaces with single space
    code = re.sub(r'[^\x00-\x7f]', r' ', code)  # remove all non-ascii characters
    code = re.sub('[' + string.punctuation + ']', ' ', code)  # remove all punctuations
    code = re.sub(r'[^A-Za-z]', ' ', code)
    code = re.sub(r'\s+', ' ', code, flags=re.DOTALL)  # replace all new lines and multiple spaces with single space

    return code.lower()


def get_description(body, title=''):
    if body is None or body == '':
        return None
    body = title + ' ' + body
    desc = re.sub('<code>.*?</code>', '', body, flags=re.DOTALL)  # remove code from description
    desc = re.sub(r'<[^>]+>', '', desc, flags=re.DOTALL)  # remove all tags
    desc = re.sub(r'\s+', ' ', desc, flags=re.DOTALL)  # replace all new lines and multiple spaces with single space
    desc = re.sub(r'[^\x00-\x7f]', r'', desc)  # remove all non-ascii characters
    desc = re.sub('[' + string.punctuation + ']', '', desc)  # remove all punctuations
    return desc.lower()


def get_tags(tags):
    tag_array = str(tags).split('|')
    tag_array = [x.lower() for x in tag_array]
    return tag_array


# remove stem words from descriptioon
def lemmatize(tokens):
    wordnet_lemmatizer = WordNetLemmatizer()
    stems = [wordnet_lemmatizer.lemmatize(token) for token in tokens if len(token) > 1]
    return tuple(stems)


def generate_shingles(desc, shingle_size=2):
    # TODO: make it generic to work for any shingle size
    # words_in_title = desc.split()
    shinglesInDoc = set()
    n_grams = ngrams(desc, shingle_size)
    for each_gram in n_grams:
        shingle = ' '.join(each_gram)
        crc = binascii.crc32(shingle.encode()) & 0xffffffff

        shinglesInDoc.add(crc)
    return list(shinglesInDoc)


def generate_minhash_signatures(shingles, coeffA, coeffB):
    # TODO: make it generic to work for any no. of hash functions

    signature = []
    for i in range(0, numHashes):
        minHashCode = nextPrime + 1
        for shingleID in shingles:
            hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime
            # hashCode = shingleID//1000
            if hashCode < minHashCode:
                minHashCode = hashCode

        signature.append(minHashCode)
    return signature


def read_filenames(bucket, prefix):
    prefix_objs = bucket.objects.filter(Prefix=prefix)
    print(prefix_objs)
    file_list = []

    for obj in prefix_objs:
        key = obj.key
        if key.endswith(".json"):
            file_list.append(key)
    return file_list


def insert_db(df, table_name, _mode):
    url = "jdbc:postgresql://ec2-3-94-26-85.compute-1.amazonaws.com/insight"
    properties = {
        "user": "pooja",
        "password": "123",
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(url=url, table=table_name, mode=_mode, properties=properties)


def read_db(table_name):
    url = "jdbc:postgresql://ec2-3-94-26-85.compute-1.amazonaws.com/insight"
    properties = {
        "user": "pooja",
        "password": "123",
        "driver": "org.postgresql.Driver"
    }
    df = DataFrameReader(sqlContext).jdbc(
        url=url, table=table_name, properties=properties
    )
    return df


# TODO: can do better instead of converting them to lists
# coeff_df = read_db("coefficients_post")
# coeffA = [int(row.coeffA) for row in coeff_df.select("coeffA").collect()]
# coeffB = [int(row.coeffB) for row in coeff_df.select("coeffB").collect()]

# print(coeffA)
file_list = read_filenames(bucket, "FinalData/Questions/")

for each_file in file_list:
    full_path = "s3a://" + bucket_name + "/" + each_file
    print("===========================  " + full_path + " ==================================")

    df = sqlContext.read.json(full_path)
    df = df.withColumn("id", df["id"].cast(LongType()))
    d
    max_val = df.agg({"id": "max"}).collect()[0]["max(id)"]
    min_val = df.agg({"id": "min"}).collect()[0]["min(id)"]
    print(each_file, max_val,min_val)
print("Total Time --- %s seconds ---" % (time.time() - start_time))

f.show()