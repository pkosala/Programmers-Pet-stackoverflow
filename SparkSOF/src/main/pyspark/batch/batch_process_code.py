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


import nltk
nltk.download("wordnet")
from nltk.stem import WordNetLemmatizer
from nltk import ngrams


conf = (SparkConf().setMaster("local").setAppName("SFO_Search"))
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

sfo_bucket = "s3a://insightstackoverflowsample/FinalData/Questions/Q1_000000000000.json"
numHashes = 10
maxShingleID = 2 ** 32 - 1
nextPrime = 4294967311


def pick_random_coeffs( k):
    #  TODO: Move this to initialize DB
    randList = []
    while k > 0:
        # Get a random shingle ID.
        randIndex = random.randint(0, maxShingleID)

        # Ensure that each random number is unique.
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID)

        # Add the random number to the list.
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

def get_tags(tags):
    tag_array = str(tags).split('|')
    tag_array = [x.lower() for x in tag_array]
    return tag_array


# remove stem words from code
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




df = sqlContext.read.json(sfo_bucket)
df = df.withColumn("id", df["id"].cast(IntegerType()))
df.show()

udf_get_tag_array = udf(get_tags,ArrayType(StringType()))
df = df.withColumn("tags", udf_get_tag_array("tags"))

df.show()

udf_getCode = udf(get_code, StringType())
df = df.withColumn("code", udf_getCode("body"))
df.show()

df = df.select('id', 'code', 'title', 'tags')
df = df.withColumn("code_length", fn.length('code'))
df = df.filter(fn.col('code_length') > 50)
df.show()

tokenizer = Tokenizer(inputCol="code", outputCol="code_tokens")
df = tokenizer.transform(df)

stop_words_remover = StopWordsRemover(inputCol="code_tokens", outputCol="code_stop_words_removed")
df = stop_words_remover.transform(df)

stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
df = df.withColumn("code_stemmed", stem("code_stop_words_removed"))
df.show()

udf_shingle = udf(generate_shingles, ArrayType(LongType()))
df = df.withColumn('shingles', udf_shingle("code_stop_words_removed"))
df = df.select('id', 'shingles', 'title', 'tags')
df.show()

coeffA = pick_random_coeffs(numHashes)
coeffB = pick_random_coeffs(numHashes)

coeffs = zip(coeffA, coeffB)
coeff_dict = [{"coeffA":x[0],"coeffB":x[1]} for x in coeffs]
coeff_schema = StructType([StructField('coeffA', LongType(), True), StructField('coeffB', LongType(), True)])
coeff_df = sqlContext.createDataFrame(data = coeff_dict,schema = coeff_schema)


minhash_udf = udf(generate_minhash_signatures, ArrayType(LongType()))
df = df.withColumn('minhash', minhash_udf("shingles", fn.array([fn.lit(x) for x in coeffA]), fn.array([fn.lit(x) for x in coeffB])))
df = df.select('id', 'minhash', 'title', 'tags')
df.show()

def insert_db(df, table_name, _mode):
    url = "jdbc:postgresql://ec2-3-94-26-85.compute-1.amazonaws.com/insight"
    properties = {
        "user": "pooja",
        "password": "123",
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(url=url, table=table_name, mode=_mode, properties=properties)


insert_db(df, "Post_code", "overwrite")
insert_db(coeff_df, "coefficients_code", "overwrite")



