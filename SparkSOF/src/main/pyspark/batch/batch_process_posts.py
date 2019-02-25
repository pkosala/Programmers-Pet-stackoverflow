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


conf = (SparkConf().setMaster("local").setAppName("SFO_Search"))
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

sfo_bucket = "s3a://insightstackoverflowsample/FinalData/Questions/Q1_000000000000.json"
numHashes = 10
maxShingleID = 2 ** 32 - 1
nextPrime = 4294967311


def pick_random_coeffs(k):
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
    code = re.sub(r'<[^>]+>', '', code, flags=re.DOTALL)  # remove all tags
    code = re.sub(r'\s+', ' ', code, flags=re.DOTALL)  # replace all new lines and multiple spaces with single space
    code = re.sub(r'[^\x00-\x7f]', r'', code)  # remove all non-ascii characters
    code = re.sub('[' + string.punctuation + ']', '', code)  # remove all punctuations
    return code.lower()


def get_description(body, title=''):
    if body is None or body == '':
        return None
    body = title +' '+body
    desc = re.sub('<code>.*?</code>', '', body, flags=re.DOTALL) # remove code from description
    desc = re.sub(r'<[^>]+>', '', desc, flags=re.DOTALL) # remove all tags
    desc = re.sub(r'\s+', ' ', desc, flags=re.DOTALL) # replace all new lines and multiple spaces with single space
    desc = re.sub(r'[^\x00-\x7f]', r'', desc) # remove all non-ascii characters
    desc = re.sub('[' + string.punctuation + ']', '', desc) # remove all punctuations
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


def generate_minhash_signatures(shingles):
    # TODO: make it generic to work for any no. of hash functions

    signature = []
    for i in range(0, numHashes):
        minHashCode = nextPrime + 1
        for shingleID in shingles:
            hashCode = (coeffA.value[i] * shingleID + coeffB.value[i]) % nextPrime
            # hashCode = shingleID//1000
            if hashCode < minHashCode:
                minHashCode = hashCode

        signature.append(minHashCode)
    return signature


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

df = sqlContext.read.json(sfo_bucket)
df = df.withColumn("id", df["id"].cast(IntegerType()))
df.show()

udf_get_tag_array = udf(get_tags,ArrayType(StringType()))
df = df.withColumn("tags", udf_get_tag_array("tags"))

df.show()

# udf_getCode = udf(get_code, StringType())
# df_with_code = df.withColumn("code", udf_getCode("body"))
# df_with_code.show()

udf_getDesc = udf(get_description, StringType())
df = df.withColumn("desc", udf_getDesc("body", "title"))
df = df.select('id', 'desc',  'title', 'tags')
df.show()

tokenizer = Tokenizer(inputCol="desc", outputCol="desc_tokens")
df = tokenizer.transform(df)

stop_words_remover = StopWordsRemover(inputCol="desc_tokens", outputCol="desc_stop_words_removed")
df = stop_words_remover.transform(df)

stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
df = df.withColumn("desc_stemmed", stem("desc_stop_words_removed"))
df.show()

udf_shingle = udf(generate_shingles, ArrayType(IntegerType()))
df = df.withColumn('shingles', udf_shingle("desc_stop_words_removed"))
df = df.select('id', 'shingles', 'title', 'tags')
df.show()

# coeffA = pick_random_coeffs(numHashes)
# coeffB = pick_random_coeffs(numHashes)
#
# coeffs = zip(coeffA, coeffB)
# coeff_dict = [{"coeffA":x[0],"coeffB":x[1]} for x in coeffs]
# coeff_schema = StructType([StructField('coeffA', IntegerType(), True), StructField('coeffB', IntegerType(), True)])
# coeff_df = sqlContext.createDataFrame(data = coeff_dict, schema = coeff_schema)

coeff_df = read_db("coefficients_post")
coeffA = sc.broadcast([int(row.coeffA) for row in coeff_df.select("coeffA").collect()])
coeffB = sc.broadcast([int(row.coeffB) for row in coeff_df.select("coeffB").collect()])


minhash_udf = udf(generate_minhash_signatures, ArrayType(IntegerType()))
# df = df.withColumn('minhash', minhash_udf("shingles", fn.array([fn.lit(x) for x in coeffA]), fn.array([fn.lit(x) for x in coeffB])))
df = df.withColumn('minhash', minhash_udf("shingles"))
df = df.select('id', 'minhash','title','tags')

df.show()

def insert_db(df, table_name, _mode):
    url = "jdbc:postgresql://ec2-3-94-26-85.compute-1.amazonaws.com/insight"
    properties = {
        "user": "pooja",
        "password": "123",
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(url=url, table=table_name, mode=_mode, properties=properties)

insert_db(df, "Post", "overwrite")
# insert_db(coeff_df, "coefficients_post", "overwrite")



