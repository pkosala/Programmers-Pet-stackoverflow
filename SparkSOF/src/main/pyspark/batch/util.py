import random
import re
import string
import binascii
import config
import nltk
nltk.download("wordnet")
from nltk.stem import WordNetLemmatizer
from nltk import ngrams


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
    shinglesInDoc = set()
    n_grams = ngrams(desc, shingle_size)
    for each_gram in n_grams:
        shingle = ' '.join(each_gram)
        crc = binascii.crc32(shingle.encode()) & 0xffffffff

        shinglesInDoc.add(crc)
    return list(shinglesInDoc)


def read_filenames(bucket, prefix_folder):
    prefix_objs = bucket.objects.filter(Prefix=prefix_folder)
    print(prefix_objs)
    file_list = []

    for obj in prefix_objs:
        key = obj.key
        if key.endswith(".json"):
            file_list.append(key)
    return file_list
