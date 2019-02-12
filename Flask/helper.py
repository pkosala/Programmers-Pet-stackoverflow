import psycopg2
import re
import string
import binascii
from psycopg2.extras import RealDictCursor
import nltk
from nltk.corpus import stopwords
en_stops = set(stopwords.words('english'))
nltk.download("wordnet")
nltk.download("stopwords")
nltk.download('punkt')
from nltk.stem import WordNetLemmatizer
from nltk import ngrams
from nltk import word_tokenize

maxShingleID = 2 ** 32 - 1
nextPrime = 4294967311


def getCoefficients():
    coeffA, coeffB =[],[]
    conn = None
    try:
        # TODO: remove connection statements and make it common
        conn = psycopg2.connect(host="ec2-3-94-26-85.compute-1.amazonaws.com", database="insight", user="pooja",
                                password="123")
        cur = conn.cursor()
        cur.execute("SELECT \"coeffA\", \"coeffB\" FROM coefficients")
        rows = cur.fetchall()

        coeffA = [i[0] for i in rows]
        coeffB = [i[1] for i in rows]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return coeffA, coeffB


def get_similar_posts(signature_of_query, tags_string, limit_count):
    query_vector = ','.join([str(i) for i in signature_of_query])
    tags_vector = ','.join([str(i) for i in [x.strip().lower() for x in tags_string.split(',')]])
    # jaccard_similarity_query = """ with cte_compare as (select id, title,ROW_NUMBER () OVER (ORDER BY jaccard_sim) as disp_order
    #                             from (
    #                             select id, title,
    #                             round(array_upper(minhash & ARRAY[""" + query_vector+"""], 1 )/(array_upper(minhash | ARRAY[""" + query_vector+"""] , 1 ) *1.0), 3) as jaccard_sim
    #                             from post where ARRAY[""" + query_vector+"""] && minhash AND tags @> '{"""+tags_vector+"""}'
    #                             ) t
    #                             order by jaccard_sim desc
    #                             limit 10
    #                             )
    #                             select id, title, disp_order FROM cte_compare order by disp_order;"""
    jaccard_similarity_query = """with match_all_tags as (select id, title,ROW_NUMBER () OVER (ORDER BY jaccard_sim ) as disp_order, 1 as query_type, jaccard_sim
                                from (
                                select id, title,
                                round(array_upper(minhash & ARRAY[""" + query_vector+"""], 1 )/(array_upper(minhash | ARRAY[""" + query_vector+"""] , 1 ) *1.0), 3) as jaccard_sim
                                from post where tags @> '{"""+tags_vector+"""}' AND ARRAY[""" + query_vector+"""] && minhash 
                                ) t 
                                order by jaccard_sim desc
                                limit """+str(limit_count)+""" 
                                ),match_any_tag as (select id, title,ROW_NUMBER () OVER (ORDER BY jaccard_sim) as disp_order, 2 as query_type, jaccard_sim
                                from (
                                select id, title,
                                round(array_upper(minhash & ARRAY[""" + query_vector+"""], 1 )/(array_upper(minhash | ARRAY[""" + query_vector+"""] , 1 ) *1.0), 3) as jaccard_sim
                                from post where tags && '{"""+tags_vector+"""}' AND ARRAY[""" + query_vector+"""] && minhash AND id not in (SELECT id from match_all_tags)
                                ) t 
                                order by jaccard_sim desc
                                limit """+str(limit_count)+""" 
                                )
                                SELECT id, title, jaccard_sim, (query_type*10)+disp_order as disp_order 
                                from (
                                select id, title, disp_order,query_type,((jaccard_sim::numeric)::text) as jaccard_sim FROM match_all_tags
                                union 
                                select id, title, disp_order,query_type,(jaccard_sim::numeric)::text as jaccard_sim FROM match_any_tag
                                ) T
                                order by (query_type*10)+disp_order limit """+str(limit_count)+""" ;"""

    print(jaccard_similarity_query)
    conn = None
    try:
        # TODO: remove connection statements and make it common
        conn = psycopg2.connect(host="ec2-3-94-26-85.compute-1.amazonaws.com", database="insight", user="pooja",
                                password="123")
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(jaccard_similarity_query)
        rows = cur.fetchall()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return rows
    # return "This is needs a fix"


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


def generate_minhash_signature(shingles_in_query, coeffA, coeffB):
    numHashes = len(coeffB)
    signature_of_cur_doc = []
    for i in range(0, numHashes):
        minHashCode = nextPrime + 1
        for shingleID in shingles_in_query:
            hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime

            if hashCode < minHashCode:
                minHashCode = hashCode
        signature_of_cur_doc.append(minHashCode)
    return signature_of_cur_doc


def get_processed_query(query):
    desc = re.sub(r'\s+', ' ', query, flags=re.DOTALL)  # replace all new lines and multiple spaces with single space
    desc = re.sub(r'[^\x00-\x7f]', r'', desc)  # remove all non-ascii characters
    desc = re.sub('[' + string.punctuation + ']', '', desc)  # remove all punctuations
    desc = desc.lower()

    nltk_tokens = word_tokenize(desc)
    desc_stop_words_removed = []
    for word in nltk_tokens:
        if word not in en_stops:
            desc_stop_words_removed.append(word)

    wordnet_lemmatizer = WordNetLemmatizer()
    desc_stemmed = [wordnet_lemmatizer.lemmatize(token) for token in desc_stop_words_removed if len(token) > 1]

    return desc_stemmed


def process_query_get_results(query,tags_string):
    coeffA, coeffB = getCoefficients()
    processed_query = get_processed_query(query)
    shingles_in_query = generate_shingles(processed_query)
    signature_of_query = generate_minhash_signature(shingles_in_query, coeffA, coeffB)

    similar_posts = get_similar_posts(signature_of_query, tags_string,10)

    return similar_posts
