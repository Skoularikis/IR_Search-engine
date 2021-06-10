import numpy as np
import tensorflow_hub as hub
from elasticsearch import Elasticsearch, helpers
import preprocessing
import os

es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

def check_if_index_exist(index):
    return es.indices.exists(index=index)


def upload_data_to_elastic():
    # Change the filepath to your filepath
    df = preprocessing.read_csv()
    # Drop all unused columns
    columns_to_drop = ["book_edition", "book_format", "image_url", "book_rating_count", "book_review_count",
                       "book_pages"]
    df = preprocessing.drop_columns(df, columns_to_drop)
    df = preprocessing.drop_no_english_sentences(df)
    df = preprocessing.remove_stopwords(df)
    # Transform the DataFrame to json string
    parsed = preprocessing.load_json(df)
=======


es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
default_index = 'books_default_index'
universal_index_name = 'book_universal'
tf_idf_index_name = "books_tf_idf"
url = "https://tfhub.dev/google/universal-sentence-encoder/4"
model = hub.load("../model")
csv_path = " "

def create_indices(path_to_preprocessed_csv = "./preprocessed_book_data_final.csv",
                   path_to_original_csv="./book_data.csv"):
    global csv_path
    if not es.ping():
        raise ValueError("Connection failed")
    if os.path.isfile(path_to_preprocessed_csv):
        print("Preprocessed file exists")
        csv_path = path_to_preprocessed_csv
    else:
        if not os.path.isfile(path_to_original_csv):
            print("Please provide the application with the original dataset")
            return
        else:
            print("Preprocessing the original file. This will take some time...")
            df = preprocessing.read_csv(path_to_original_csv)
            df = preprocessing.drop_columns(df)
            df = preprocessing.remove_not_null(df)
            df = preprocessing.remove_non_english_words(df)
            df = preprocessing.drop_no_english_sentences(df)  # language detect
            df = preprocessing.remove_stopwords_lemmetize(df)
            csv_path = path_to_preprocessed_csv
            preprocessing.save_to_csv(df, csv_path)

    if not es.indices.exists(default_index):
        upload_to_default_index()
    if not es.indices.exists(tf_idf_index_name):
        upload_to_elasticsearch_with_tf_idf()
    if not es.indices.exists(universal_index_name):
        upload_to_elasticsearch_with_universal()

    return True


def create_tf_idf_index():
    # Define the index mapping
    config = {
        "settings": {
            "number_of_shards": 1,
            "similarity": {
                "scripted_tfidf": {
                    "type": "scripted",
                    "script": {
                        "source": "double tf = Math.sqrt(doc.freq); double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; double norm = 1/Math.sqrt(doc.length); return query.boost * tf * idf * norm;"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "book_authors": {
                    "type": "text",
                    "similarity": "scripted_tfidf"
                },
                "book_desc": {
                    "type": "text",
                    "similarity": "scripted_tfidf"
                },
                "book_isbn": {
                    "type": "text"
                },
                "book_rating": {
                    "type": "float"
                },
                "book_title": {
                    "type": "text",
                    "similarity": "scripted_tfidf"
                },
                "genres": {
                    "type": "text"
                },
                "book_desc_original": {
                    "type": "text"
                }
            }
        }
    }
    try:
        # Create the index if not exists
        if not es.indices.exists(tf_idf_index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es.indices.create(
                index=tf_idf_index_name, body=config
            )
            print("Created Index -> ", tf_idf_index_name)
        else:
            print("Index " + tf_idf_index_name + " exists...")
    except Exception as ex:
        print(str(ex))


def create_universal_index():
    # Define the index mapping
    mapping = {
        "mappings": {
            "properties": {
                "book_authors": {
                    "type": "text"  # formerly "string"
                },
                "book_desc": {
                    "type": "text"  # formerly "string"
                },
                "book_isbn": {
                    "type": "text"
                },
                "book_rating": {
                    "type": "float"
                },
                "book_title": {
                    "type": "text"
                },
                "genres": {
                    "type": "text"
                },
                "book_desc_original": {
                    "type": "text"
                },
                "desc_vec_dense": {
                    "type": "dense_vector",
                    "dims": 512
                },

            }
        }
    }
    try:
        # Create the index if not exists
        if not es.indices.exists(universal_index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es.indices.create(
                index=universal_index_name, body=mapping  # ignore=[400, 404]
            )
            print("Created Index -> ", universal_index_name)
        else:
            print("Index book test exists...")
    except Exception as ex:
        print(str(ex))


def upload_to_default_index():
    df = preprocessing.read_csv(csv_path)
    df.dropna(inplace=True, subset=["book_desc"])
    parsed = preprocessing.load_json(df)
    es.indices.create(default_index)
    print("Created Index -> ", default_index)
    upload_data_to_elastic(parsed, default_index)
    print("\n Uploaded to elasticsearch with index", default_index)


def upload_to_elasticsearch_with_tf_idf():
    df = preprocessing.read_csv(csv_path)
    df.dropna(inplace=True, subset=["book_desc"])
    parsed = preprocessing.load_json(df)
    create_tf_idf_index()
    upload_data_to_elastic(parsed, tf_idf_index_name)
    print("\n Uploaded to elasticsearch with index", tf_idf_index_name)


def upload_to_elasticsearch_with_universal():
    df = preprocessing.read_csv(csv_path)
    df.dropna(inplace=True, subset=["book_desc"])
    df['desc_vec_dense'] = df['book_desc'].apply(lambda x: np.squeeze(np.asarray(model([x])[0])))
    parsed = preprocessing.load_json(df)
    create_universal_index()
    upload_data_to_elastic(parsed, universal_index_name)
    print("\n Uploaded to elasticsearch with index", universal_index_name)


def upload_data_to_elastic(parsed, index_name):

    entries = []
    for i in range(0, len(parsed['data'])):
        source = parsed['data'][i]
        entry = {
            "_index": index_name,
            "_id": i,
            "_source": source
        }
        entries.append(entry)
        if len(entries) >= 50:
            helpers.bulk(es, entries)
            entries = []
    if len(entries) > 0:
        helpers.bulk(es, entries)



def search_in_elasticsearch(search_term):
    res = es.search(
        index="books",
        size=20,


def search_in_elasticsearch_with_default_index(search_term):
    result = es.search(
        index=default_index,
        body={
            "query": {
                "multi_match": {
                    "query": search_term,
                    "fields": [
                        "book_authors",
                        "book_desc",
                        "book_title"
                    ]
                }
            }
        }

    )
    for res in result['hits']['hits']:
        print(res['_score'])
    return result


def search_in_elasticsearch_with_tf_idf(search_term):
    result = es.search(
        index="books_tf_idf",
>>>>>>> latest_version
        body={
            "query": {
                "multi_match": {
                    "query": search_term,
                    "fields": [
                        "book_authors",
                        "book_desc",
                        "book_title"
                    ]
                }
            }
        }

    )
    return res




def search_in_elasticsearch_with_universal_index(search_term):
    query_vector = np.squeeze(np.asarray(model([search_term])))
    s_body = {
        "query": {
            "script_score": {
                "query": {
                    "match_all": {}
                },
                "script": {
                    "source": "cosineSimilarity(params.queryVector, 'desc_vec_dense') + 1.0",
                    "params": {
                        "queryVector": query_vector
                    }
                }
            }
        }
    }

    # Semantic vector search with cosine similarity
    result = es.search(index=universal_index_name, body=s_body)
    for res in result['hits']['hits']:
        print(res['_score'])
    return result



