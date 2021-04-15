import numpy as np
import tensorflow_hub as hub
from elasticsearch import Elasticsearch, helpers
from sklearn.feature_extraction.text import TfidfVectorizer
# from word_embeddings.universal import use_universal_model, get_Model
from tqdm import tqdm

import preprocessing

es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
url = "https://tfhub.dev/google/universal-sentence-encoder/4"
model = hub.load(url)
index_name = 'book_test'

def check_if_index_exist(index):
    return es.indices.exists(index=index_name)

def create_book_test_index():
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
                # "book_desc2": {
                #     "type": "text"
                # },
                # "desc_vec_sparse": {
                #     "type": "sparse_vector",
                #     "dims": 18333
                # },
                "desc_vec_dense": {
                    "type": "dense_vector",
                    "dims": 512
                }
            }
        }
    }
    try:
        # Create the index if not exists
        if not es.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es.indices.create(
                index=index_name, body=mapping  # ignore=[400, 404]
            )
            print("Created Index -> ", index_name)
        else:
            print("Index book test exists...")
    except Exception as ex:
        print(str(ex))


def insert_qa(body):
    if not es.indices.exists(index_name):
        create_book_test_index()
    # Insert a record into the es index
    es.index(index=index_name, body=body)


def process_books():
    df = preprocessing.read_csv()
    df['desc_vec_dense'] = df['book_desc'].apply(lambda x: model([x])[0].tolist())
    parsed = preprocessing.load_json(df)
    upload_data_to_elastic(parsed)
    # print("\nIndexing QA's...")
    # for index, row in tqdm(df.iterrows()):
    #     # Index each qa pair along with the question id and embedding
    #     insert_qa({
    #         'book_authors': row['book_authors'],
    #         'book_desc': row['book_desc'],
    #         'book_isbn': row['book_isbn'],
    #         'book_rating': row['book_rating'],
    #         'book_title': row['book_title'],
    #         'genres': row['genres'],
    #         'desc_vec_dense': np.asarray(model([row['book_desc']])[0]).tolist(),
    #         '_id': index
    #     })

def upload_data_to_elastic(parsed):
    # # Change the filepath to your filepath
    # df = preprocessing.read_csv()
    # # Drop all unused columns
    # columns_to_drop = ["book_edition", "book_format", "image_url", "book_rating_count", "book_review_count",
    #                    "book_pages"]
    # # df = preprocessing.drop_columns(df, columns_to_drop)
    # # df = preprocessing.drop_no_english_sentences(df)
    # df = preprocessing.remove_stopwords(df)
    # df = preprocessing.tf_idf_vctrz(df)
    # # Transform the DataFrame to json string
    # parsed = preprocessing.load_json(df)
    entries = []
    for i in range(0, len(parsed['data'])):
        source = parsed['data'][i]
        entry = {
            "_index": "books",
            "_id": i,
            "_source": source
        }
        entries.append(entry)
        if len(entries) >= 50:
            helpers.bulk(es, entries)
            entries = []
    if len(entries) > 0:
        helpers.bulk(es, entries)

    # Get one random result
    # res = es.get(index='test_index', id=0)
    # print(res['_source'])

def search_in_elasticsearch(search_term):
    res = es.search(
        index="books",
        size=20,
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

def cosine_similarity_search(search_term):
    v = TfidfVectorizer()
    search_term = [search_term]
    query_vec = v.fit_transform(search_term)
    query_vec = query_vec.toarray()

    res = es.search(
        index="books",
        size=20,
        body={
            "query": {
                "script_score": {
                    "query": {
                        "match_all": {}
                    },
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'desc_vec') + 1.0",
                        "params": {"query_vector": query_vec}
                    }
                }
            }
        }
    )

    return res

# cosine_similarity_search("this is a test")

process_books()
