from elasticsearch import Elasticsearch, helpers
import preprocessing
from sklearn.feature_extraction.text import TfidfVectorizer

es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

def check_if_index_exist(index):
    return es.indices.exists(index=index)


def upload_data_to_elastic():
    # Change the filepath to your filepath
    df = preprocessing.read_csv()
    # Drop all unused columns
    columns_to_drop = ["book_edition", "book_format", "image_url", "book_rating_count", "book_review_count",
                       "book_pages"]
    # df = preprocessing.drop_columns(df, columns_to_drop)
    # df = preprocessing.drop_no_english_sentences(df)
    df = preprocessing.remove_stopwords(df)
    df = preprocessing.tf_idf_vctrz(df)
    # Transform the DataFrame to json string
    parsed = preprocessing.load_json(df)
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
                        "match_all":{}
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