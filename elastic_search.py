import pandas as pd
import json
from elasticsearch import Elasticsearch, helpers

# Change the filepath to your filepath
df = pd.read_csv("./book_data.csv")

# Drop all unused columns
df = df.drop(["book_edition", "book_format", "image_url","book_rating_count","book_review_count","book_pages"],axis=1)
# Transform the DataFrame to json string
result = df.to_json(orient="table")
parsed = json.loads(result)

# Create the elastic search instance
es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
# Bulk upload the json file to the test_index
entries=[]
for i in range(0,len(parsed['data'])):
    source = parsed['data'][i]
    entry = {
        "_index": "test_index",
        "_id": i,
        "_source": source
            }
    entries.append(entry)
    if len(entries) >= 50:
        helpers.bulk(es, entries)
        entries=[]
if len(entries) > 0:
    helpers.bulk(es, entries)


res = es.get(index='test_index',id=0)   
print(res['_source'])