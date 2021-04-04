import pandas as pd
import json
from elasticsearch import Elasticsearch, helpers
from nltk.corpus import stopwords
from langdetect import detect
# Run this if running into errors. Not needed every run
# import nltk
# nltk.download('stopwords')

# Change the filepath to your filepath
df = pd.read_csv("./book_data.csv")


# Drop all unused columns, get rid of null rows for book_desc (needed for removal of stop words, consider dropping all
# nulls?)
df = df.drop(["book_edition", "book_format", "image_url","book_rating_count","book_review_count","book_pages"],axis=1)



langs = []

# Drop non english rows
for i in range(len(df)):
    try:
        lang = detect(df[i:(i+1)]["book_desc"][i])
    except:
        continue    
    if lang != "en":
        #df1.drop(index=i, axis=0)
        #print(lang)
        langs.append(i)
        
rows = df.index[langs]

df = df.drop(rows,axis=0)



df = df[df['book_desc'].notnull()]
# Stop word removal prior to indexing
stop = stopwords.words('english')
df['book_desc'] = df['book_desc'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
df['book_desc'] = df['book_desc'].str.replace('[^\w\s]', ' ')
# print(df["book_desc"])




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


res = es.get(index='test_index', id=0)
print(res['_source'])