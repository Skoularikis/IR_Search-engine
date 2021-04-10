import pandas as pd
import json
from langdetect import detect
from nltk.corpus import stopwords
# Run this if running into errors. Not needed every run
# import nltk
# nltk.download('stopwords')

def read_csv():
    return pd.read_csv("./book_data.csv")


def load_json(df):
    result = df.to_json(orient="table")
    parsed = json.loads(result)
    return parsed


def drop_columns(df, columns_to_drop):
    # Drop all unused columns
    df = df.drop(columns_to_drop, axis=1)
    return df


def drop_no_english_sentences(df):
    langs = []

    # Drop non english rows
    for i in range(len(df)):
        try:
            lang = detect(df[i:(i + 1)]["book_desc"][i])
        except:
            continue
        if lang != "en":
            # df1.drop(index=i, axis=0)
            # print(lang)
            langs.append(i)

    rows = df.index[langs]

    df = df.drop(rows, axis=0)
    return df

def remove_stopwords(df):

    df = df[df['book_desc'].notnull()]
    # Stop word removal prior to indexing
    stop = stopwords.words('english')
    df['book_desc'] = df['book_desc'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
    df['book_desc'] = df['book_desc'].str.replace('[^\w\s]', ' ')
    return df

