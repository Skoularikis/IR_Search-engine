import pandas as pd
import json
from langdetect import detect
from nltk.corpus import stopwords
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer


# Run this if running into errors. Not needed every run
# import nltk
# nltk.download('stopwords')
# nltk.download('punkt')

def read_csv():
    return pd.read_csv("./preprocessed_book_data.csv")


def load_json(df):
    result = df.to_json(orient="table")
    parsed = json.loads(result)
    return parsed


def drop_columns(df, columns_to_drop):
    # Drop all unused columns

    df = df.drop(columns_to_drop, axis=1)
    return df


def dro_no_english_sentences(df):
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
    # Remove rows will null values for book_desc
    df = df[df['book_desc'].notnull()]

    # Stop word removal prior to indexing along with removal of punctuation in description, genres, and authors
    stop = stopwords.words('english')
    df['book_desc2'] = df['book_desc'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
    df['book_desc2'] = df['book_desc2'].str.replace('[^\w\s]', ' ')
    df['genres'] = df['genres'].str.replace('[^\w\s]', ' ')
    df['book_authors'] = df['book_authors'].str.replace('[^\w\s]', ' ')
    return df

def tf_idf_vctrz(df):
    vectrzr = TfidfVectorizer()
    vector = vectrzr.fit_transform(df['book_desc'])
    vec_array = vector.toarray()
    df['desc_vec_sparse'] = pd.DataFrame(((x,) for x in vec_array), columns=['lists'])
    return df
