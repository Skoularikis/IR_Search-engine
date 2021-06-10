import pandas as pd
import json
from langdetect import detect
from nltk.corpus import stopwords
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer
import string
from nltk.stem import WordNetLemmatizer

# Run this if running into errors. Not needed every run
# nltk.download('stopwords')
# nltk.download('punkt')
# nltk.download('words')
# nltk.download('wordnet')

def read_csv(filepath):
    return pd.read_csv(filepath)

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

def remove_not_null(df):
    df = df[df["book_desc"].notnull()]
    return df

def remove_non_english_words(df):
    english_words = set(nltk.corpus.words.words())
    df['book_desc'] = df['book_desc'].apply(lambda x: ' '.join(
        [word for word in nltk.wordpunct_tokenize(x) if word.lower() in english_words]))
    return df

def drop_columns(df):
    # Drop all unused columns
    columns_to_drop = ["book_edition", "book_format", "image_url", "book_rating_count", "book_review_count",
                       "book_pages"]
    df = df.drop(columns_to_drop, axis=1)
    return df

def drop_no_english_sentences(df):
    langs = []
    # Drop non english rows
    for i in range(len(df)):
        try:
            lang = detect(df["book_desc"][i])

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

    rows = df.index[langs]
    df = df.drop(rows, axis=0)
    df['book_desc_original'] = df['book_desc']
    return df

def remove_stopwords_lemmetize(df):
    # Remove rows will null values for book_desc
    lemmatizer = WordNetLemmatizer()
    stop = set(stopwords.words('english'))
    df['book_desc'] = df['book_desc'].apply(
        lambda x: ' '.join([lemmatizer.lemmatize(word.strip().lower()) for word in nltk.wordpunct_tokenize(x) if word not in stop and word not in string.punctuation]))
    df['book_desc'] = df['book_desc'].str.replace('[^\w\s]', ' ')
    df['genres'] = df['genres'].str.replace('[^\w\s]', ' ')
    df['book_authors'] = df['book_authors'].str.replace('[^\w\s]', ' ')
    return df

def rows_with_long_desc(df,length):
    df = df[df['book_desc'].apply(lambda x: len(x) < length)]
    df.reset_index(drop=True, inplace=True)
    return df


def save_to_csv(df,filename):
    df.to_csv(filename, index=False)

def tf_idf_vctrz(df):
    vectrzr = TfidfVectorizer()
    vector = vectrzr.fit_transform(df['book_desc'])
    vec_array = vector.toarray()
    df['desc_vec_sparse'] = pd.DataFrame(((x,) for x in vec_array), columns=['lists'])
    return df

