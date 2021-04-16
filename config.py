"""Flask configuration."""
from os import environ, path
from dotenv import load_dotenv

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))

class Config:
    """Base config."""
    SECRET_KEY = environ.get('SECRET_KEY')
    SESSION_COOKIE_NAME = environ.get('SESSION_COOKIE_NAME')
    STATIC_FOLDER = 'static'
    TEMPLATES_FOLDER = 'templates'

# Min score for the match
TF_IDF_INDEX_NAME = "books_tf_idf"


# Universal Sentence Encoder Tf Hub url
MODEL_URL = "https://tfhub.dev/google/universal-sentence-encoder/4"
# Index name for universal
universal_index_name = "books_universal"
# Min score for the match
SEARCH_THRESH = 1.2