import tensorflow_hub as hub
import numpy as np

url = "https://tfhub.dev/google/universal-sentence-encoder/4"
model = hub.load(url)

def use_universal_model(query):
    # Load the universal-sentence-encoder
    query_vec = np.asarray(model([query])[0]).tolist()
    return query_vec

def get_Model():
    return model
