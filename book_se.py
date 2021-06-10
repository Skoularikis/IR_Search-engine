from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
import elasticsearch_operations


app = Flask(__name__)
# Load configs


@app.route('/')
def search():
    elasticsearch_operations.create_indices()
    return render_template('search.html')


@app.route('/book_results', methods=['POST', 'GET'])
def results():

    if request.method == 'POST':
        search_term = request.form["input"]
        res_default = elasticsearch_operations.search_in_elasticsearch_with_default_index(search_term)
        res = elasticsearch_operations.search_in_elasticsearch_with_tf_idf(search_term)
        res_universal = elasticsearch_operations.search_in_elasticsearch_with_universal_index(search_term)
        return render_template("result.html", search_term=search_term, res_default=res_default, res=res, res_universal=res_universal)


if __name__ == '__main__':
    app.run(debug=True)

