from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
import elasticsearch_operations



app = Flask(__name__)


@app.route('/')
def search():

    elasticsearch_operations.create_indices()
    return render_template('search.html')


@app.route('/book_results', methods=['POST', 'GET'])
def results():

    if request.method == 'POST':
        search_term = request.form["input"]
        res = elasticsearch_operations.search_in_elasticsearch(search_term)
        return render_template("result.html", search_term=search_term, res=res)





if __name__ == '__main__':
    app.run(debug=True)


