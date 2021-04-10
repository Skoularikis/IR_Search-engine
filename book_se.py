from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
import elasticsearch_operations

app = Flask(__name__)
es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])


@app.route('/')
def search():
    return render_template('search.html')


@app.route('/book_results', methods=['POST', 'GET'])
def results():
    if request.method == 'POST':
        search_term = request.form["input"]
        res = elasticsearch_operations.search_in_elasticsearch(search_term)
        return render_template("result.html", search_term=search_term, res=res)


if __name__ == '__main__':
    # create_model()
    if not elasticsearch_operations.check_if_index_exist("books"):
        elasticsearch_operations.upload_data_to_elastic()
    app.debug = True
    app.run()
    app.run(debug=True)
