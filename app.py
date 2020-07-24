import flask
from data_fetcher import get_transactions, get_blocks

app = flask.Flask(__name__)
app.config["DEBUG"] = False

# A route to return all of the available entries in our catalog.
@app.route('/api/v1/explorer/_transactions', methods=['GET'])
def api_transactions():
    return get_transactions(limit=flask.request.args.get('limit'), offset=flask.request.args.get('offset'))

@app.route('/api/v1/explorer/_blocks', methods=['GET'])
def api_blocks():
    return get_blocks(limit=flask.request.args.get('limit'), offset=flask.request.args.get('offset'))

app.run(host='0.0.0.0', port=8080)