import flask
from data_fetcher import get_transactions, get_blocks, get_metric, get_search
from db import ch_client

app = flask.Flask(__name__)
ch_client = ch_client()
app.config["DEBUG"] = True

# A route to return all of the available entries in our catalog.


@app.route('/api/v1/explorer/_transactions', methods=['GET'])
def api_transactions():
    return get_transactions(ch_client, limit=flask.request.args.get('limit'), offset=flask.request.args.get('offset'))


@app.route('/api/v1/explorer/_blocks', methods=['GET'])
def api_blocks():
    return get_blocks(ch_client, limit=flask.request.args.get('limit'), offset=flask.request.args.get('offset'))


@app.route('/api/v1/analytics/_metric', methods=['GET'])
def api_metric():
    return get_metric(ch_client,
                      from_timestamp=flask.request.args.get('from_timestamp'),
                      to_timestamp=flask.request.args.get('to_timestamp'),
                      interval=flask.request.args.get('interval'),
                      metric_name=flask.request.args.get('metric_name'),
                      mode=flask.request.args.get('mode'))

# Search 

@app.route('/api/v1/_search', methods=['GET'])
def api_search():
    return get_search(ch_client, search=flask.request.args.get('search'), search_type=flask.request.args.get('search_type'))

app.run(host='0.0.0.0', port=8080)
