import flask
from data_fetcher import get_transactions, get_blocks, get_metric, get_search, get_search_detailed, \
    get_address_balance, get_block_history
from db import ch_client

app = flask.Flask(__name__)
ch_client = ch_client()
app.config["DEBUG"] = False

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


@app.route('/api/v1/_search_detailed', methods=['GET'])
def api_search_detailed():
    return get_search_detailed(ch_client, search=flask.request.args.get('search'), search_type=flask.request.args.get('search_type'))


@app.route('/api/v1/_balance', methods=['GET'])
def api_address_balance():
    return get_address_balance(ch_client, address=flask.request.args.get('address'))


@app.route('/api/v1/_block_history', methods=['GET'])
def api_block_history():
    return get_block_history(ch_client, block=flask.request.args.get('block'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
