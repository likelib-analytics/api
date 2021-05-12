import pandas as pd
from datetime import datetime
from db import get_explorer_query, get_metric_query, get_address_search_query, get_transaction_search_query, \
    get_block_search_query, get_address_search_detailed_query, get_transaction_search_detailed_query, \
    get_block_search_detailed_query, get_address_balance_query, get_block_history_query


def get_transactions(ch_client, **kwargs):
    data = ch_client.execute(
        get_explorer_query(kwargs['limit'], kwargs['offset']),
        {"table_name": 'transactions', "limit": kwargs['limit'], "offset": kwargs['offset']})
    if len(data) == 0:
        return '[]'
    df = pd.DataFrame(data, columns=['from',
                                     'to',
                                     'transactionHash',
                                     'type',
                                     'amount',
                                     'data',
                                     'fee',
                                     'dt',
                                     'depth'])
    df['dt'] = df['dt'].dt.strftime('%Y/%d/%m %H:%M:%S')
    return df.to_json(orient='records')


def get_blocks(ch_client, **kwargs):
    data = ch_client.execute(
        get_explorer_query(kwargs['limit'], kwargs['offset']),
        {"table_name": "blocks", "limit": kwargs['limit'], "offset": kwargs['offset']})
    if len(data) == 0:
        return '[]'
    df = pd.DataFrame(data, columns=['coinbase',
                                     'depth',
                                     'nonce',
                                     'previous_block_hash',
                                     'dt'])
    df['nonce'] = df['nonce'].astype('int64')
    df['dt'] = df['dt'].dt.strftime('%Y/%d/%m %H:%M:%S')
    return df.to_json(orient='records')


def get_metric(ch_client, from_timestamp, to_timestamp, interval, metric_name, mode):

    # Initialize necessary dicts
    interval_dict = {'1b': 'block',
                     '1m': 'minute',
                     '1h': 'hour',
                     '1d': 'day'}

    intervals_dict_seconds = {
        '1m': 60,
        '1h': 60*60,
        '1d': 60*60*24}
    mode_dict = {'live': '',
                 'demo': '_demo'}

    # Convert strings into datetime
    from_timestamp_dt = datetime.strptime(
        from_timestamp, '%Y-%m-%d %H:%M:%S')
    to_timestamp_dt = datetime.strptime(
        to_timestamp, '%Y-%m-%d %H:%M:%S')

    # Prepare parameters for CH query
    table_name = f'{metric_name}_{interval_dict[interval]}_mv{mode_dict[mode]}'
    datetime_field_name = f'dt_{interval_dict[interval]}'
    timedelta = (to_timestamp_dt-from_timestamp_dt).total_seconds()
    interval = intervals_dict_seconds[interval]
    # Request and convert data
    data = ch_client.execute(
        get_metric_query(),
        {'from_timestamp': from_timestamp, 'to_timestamp': to_timestamp, "datetime_field_name": datetime_field_name,
         "metric_name": metric_name, "table_name": table_name, "timedelta": timedelta, "interval": interval})
    if len(data) == 0:
        return '[]'
    df = pd.DataFrame(data, columns=['dt', 'value'])
    df['dt'] = df['dt'].dt.strftime('%Y/%d/%m %H:%M:%S')
    return df.to_json(orient='records')


def get_search(ch_client, search, search_type):
    search_types = {
        'address': get_address_search_query(),
        'transactions': get_transaction_search_query(),
        'blocks': get_block_search_query()
    }
    data = ch_client.execute(search_types[search_type], {"search": f"%{search.replace("%", "\%")}%"})
    if len(data) == 0:
        return {'type': search_type, 'data': []}
    if search_type == 'blocks':
        max_block = data[0][0]
        data = []
        for i in range(max_block + 1):
            if str(search) in str(i):
                data.append(i)
        return {'type': search_type, 'data': data}
    return {'type': search_type, 'data': [item[0] for item in data]}


def get_search_detailed(ch_client, search, search_type):
    search_types = {
        'address': get_address_search_detailed_query(),
        'transactions': get_transaction_search_detailed_query(),
        'blocks': get_block_search_detailed_query()
    }
    data = ch_client.execute(search_types[search_type], {"search": search})
    if len(data) == 0:
        return '[]'
    if search_type == 'blocks':
        df = pd.DataFrame(data, columns=['coinbase',
                                         'depth',
                                         'nonce',
                                         'previous_block_hash',
                                         'dt'])
        df['nonce'] = df['nonce'].astype('int64')

    elif search_type == 'transactions' or search_type == 'address':
        df = pd.DataFrame(data, columns=['from',
                                         'to',
                                         'transactionHash',
                                         'type',
                                         'amount',
                                         'data',
                                         'fee',
                                         'dt',
                                         'depth'])
    df['dt'] = df['dt'].dt.strftime('%Y/%d/%m %H:%M:%S')
    return df.to_json(orient='records')


def get_address_balance(ch_client, address):
    query = get_address_balance_query(address)
    data = ch_client.execute(query, {"address": address})
    if not data:
        return {'address': address, 'balance': 0}
    return {'address': address, 'balance': max(data[0][0], 0)}

def get_block_history(ch_client, block):
    query = get_block_history_query(block)
    data = ch_client.execute(query, {"block": block})
    if not data:
        return {'block': block, 'transactions': []}
    df = pd.DataFrame(data, columns=['from',
                                     'to',
                                     'amount',
                                     'transactionHash',
                                     'type',
                                     'data',
                                     'dt'])
    df['dt'] = df['dt'].dt.strftime('%Y/%d/%m %H:%M:%S')
    return {'block': block, 'transactions': df.to_dict('records')}
