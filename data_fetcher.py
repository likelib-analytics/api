import pandas as pd
from datetime import datetime
from db import get_explorer_query, get_metric_query


def get_transactions(ch_client, **kwargs):
    data = ch_client.execute(get_explorer_query(
        'transactions', kwargs['limit'], kwargs['offset']))
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
    data = ch_client.execute(get_explorer_query(
        'blocks', kwargs['limit'], kwargs['offset']))
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
        from_timestamp, '%Y-%m-%dT%H:%M:%S')
    to_timestamp_dt = datetime.strptime(
        to_timestamp, '%Y-%m-%dT%H:%M:%S')

    # Prepare parameters for CH query
    table_name = f'{metric_name}_{interval_dict[interval]}_mv{mode_dict[mode]}'
    datetime_field_name = f'dt_{interval_dict[interval]}'
    timedelta = (to_timestamp_dt-from_timestamp_dt).total_seconds()
    interval = intervals_dict_seconds[interval]

    # Request and convert data
    data = ch_client.execute(get_metric_query(
        datetime_field_name, metric_name, table_name, timedelta, interval), {'from_timestamp': from_timestamp.replace('T', ' '),
                                                                             'to_timestamp': to_timestamp.replace('T', ' ')})
    df = pd.DataFrame(data, columns=['dt', 'value'])
    df['dt'] = df['dt'].dt.strftime('%Y/%d/%m %H:%M:%S')
    return df.to_json(orient='records')
