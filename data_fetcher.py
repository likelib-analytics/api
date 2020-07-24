import pandas as pd
from db import ch_client, get_explorer_query


def get_transactions(**kwargs):
    data = ch_client().execute(get_explorer_query(
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
    df['dt']=df['dt'].dt.strftime('%Y/%d/%m %H:%M:%S')
    return df.to_json(orient='records')


def get_blocks(**kwargs):
    data = ch_client().execute(get_explorer_query(
        'blocks', kwargs['limit'], kwargs['offset']))
    df = pd.DataFrame(data, columns=['coinbase',
                                     'depth',
                                     'nonce',
                                     'previous_block_hash',
                                     'dt'])
    df['nonce'] = df['nonce'].astype('int64')
    df['dt']=df['dt'].dt.strftime('%Y/%d/%m %H:%M:%S')
    return df.to_json(orient='records')
