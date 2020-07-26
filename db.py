from clickhouse_driver import Client
from config import CLICKHOUSE_HOST


def ch_client():
    return Client(host=CLICKHOUSE_HOST)


def get_explorer_query(table_name, limit, offset):
    query = f'''
    SELECT * FROM {table_name} ORDER BY dt DESC
    '''
    if limit:
        query = query + f'LIMIT {limit} '

    if offset:
        query = query + f'OFFSET {offset}'
    return query


def get_metric_query(datetime_field_name, metric_name, table_name, timedelta, interval):
    query = f'''
    SELECT
        dt,
        sum(value)
    FROM
        (
        SELECT {datetime_field_name} as dt, {metric_name} as value
        FROM {table_name}
        PREWHERE
            dt >= toDateTime(%(from_timestamp)s, 'UTC') AND dt < toDateTime(%(to_timestamp)s, 'UTC')
        
        
        UNION ALL
        
        SELECT
            arrayJoin(
            arrayMap(x -> toDateTime(x), 
            timeSlots(toDateTime(%(from_timestamp)s, 'UTC'), toUInt32({timedelta}), toUInt32({interval})))) as dt, (0) as value) 
        GROUP BY dt
        ORDER BY dt
    '''
    return query


def get_address_search_query(search):
    query = f'''
    SELECT DISTINCT * FROM
    (SELECT  from
    FROM transactions
    WHERE from LIKE '%{search}%'

    UNION ALL
    
    SELECT to
    FROM transactions
    WHERE to LIKE '%{search}%')
    '''
    return query


def get_transaction_search_query(search):
    query = f'''
    SELECT DISTINCT transactionHash
    FROM transactions
    WHERE transactionHash LIKE '%{search}%'
    '''
    return query


def get_block_search_query(search):
    query = '''
    SELECT max(depth)
    FROM blocks
    '''
    return query


def get_address_search_detailed_query(search):
    query = f'''
    SELECT DISTINCT *
    FROM transactions
    WHERE from = '{search}' OR  to = '{search}' 
    ORDER BY depth DESC
    '''
    return query


def get_address_balance_query(search):
    query = f'''
    select runningAccumulate(balance)
    from balance_state_block_mv
    where address = '{search}'
    ORDER BY dt_block desc
    LIMIT 1'''
    return query


def get_transaction_search_detailed_query(search):
    query = f'''
    SELECT DISTINCT *
    FROM transactions
    WHERE transactionHash = '{search}'
    '''
    return query


def get_block_search_detailed_query(search):
    query = f'''
    SELECT DISTINCT *
    FROM blocks
    WHERE depth = '{search}'
    '''
    return query


def get_block_history_query(search):
    query = f'''
    SELECT from, to, amount, transactionHash, type, data, dt
    FROM transactions
    WHERE depth = '{search}'
    '''
    return query
