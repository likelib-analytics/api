from clickhouse_driver import Client
from config import CLICKHOUSE_HOST


def ch_client():
    return Client(host=CLICKHOUSE_HOST)


def get_explorer_query(limit, offset):
    query = f'''
    SELECT * FROM %(table_name)s ORDER BY dt DESC
    '''
    if limit:
        query = query + f'LIMIT %(limit)s '

    if offset:
        query = query + f'OFFSET %(offset)s'
    return query


def get_metric_query():
    query = f'''
    SELECT
        dt,
        sum(value)
    FROM
        (
        SELECT %(datetime_field_name)s as dt, %(metric_name)s as value
        FROM %(table_name)s
        PREWHERE
            dt >= toDateTime(%(from_timestamp)s, 'UTC') AND dt < toDateTime(%(to_timestamp)s, 'UTC')
        
        UNION ALL
        
        SELECT
            arrayJoin(
            arrayMap(x -> toDateTime(x), 
            timeSlots(toDateTime(%(from_timestamp)s, 'UTC'), toUInt32(%(timedelta)s), toUInt32(%(interval)s)))) as dt, (0) as value) 
        GROUP BY dt
        ORDER BY dt
    '''
    return query


def get_address_search_query():
    query = f'''
    SELECT DISTINCT * FROM
    (SELECT  from
    FROM transactions
    WHERE from LIKE %(search)s

    UNION ALL
    
    SELECT to
    FROM transactions
    WHERE to LIKE %(search)s)
    '''
    return query


def get_transaction_search_query():
    query = f'''
    SELECT DISTINCT transactionHash
    FROM transactions
    WHERE transactionHash LIKE %(search)s
    '''
    return query


def get_block_search_query():
    query = '''
    SELECT max(depth)
    FROM blocks
    '''
    return query


def get_address_search_detailed_query():
    query = f'''
    SELECT DISTINCT *
    FROM transactions
    WHERE from = %(search)s OR  to = %(search)s
    ORDER BY depth DESC
    '''
    return query


def get_address_balance_query():
    query = f'''
    select runningAccumulate(balance)
    from balance_state_block_mv
    where address = %(address)s
    ORDER BY dt_block desc
    LIMIT 1'''
    return query


def get_transaction_search_detailed_query():
    query = f'''
    SELECT DISTINCT *
    FROM transactions
    WHERE transactionHash = %(search)s
    '''
    return query


def get_block_search_detailed_query():
    query = f'''
    SELECT DISTINCT *
    FROM blocks
    WHERE depth = %(search)s
    '''
    return query


def get_block_history_query():
    query = f'''
    SELECT from, to, amount, transactionHash, type, data, dt
    FROM transactions
    WHERE depth = %(block)s
    '''
    return query
