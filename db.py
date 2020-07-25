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
        ORDER BY dt
        
        UNION ALL
        
        SELECT
            arrayJoin(
            arrayMap(x -> toDateTime(x), 
            timeSlots(toDateTime('{'2020-03-01 00:00:00'}'), toUInt32({timedelta}), toUInt32({interval})))) as dt, (0) as value) 
        GROUP BY dt
    '''
    return query
