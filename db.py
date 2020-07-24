from clickhouse_driver import Client
from config import CLICKHOUSE_HOST


def ch_client():
    return Client(host=CLICKHOUSE_HOST)

def get_explorer_query(table_name, limit, offset):
    query = '''SELECT * FROM {}
                    ORDER BY dt DESC

                    '''.format(table_name)
    if limit != None:
        query = query + 'LIMIT {} '.format(limit)

    if offset != None:
        query = query + 'OFFSET {}'.format(offset)
    return query