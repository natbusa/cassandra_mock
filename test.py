"""File modified by Lucio Montero in 2022 to add the if __name__ == "__main__": to affect the tests
"execution, to make this file runnable easily in PyCharm."""
from cassandra_mock.cluster import Cluster

CDB_DATA = {
    'autoscience': {
        'ideas': {
            '0': {
                'title': 'amy',
                'desc': 'dances',
            },
            '1': {
                'title': 'bob',
                'desc': 'writes',
            },
            '2': {
                'title': 'sue',
                'desc': 'runs',
            },
            '3': {
                'title': 'joe',
                'desc': 'sings',
            },
            '4': {
                'title': 'Ada',
                'desc': 'thinks',
            }
        },
        'projects': {
            '0': {
                'title': 'car',
                'desc': 'drive',
            },
            '1': {
                'title': 'plane',
                'desc': 'fly',
            },
            '2': {}
        },
        'relationships': {
            'ideas': {
                '0': {
                    'ideas': {
                        '4': {},
                        '2': {}
                    },
                    'projects': {
                        '0': {'oh': 'my'},
                        '1': {}
                    }
                },
                '1': {
                    'projects': {
                        '0': {}
                    }
                },
                '2': {
                    'ideas': {
                        '3': {},
                        '0': {}
                    }
                },
                '3': {}
            },
            'projects': {
                '1': {
                    'ideas': {
                        '0': {},
                        '2': {},
                        '4': {}
                    }
                }
            }
        }
    }
}

CDB_INDEX = {
    'autoscience': {
        'projects': [['id']],
        'ideas': [['id']],
        'relationships': [['from_type', 'from_id'], 'to_type', 'to_id']
    }
}

CASSANDRA_DATA = {'data': CDB_DATA, 'index': CDB_INDEX}

# start the mock

cluster = Cluster([':memory:'], CASSANDRA_DATA)
session = cluster.connect()
session.set_keyspace("autoscience")

# queries

try:
    a = session._query('autoscience', 'pippo', ['title', 'foo'], ['0'], [])
    print(a)
except:
    pass

session.execute('use autoscience;')


stmts = [
    "select * from relationships where from_type='ideas' and from_id='0' and to_type='projects';",
    "select * from relationships where from_type='ideas' and from_id='0' ;",
    "insert into autoscience.projects (id, name, pippo) values ('44', 1,'33');",
    "select * from projects where id='44';",
    "select title from projects;",
    "update autoscience.projects set name='baudo' where id='44';",
    "select name from projects;",
    """
        CREATE TABLE sblocks (
            id uuid,
            block_id uuid,
            sub_block_id uuid,
            data blob,
            PRIMARY KEY ( (id, block_id), sub_block_id )
        );
    """,
    "insert into autoscience.sblocks (id, block_id, sub_block_id, data) values ('a','b','c','d');",
    "select * from sblocks;"
]

"""
----
{'from_type': 'ideas', 'from_id': '0', 'to_type': 'projects', 'oh': 'my', 'to_id': '0'}
{'from_type': 'ideas', 'from_id': '0', 'to_type': 'projects', 'to_id': '1'}
----
{'from_type': 'ideas', 'from_id': '0', 'to_id': '4', 'to_type': 'ideas'}
{'from_type': 'ideas', 'from_id': '0', 'to_id': '2', 'to_type': 'ideas'}
{'from_type': 'ideas', 'from_id': '0', 'oh': 'my', 'to_id': '0', 'to_type': 'projects'}
{'from_type': 'ideas', 'from_id': '0', 'to_id': '1', 'to_type': 'projects'}
----
----
{'id': '44', 'name': 1, 'pippo': '33'}
----
{'title': 'car'}
{'title': 'plane'}
{'title': None}
{'title': None}
----
----
{'name': None}
{'name': None}
{'name': None}
{'name': 'baudo'}
----
----
----
{'data': 'd', 'sub_block_id': 'c', 'block_id': 'b', 'id': 'a'}
"""
if __name__ == "__main__":
    for (i, stmt) in enumerate(stmts):
        print('----', i)
        rows = session.execute(stmt)
        if rows:
            for row in rows: print(row)
