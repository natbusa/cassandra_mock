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

print('----')
rows = session.execute("select * from relationships where from_type='ideas' and from_id='0' and to_type='projects';")
for row in rows: print(row)

print('----')
rows = session.execute("select * from relationships where from_type='ideas' and from_id='0' ;")
for row in rows: print(row)

print('----')
rows = session.execute("select * from relationships where from_type='ideas' and from_id='0' ;")
for row in rows: print(row)

print('----')
rows = session.execute("insert into autoscience.projects (id, name, pippo) values ('44', 1,'33');")

print('----')
rows = session.execute("select * from projects where id='44';")
for row in rows: print(row)

print('----')
rows = session.execute("select title from projects;")
for row in rows: print(row)

print('----')
session.execute("update autoscience.projects set name='baudo' where id='44';")
for row in rows: print(row)

print('----')
rows = session.execute("select name from projects;")
for row in rows: print(row)

