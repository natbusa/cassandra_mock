from cassandra_mock.cluster import Cluster

CDB_DATA = {
    'mybook': {
        'users': {
            'nat': {
                'what': 'invent',
                'when': 'ever',
            },
            'amy': {
                'what': 'runs',
                'when': 'in the mornings',
            },
            'joe': {
                'what': 'sings',
                'when': 'at night',
            }
        }
    }
}

CDB_INDEX = {
    'mybook': {
        'users': [['id']]
    }
}

CASSANDRA_DATA = {'data': CDB_DATA, 'index': CDB_INDEX}

# start the mock

cluster = Cluster([':memory:'], CASSANDRA_DATA)
session = cluster.connect()
session.set_keyspace("mybook")

# queries

try:
    a = session._query('autoscience', 'pippo', ['title', 'foo'], ['0'], [])
    print(a)
except:
    pass


stmts = [
    """
        CREATE TABLE posts (
            user_id text,
            month text,
            id text,
            title text,
            body text,
            PRIMARY KEY ( (user_id, month), id)
        );
    """,
    "insert into mybook.posts (user_id, month, id, title, body) values ('nat','june','1','first', 'it is me, mario');",
    "insert into mybook.posts (user_id, month, id, title, body) values ('nat','june','2','one', 'hello');",
    "insert into mybook.posts (user_id, month, id, title, body) values ('nat','july','3','more', 'yo dawg!');",
    "use mybook;",
    "select * from posts;",
    
    "select id, what from users;",
    "select * from users where id='nat';",

]

for i in stmts:
    print('\nstmt = "{}")'.format(i))
    print('session.execute(stmt)'.format(i))
    rows = session.execute(i)
    if rows:
        for row in rows:
            print('# ', row)
