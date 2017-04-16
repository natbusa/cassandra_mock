# Cassandra_mock

Mocking the cassandra python API. Duh.  

Ok, but why?

1) Study project: A fun way to learn about cassandra, no/new SQL
2) Match cassandra data structures with native pythonic nested dicts and lists
3) Build a CQL AST in python and actually process CQL queries on the mock
4) Look ma, no cluster :)

I am sure that you can actually use cassandra and embedded it in memory in a python module, - and in facts I have constructed a few times embedded cassandra engines in my apps, and apis - but this time I wanted a really minimalistics and cassandra-less ... cassandra.

## How it works

Use the same api calls as in the cassandra python driver as shown here below:

connect to cassandra mock
```
cluster = Cluster([':memory:'])
session = cluster.connect("mybook")
```

### Create table 
```python
stmt = "
    CREATE TABLE posts (
        user_id text,
        month text,
        id text,
        title text,
        body text,
        PRIMARY KEY ( (user_id, month), id)
    );
")
session.execute(stmt)
```
### Insert 
```python
stmt = "insert into mybook.posts (user_id, month, id, title, body) 
                          values ('nat','june','1','first', 'it is me, mario');")
session.execute(stmt)

stmt = "insert into mybook.posts (user_id, month, id, title, body) 
                          values ('nat','june','2','one', 'hello');")
session.execute(stmt)

stmt = "insert into mybook.posts (user_id, month, id, title, body) 
                          values ('nat','july','3','more', 'yo dawg!');")
session.execute(stmt)
```

#### Query
```python
stmt = "use mybook;")
session.execute(stmt)

stmt = "select * from posts;")
session.execute(stmt)
#  {'title': 'first', 'body': 'it is me, mario', 'id': '1', 'month': 'june', 'user_id': 'nat'}
#  {'title': 'one', 'body': 'hello', 'id': '2', 'month': 'june', 'user_id': 'nat'}
#  {'title': 'more', 'body': 'yo dawg!', 'id': '3', 'month': 'july', 'user_id': 'nat'}


```
## Define an initial data load

Cassandra in memory uses just one python dictionary! :)   
Top keys of this dictionary are 'data' and 'index', obviously one for the data, the other for the indices.

```python
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


stmt = "select id, what from users;")
session.execute(stmt)
#  {'id': 'nat', 'what': 'invent'}
#  {'id': 'amy', 'what': 'runs'}
#  {'id': 'joe', 'what': 'sings'}

stmt = "select * from users where id='nat';")
session.execute(stmt)
#  {'id': 'nat', 'what': 'invent', 'when': 'ever'}
```

## Supported CQL statements

  - CREATE TABLE
  - SELECT
  - INSERT
  - UPDATE
  - DELETE
  - USE

## Educational
Feel free to use it to teach Python, Cassandra, CQL/SQL, AST parsing, testing driven design (TTD), Object Oriented programming, mocking etc. Great for students, coders, and sql enthusiasts.

## Experimental
This mock is experimental and not really meant to fit any purpose other than understading how cassandra data structures works and how they could be mocked using native python types. It's probably a great base for students who want to understand Data structures, python classes, AST parsing, and CQL/SQL syntax. Other than that feel free to do with it whatever you want.

## Cassandra Python API
This cassandra mocks reproduces _some_ of the features of the cassandra python API.
Please refer to Cassandra (http://cassandra.apache.org/) and the Python API https://datastax.github.io/python-driver/index.html

## This is a mock!
This is a mock! If you want design and testing cassandra-powered applications and go an actually download Cassandra. Please refer to http://cassandra.apache.org/ for the open source version and Datastax https://www.datastax.com/ for the enterprise edition.

## License
Copyright 2017 Natalino Busa, Apache Licence 2.0
