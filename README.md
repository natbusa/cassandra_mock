# Cassandra_mock

Mocking the cassandra python API. Duh.  

Ok, but why?
1) Match cassandra data structures with native pythonic nested dicts and lists
2) Build a CQL AST in python and actually process CQL queries on the mock
3) Look ma, no cluster :)

I am sure that you can actually use cassandra and embedded it in memory in a python module, - and in facts I have constructed a few times embedded cassandra engines in my apps, and apis - but this time I wanted a really minimalistics and cassandra-less ... cassandra.

## Experimental
This mock is experimental and not really meant to fit any purpose other than understading how cassandra data structures works and how they could be mocked using native python types. It's probably a great base for students who want to understand Data structures, python classes, AST parsing, and CQL/SQL syntax. Other than that feel free to do with it whatever you want.

## Cassandra Python API
This cassandra mocks reproduces _some_ of the features of the cassandra python API.
Please refer to Cassandra (http://cassandra.apache.org/) and the Python API https://datastax.github.io/python-driver/index.html

## This is a mock!
This is a mock! If you want proper testing and an actual cassandra database running, please refer to http://cassandra.apache.org/ for the open source version and Datastax https://www.datastax.com/ for the enterprise edition.

## License
Copyright 2017 Natalino Busa, Apache Licence 2.0
