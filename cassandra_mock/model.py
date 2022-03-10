class ModelMock(dict):
    """A mock of the Cassandra documents.
    For now, it is a dictionary whose elements can also be accessed as atrributes of this object"""
# some lists and dicts logistics
    def __getattr__(self, key):
        return self[key]
    def __setattr__(self, key, value):
        self[key] = value
    def __delattr__(self, key):
        del self[key]

class ResultsList(list):
    def one(self):
        assert len(self) == 1, self
        return self[0]