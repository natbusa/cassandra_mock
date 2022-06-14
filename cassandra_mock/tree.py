"""
Modified by Lucio Montero in 2022 to add new features and capabilities to the Tree objects, and to
make that Tree(obj) for Tree object returns obj instead of creating a new object based in the obj
keys and values.
"""

class Tree(dict):
    """A tree implementation using python's autovivification feature."""
    
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value
    
    # cast a (nested) dict to a (nested) Tree class
    def __init__(self, data=None):
        if data is None:
            data = {}
        for k, data in data.items():
            if isinstance(data, dict):
                self[k] = type(self)(data)
            else:
                self[k] = data

    def __new__(cls, data=None):
        if isinstance(data, Tree):
            return data
        else:
            return dict.__new__(Tree, data)

    def one(self):
        return self
        assert 0, ("tone", len(self), self, next(iter(self.values())))
        assert len(self) == 1, self
        return next(iter(self.values()))

    def __getattr__(self, key):
        return self[key]
    def __setattr__(self, key, value):
        self[key] = value
    def __delattr__(self, key):
        del self[key]

