class Tree(dict):
    """A tree implementation using python's autovivification feature."""
    
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value
    
    # cast a (nested) dict to a (nested) Tree class
    def __init__(self, data={}):
        for k, data in data.items():
            if isinstance(data, dict):
                self[k] = type(self)(data)
            else:
                self[k] = data

    def one(self):
        assert 0, "tone"
        assert len(self) == 1, self
        return next(iter(self.values()))

    def __getattr__(self, key):
        return self[key]
    def __setattr__(self, key, value):
        self[key] = value
    def __delattr__(self, key):
        del self[key]

