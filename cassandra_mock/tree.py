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
