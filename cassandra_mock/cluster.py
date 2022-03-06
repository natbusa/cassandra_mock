from .tree import Tree
from .parser import simpleSQL
import re
from unittest.mock import MagicMock
from cassandra.cluster import SimpleStatement, PreparedStatement
import cassandra.cluster, cassandra.query

# some lists and dicts logistics

def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def add_index(k, v, l):
    """
    takes a key, value and a list(dicts),
    returns a list of dicts with the extra key value added to the inner dicts
    """
    out = []
    for i in l:
        i[k] = v
        out.append(i)
    return out


def flat_one(d, kn):
    """ d must be a dict(k,list(dict)), returns a list(dict)"""
    out = []
    for k, v in d.items():
        for i in add_index(kn, k, v):
            out.append(i)
    return out


def flat(d, kk, level):
    """ d must be a dict, kk is a list of key names , level is the amount of dict levels to flatten """
    if level == 0:
        out = [d]
    else:
        lc = [flat(v, kk[1:], level - 1) for v in d.values()]
        dlc = dict(zip(d.keys(), lc))
        out = flat_one(dlc, kk[0])
    return out


class Session:
    DEFAULTS = dict()
    DEFAULTS['QUERY_LIMIT'] = 1000
    shutdown = MagicMock()
    prepare = cassandra.cluster.Session.prepare
    _default_timeout = 10
    _row_factory = staticmethod(cassandra.cluster.named_tuple_factory)
    @property
    def row_factory(self):
        return self._row_factory
    @property
    def default_timeout(self):
        return self._default_timeout

    def __init__(self, data, use_keyspace=None):
        self.use_keyspace = use_keyspace
        self.db = data['data']
        self.index = data['index']
        self.encoder = cassandra.cluster.Encoder()

    def set_keyspace(self, use_keyspace):
        self.use_keyspace = use_keyspace
    
    def _check_keyspace_table(self, keyspace, table=None):
        
        if not self.db.get(keyspace):
            raise KeyError(f"Keyspace {keyspace} not in self.db.")
        
        if table and (self.db[keyspace].get(table) is None):
            raise KeyError(f"Table {table} not in self.db[{keyspace}].")
    
    def _query(self, keyspace, table, sel=[], where_pkeys=[], where_ckeys=[], limit=DEFAULTS['QUERY_LIMIT']):
        
        # if no keyspace given use the default
        keyspace = keyspace if keyspace else self.use_keyspace
        
        # check keyspace, table
        self._check_keyspace_table(keyspace, table)
        
        d = self.db[keyspace][table]
        pkeys_keys = list(self.index[keyspace][table][0])
        ckeys_keys = list(self.index[keyspace][table][1:]) if len(self.index[keyspace][table]) > 1 else []
        
        if where_pkeys:
            # primary keys MUST be present or extract all table
            if len(pkeys_keys) != len(where_pkeys):
                raise KeyError("At least one primary key is not present")
            
            where_pkeys_dict = dict(zip(pkeys_keys, where_pkeys))
            
            # clustering keys MAY be present
            clevels_all = len(ckeys_keys)
            clevels_query = len(where_ckeys)
            clevels_left = clevels_all - clevels_query
            if clevels_left < 0:
                raise KeyError("clevels_left < 0")
            
            cl_idx = ckeys_keys[-clevels_left:] if clevels_left else []
            where_ckeys_dict = dict(zip(ckeys_keys[0:clevels_query], where_ckeys))
            
            for key in where_pkeys + where_ckeys:
                if d.get(key):
                    d = d[key]
                else:
                    d = None
                    break
            
            # flatten the results as a list of dicts for the
            # remaining clustering key clevels_all cassandra stores clustering trees
            # as a single physical data structure, but display them as multiple rows
            d = flat(d, cl_idx, clevels_left) if d is not None else []
            
            # add remaining keys (if any output)
            d = [merge_dicts(where_pkeys_dict, where_ckeys_dict, v) for v in d]
        
        else:
            keys = pkeys_keys + ckeys_keys
            d = flat(d, keys, len(keys))
        
        # apply sel, conforming to cassandra if not in the struct return None
        if sel:
            d = [dict((k, v[k]) if k in v else (k, None) for k in sel) for v in d]
        
        # apply limit
        d = d[0:limit]
        
        return d
    
    def _insert(self, keyspace, table, update_dict={}, where_pkeys=[], where_ckeys=[], limit=1000):
        
        # if no keyspace given use the default
        keyspace = keyspace if keyspace else self.use_keyspace
        
        # check keyspace, table
        self._check_keyspace_table(keyspace, table)
        
        d = self.db[keyspace][table]
        pkeys_keys = self.index[keyspace][table][0]
        ckeys_keys = self.index[keyspace][table][1:] if len(self.index[keyspace][table]) > 1 else []
        
        # both partition and clustering keys must be available
        if len(pkeys_keys) != len(where_pkeys) and len(ckeys_keys) != len(where_ckeys):
            raise
        
        # update the record
        for i in where_pkeys + where_ckeys:
            d = d[i]  # create and dive

        # update/create the record
        for k, v in update_dict.items():
            d[k] = v
    
    def execute(self, s, parameters=None, **kwargs):
        
        def cast_value(s):
            if re.search("^'.*'$", s):
                return s[1:-1]
            elif re.search('^[-+]?[0-9]+$', s):
                return int(s)
            else:
                return float(s)
        
        if isinstance(s, SimpleStatement):
            s = s.query_string
            if parameters:
                s = cassandra.query.bind_params(s, params, self.encoder)
        elif isinstance(s, PreparedStatement):
            assert 0, s
        p = simpleSQL.parseString(s)
        
        if p[0] == 'use':
            self.set_keyspace(p[1])
            return
        
        if p[0] == 'insert':
            b = p['table']
            (keyspace, table) = (b[0], b[2]) if len(b) > 1 else (self.use_keyspace, b[0])
            
            # check keyspace, table
            self._check_keyspace_table(keyspace, table)
            
            col_names = p['columns']['list']
            col_values = [cast_value(i) for i in p['values']['list']]
            cols_kv = dict(zip(col_names, col_values))
            
            pkeys_keys = list(self.index[keyspace][table][0])
            ckeys_keys = list(self.index[keyspace][table][1:]) if len(self.index[keyspace][table]) > 1 else []
            
            try:
                where_pkeys = [cols_kv[k] for k in pkeys_keys]
                where_ckeys = [cols_kv[k] for k in ckeys_keys]
            except:
                # missing primary key
                raise
            
            # remove keys from items to store
            for k in pkeys_keys + ckeys_keys:
                del cols_kv[k]
            
            return self._insert(keyspace, table, cols_kv, where_pkeys, where_ckeys)
        
        if p[0] == 'update':
            b = p['table']
            (keyspace, table) = (b[0], b[2]) if len(b) > 1 else (self.use_keyspace, b[0])
            
            # check keyspace, table
            self._check_keyspace_table(keyspace, table)
            
            cols_kv = {}
            b = p.get('set')
            if b:
                for i in range(len(b)):
                    if (i % 2):
                        continue
                    cols_kv[b[i][0]] = cast_value(b[i][2])
            
            where_pkeys = []
            where_ckeys = []
            b = p.get('where')
            if b:
                pkeys_keys = list(self.index[keyspace][table][0])
                ckeys_keys = list(self.index[keyspace][table][1:]) \
                    if len(self.index[keyspace][table]) > 1 else []
                
                where_kv = dict()
                for i in range(len(b)):
                    if not (i % 2):
                        continue
                    where_kv[b[i][0]] = cast_value(b[i][2])
                
                where_pkeys = [where_kv[k] for k in pkeys_keys if where_kv.get(k) is not None]
                where_ckeys = [where_kv[k] for k in ckeys_keys if where_kv.get(k) is not None]
            
            return self._insert(keyspace, table, cols_kv, where_pkeys, where_ckeys)
        
        if p[0] == 'select':
            b = p['table']
            (keyspace, table) = (b[0], b[2]) if len(b) > 1 else (self.use_keyspace, b[0])
            
            # check keyspace, table
            self._check_keyspace_table(keyspace, table)
            
            cols_sel = p.get('columns')
            cols_sel = [] if '*' in cols_sel else cols_sel
            
            where_pkeys = []
            where_ckeys = []
            b = p.get('where')
            if b:
                pkeys_keys = list(self.index[keyspace][table][0])
                ckeys_keys = list(self.index[keyspace][table][1:]) \
                    if len(self.index[keyspace][table]) > 1 else []
                
                where_kv = dict()
                for i in range(len(b)):
                    if not (i % 2):
                        continue
                    where_kv[b[i][0]] = cast_value(b[i][2])
                
                where_pkeys = [where_kv[k] for k in pkeys_keys if where_kv.get(k) is not None]
                where_ckeys = [where_kv[k] for k in ckeys_keys if where_kv.get(k) is not None]
            
            limit = self.DEFAULTS['QUERY_LIMIT']
            b = p.get('limit')
            if b:
                limit = b[1]
            
            return self._query(keyspace, table, cols_sel, where_pkeys, where_ckeys, limit)
        
        if p[0] == 'create' and p[1] == 'table':
            b = p['table']
            (keyspace, table) = (b[0], b[2]) if len(b) > 1 else (self.use_keyspace, b[0])
            
            # check keyspace
            self._check_keyspace_table(keyspace)
            
            primary_key = p['columns_def']['columns']['column']['primary_key'][2]
            if len(primary_key) > 1:
                self.index[keyspace][table] = [list(primary_key[0])] + list(primary_key[2])
            else:
                keys = list(primary_key[0])
                self.index[keyspace][table] = [keys[0]] + keys[0][1:]
            
            #  create an empty tree in db
            self.db[keyspace][table] = Tree()


class Cluster:
    _default_load_balancing_policy = cassandra.cluster.Cluster._default_load_balancing_policy
    _load_balancing_policy = None
    _default_retry_policy = cassandra.cluster.RetryPolicy()
    @property
    def load_balancing_policy(self):
        return self._load_balancing_policy
    @property
    def default_retry_policy(self):
        """
        A default :class:`.policies.RetryPolicy` instance to use for all
        :class:`.Statement` objects which do not have a :attr:`~.Statement.retry_policy`
        explicitly set.
        """
        return self._default_retry_policy

    def __init__(self, seed, data, port=None, protocol_version=None):
        # must clearly state :memory: in the list of seed
        if ':memory:' not in seed:
            raise ValueError("One of the elements in 'seed' should be ':memory:'")
        
        self.data = Tree(data)
        self.session = None
        self.port = port
        self.protocol_version = protocol_version

        self.profile_manager = cassandra.cluster.ProfileManager()
        self.profile_manager.profiles[cassandra.cluster.EXEC_PROFILE_DEFAULT] = cassandra.cluster.ExecutionProfile(
            self.load_balancing_policy,
            self.default_retry_policy,
            request_timeout=Session._default_timeout,
            row_factory=Session._row_factory
        )

    def connect(self, use_keyspace=None):
        self.session = Session(self.data, use_keyspace)
        return self.session
