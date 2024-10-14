import sys
import warnings
import numpy as np

from charmpandas.interface import lookup_join_type, lookup_aggregation, \
    get_result_field, GroupByOperations


try:
    from typing import final
except ImportError:
    final = lambda f: f


next_table_name = 0
interface = None


def get_table_name():
    global next_table_name
    name = next_table_name
    next_table_name += 1
    return name


def set_interface(x):
    global interface
    if interface is not None:
        raise ValueError("Interface is already set")
    interface = x


def get_interface():
    global interface
    return interface


class Field(object):
    def __init__(self, fname, shape, stencil, **kwargs):
        self.name = fname
        self.shape = shape
        if isinstance(shape, int):
            self._key_type = int
            self._slice_type = slice
        else:
            self._key_type = tuple
            self._slice_type = tuple
        self.stencil = stencil
        self.slice_key = kwargs.pop('slice_key', None)
        self.graph = kwargs.pop('graph', FieldOperationNode('noop', [self]))
        # TODO get ghost data from kwargs

    def __getitem__(self, key):
        if isinstance(key, self._slice_type) or isinstance(key, self._key_type):
            node = FieldOperationNode('getitem', [self, key])
            return Field(self.name, self.shape, self.stencil, slice_key=key,
                         graph=node)

    def __setitem__(self, key, value):
        if isinstance(key, self._slice_type) or isinstance(key, self._key_type):
            node = FieldOperationNode('setitem', [self, key, value])
            self.stencil.active_graph.insert(node)

    def __add__(self, other):
        node = FieldOperationNode('+', [self, other])
        return Field(self.name, self.shape, self.stencil,
                     graph=node)

    def __sub__(self, other):
        node = FieldOperationNode('-', [self, other])
        return Field(self.name, self.shape, self.stencil,
                     graph=node)

    def __mul__(self, other):
        node = FieldOperationNode('*', [self, other])
        return Field(self.name, self.shape, self.stencil,
                     graph=node)

    def __rmul__(self, other):
        return self * other

    def __div__(self, other):
        return self * (1 / other)


class DataFrameGroupByField(object):
    def __init__(self, df_groupby, field):
        self.df_groupby = df_groupby
        self.field = field

    def sum(self):
        return self.df_groupby.execute([(self.field, GroupByOperations.sum, 
                                         get_result_field(GroupByOperations.sum, self.field))])
    
    def count(self):
        return self.df_groupby.execute([(self.field, GroupByOperations.count,
                                         get_result_field(GroupByOperations.count, self.field))])
    
    def aggregate(self, agg_fn):
        if not isinstance(agg_fn, str):
            raise ValueError("Invalid aggregation operation")
        return self.df_groupby.execute({self.field, lookup_aggregation(agg_fn)})


class DataFrameGroupBy(object):
    def __init__(self, df, by):
        self.df = df
        self.by = by

    def __getitem__(self, field):
        return DataFrameGroupByField(self, field)

    def execute(self, aggs):
        interface = get_interface()
        result = DataFrame(None)
        interface.groupby(self.df.name, self.by, aggs, result.name)
        return result
    
    def sum(self):
        raise NotImplementedError("Groupby requires specifying target columns"
                                  "for now")
        #return self.execute({GroupByOperations.sum})
    
    def count(self):
        raise NotImplementedError("Groupby requires specifying target columns"
                                  "for now")
        #return self.execute(GroupByOperations.count)
    
    def aggregate(self, aggs):
        agg_list = []
        if isinstance(aggs, str):
            raise NotImplementedError("Groupby requires specifying target columns"
                                    "for now")
        elif isinstance(aggs, dict):
            for field, opers in aggs:
                if isinstance(opers, list):
                    for oper in opers:
                        agg_type = lookup_aggregation(oper)
                        agg_list.append((field, agg_type, get_result_field(agg_type, field)))
                else:
                    agg_type = lookup_aggregation(opers)
                    agg_list.append((field, agg_type, get_result_field(agg_type, field)))
            return self.execute(aggs)


class DataFrame(object):
    def __init__(self, data):
        interface = get_interface()
        self.name = get_table_name()
        if isinstance(data, str):
            interface.read_parquet(self.name, data)
        elif data == None:
            # this is a result of some operation
            pass
        else:
            raise NotImplementedError("Only way to create dataframe right now"
                                      "is to read a parquet file")

    def get(self):
        interface = get_interface()
        return interface.fetch_table(self.name)
    
    def print(self):
        interface = get_interface()
        interface.print_table(self.name)
    
    def join(self, other, on, how='inner'):
        interface = get_interface()
        result = DataFrame(None)

        if isinstance(on, str):
            k1 = k2 = on
        elif isinstance(on, list) or isinstance(on, tuple):
            k1, k2 = on

        join_type = lookup_join_type(how)

        interface.join_tables(self.name, other.name, result.name,
                              k1, k2, join_type)
        return result

    def groupby(self, by):
        return DataFrameGroupBy(self, by)