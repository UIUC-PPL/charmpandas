import sys
import warnings
import numpy as np

from charmpandas.interface import lookup_join_type, lookup_aggregation, \
    get_result_field, GroupByOperations
from charmpandas.ast import FieldOperationNode, ArrayOperations

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


class DataFrameField(object):
    def __init__(self, df, field=None, graph=None):
        self.df = df
        self.field = field
        if graph is None:
            self.graph = FieldOperationNode(ArrayOperations.noop, [self])
        else:
            self.graph = graph

    def __add__(self, other):
        node = FieldOperationNode(ArrayOperations.add, [self, other])
        return DataFrameField(self.df, graph=node)

    def __sub__(self, other):
        node = FieldOperationNode(ArrayOperations.sub, [self, other])
        return DataFrameField(self.df, graph=node)

    def __mul__(self, other):
        node = FieldOperationNode(ArrayOperations.multiply, [self, other])
        return DataFrameField(self.df, graph=node)

    def __rmul__(self, other):
        return self * other

    def __div__(self, other):
        return self * (1 / other)


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
        
    def __getitem__(self, field):
        return DataFrameField(self, field=field)
    
    def __setitem__(self, field, data):
        interface = get_interface()
        interface.set_column(self.name, field, data)

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
            k1 = k2 = [on]
        elif isinstance(on, list):
            k1 = k2 = on
        else:
            raise ValueError("Join keys have to be a list")

        join_type = lookup_join_type(how)

        interface.join_tables(self.name, other.name, result.name,
                              k1, k2, join_type)
        return result

    def groupby(self, by):
        return DataFrameGroupBy(self, by)