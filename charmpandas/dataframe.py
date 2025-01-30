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

    def binary_op(self, other, op):
        node = FieldOperationNode(op, [self, other])
        return DataFrameField(self.df, graph=node)

    def __add__(self, other):
        return self.binary_op(other, ArrayOperations.add)

    def __sub__(self, other):
        return self.binary_op(other, ArrayOperations.sub)

    def __mul__(self, other):
        return self.binary_op(other, ArrayOperations.multiply)

    def __rmul__(self, other):
        return self * other

    def __div__(self, other):
        return self.binary_op(other, ArrayOperations.divide)
    
    def __lt__(self, other):
        return self.binary_op(other, ArrayOperations.less_than)

    def __le__(self, other):
        return self.binary_op(other, ArrayOperations.less_equal)
    
    def __gt__(self, other):
        return self.binary_op(other, ArrayOperations.greater_than)
    
    def __ge__(self, other):
        return self.binary_op(other, ArrayOperations.greater_equal)
    
    def __eq__(self, other):
        return self.binary_op(other, ArrayOperations.equal)
    
    def __ne__(self, other):
        return self.binary_op(other, ArrayOperations.not_equal)


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
        
    def __del__(self):
        # mark this df for deletion
        interface = get_interface()
        interface.mark_deletion(self.name)
        
    def __getitem__(self, rhs):
        if isinstance(rhs, str):
            return DataFrameField(self, field=rhs)
        elif isinstance(rhs, DataFrameField):
            interface = get_interface()
            result = DataFrame(None)
            interface.filter(self.name, rhs, result)
            return result
    
    def __setitem__(self, field, data):
        interface = get_interface()
        interface.set_column(self.name, field, data)

    def get(self):
        interface = get_interface()
        return interface.fetch_table(self.name)
    
    def print(self):
        interface = get_interface()
        interface.print_table(self.name)
    
    def merge(self, other, on=None, left_on=None, right_on=None, how='inner'):
        interface = get_interface()
        result = DataFrame(None)
        join_type = lookup_join_type(how)

        if left_on == None and right_on == None:
            if on == None:
                raise ValueError("Required argument 'on'")
            else:
                left_on = right_on = on

        if on == None:
            if left_on == None or right_on == None:
                raise ValueError("Both left_on and right_on required")

        interface.join_tables(self.name, other.name, result.name,
                              left_on, right_on, join_type)
        return result

    def groupby(self, by):
        return DataFrameGroupBy(self, by)