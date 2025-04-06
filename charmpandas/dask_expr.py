import dask.dataframe as dd
import charmpandas as pd
from dask.dataframe.dask_expr._expr import Expr
from dask.dataframe.dask_expr._groupby import GroupBy
from charmpandas.interface import LocalCluster, CCSInterface
from functools import lru_cache
import copy
cluster = LocalCluster(min_pes=4,max_pes=4, odf=4,activity_timeout=60)
pd.set_interface(cluster)
def execute_dask(dask_obj, depth=0): # for now replaces dask read parquet with charmpandas read_parquet
    # print(simplified_expr)
    if not (isinstance(dask_obj, Expr) or isinstance(dask_obj, GroupBy)):
        # print(f'Operation - {simplified_expr} not supported in charm pandas')
        return dask_obj
    else:
        if isinstance(dask_obj, GroupBy):
            pd_df = execute_dask(dask_obj.obj.expr)
            return pd_df.groupby(dask_obj.by)
        else:
            args = []
            if '_funcname' not in dir(dask_obj):
                # print(f'Operation - {simplified_expr} not supported')
                return dask_obj
            try:
                args = [execute_dask(o, depth+1) for o in dask_obj.operands]
            except Exception as e:
                print(f"Error in executing {dask_obj}: {e}")
            result = charm_mapper(dask_obj._funcname, args)
            # Clear the cache only at the top level (depth=0)
            if depth == 0:
                read_parquet.cache_clear()
            return result

@lru_cache(maxsize=None)
def read_parquet(path):
    return pd.read_parquet(path)

def charm_mapper(func_name, args):
    # Dataframe operations  
    if func_name == 'read_parquet':
        return read_parquet(args[0])
    elif func_name == 'projection':
        return args[0][args[1]]
    elif func_name == 'merge':
        return args[0].merge(args[1], how=args[2], left_on=args[3], right_on=args[4])
    elif func_name == 'groupby': # Difficult to integrate with other expr since groupby is not an expr
        return args[0].groupby(args[1])
    elif func_name == 'add':
        return args[0] + args[1]
    elif func_name == 'sub':
        return args[0] - args[1]
    elif func_name == 'mul':
        return args[0] * args[1]
    elif func_name == 'div':
        return args[0] / args[1]
    elif func_name == 'lt':
        return args[0] < args[1]
    elif func_name == 'le':
        return args[0] <= args[1]
    elif func_name == 'gt':
        return args[0] > args[1]
    elif func_name == 'ge':
        return args[0] >= args[1]
    elif func_name == 'eq':
        return args[0] == args[1]
    elif func_name == 'ne':
        return args[0] != args[1]
    elif func_name == 'count':
        return args[0].count()
    elif func_name == 'sum':
        return args[0].sum()
    elif func_name == 'assign':
        print(args)
        if len(args) == 2: # Assign a df
            args[0] = args[1]
            return args[0]
        else: # Assign a column
            args[0][args[1]] = args[2]
            return args[0]
    # Add assignment operations
    return None

# New function to handle groupby operations
