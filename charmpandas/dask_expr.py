import dask.dataframe as dd
import charmpandas as pd
from dask.dataframe.dask_expr._expr import Expr
from dask.dataframe.dask_expr._groupby import GroupBy
from charmpandas.interface import LocalCluster, CCSInterface
from functools import lru_cache
cluster = LocalCluster(min_pes=4,max_pes=4, odf=4,activity_timeout=60)
pd.set_interface(cluster)
def execute_dask(simplified_expr): # for now replaces dask read parquet with charmpandas read_parquet
    callables = ['read_parquet']
    print(simplified_expr)
    if not (isinstance(simplified_expr, Expr) or isinstance(simplified_expr, GroupBy)):
        print(f'Operation - {simplified_expr} not supported in charm pandas')
        return simplified_expr
    else:
        if isinstance(simplified_expr, GroupBy):
            return simplified_expr
            # return charm_mapper('groupby', [simplified_expr.by])
        # elif simplified_expr._funcname in callables:
        #     f = charm_mapper(simplified_expr._funcname)
        #     args = [execute_dask(o) for o in simplified_expr.operands]
        #     return f(args)
        else:
            args = []
            if '_funcname' not in dir(simplified_expr):
                print(f'Operation - {simplified_expr} not supported')
                return simplified_expr
            try:
                args = [execute_dask(o) for o in simplified_expr.operands]
            except:
                print("No operands found")
            return charm_mapper(simplified_expr._funcname, args)


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
    return None