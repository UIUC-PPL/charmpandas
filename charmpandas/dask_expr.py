import dask.dataframe as dd
import charmpandas as pd

def execute_dask(simplified_expr): # for now replaces dask read parquet with charmpandas read_parquet
    print(simplified_expr)
    if 'dask.dataframe.dask_expr' in str(type(simplified_expr)):
        print(f'{simplified_expr._funcname}')
        print(f'Operands - {simplified_expr.operands})')
        if simplified_expr._funcname == 'read_parquet':
            print("Swapping dask read parquet with charmpandas")
            print(f"Reading - {simplified_expr.operands[0]}")
            simplified_expr = pd.read_parquet(execute_dask(simplified_expr.operands[0]))
        # Need to add similar kind of execs for all other functions
        else:
            for o in simplified_expr.operands:
                o = execute_dask(o)
    return simplified_expr