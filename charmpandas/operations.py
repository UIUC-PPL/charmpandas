from charmpandas.dataframe import get_interface, get_table_name, DataFrame

def read_parquet(file_path):
    return DataFrame(file_path)

def concat(objs):
    if (objs and len(objs) == 0) or objs is None:
        return
    interface = get_interface()
    result = DataFrame(None)
    interface.concat_tables(objs, result.name)
    return result