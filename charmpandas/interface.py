import struct
import pyarrow as pa
import subprocess
import threading
import time
from pyccs import Server


def to_bytes(value, dtype='I'):
    return struct.pack(dtype, value)

def string_bytes(value):
    assert(isinstance(value, str))
    cmd = to_bytes(len(value), 'i')
    cmd += to_bytes(value.encode('utf-8'), '%is' % len(value))
    return cmd

def pandas_from_bytes(bvalue):
    buffer = pa.py_buffer(bvalue)
    reader = pa.ipc.open_stream(buffer)
    return reader.read_all().to_pandas()


def from_bytes(bvalue, dtype='I'):
    if dtype == 'table':
        return pandas_from_bytes(bvalue)
    else:
        return struct.unpack(dtype, bvalue)[0]


class Handlers(object):
    connection_handler = b'connect'
    disconnection_handler = b'disconnect'
    sync_handler = b'sync'
    async_handler = b'async'
    async_group_handler = b'async_group'
    rescale_handler = b'rescale'


class Operations(object):
    read = 0
    fetch = 1
    set_column = 2
    groupby = 3
    join = 4
    print = 5
    concat = 6
    filter = 7
    rescale = 8


class GroupByOperations(object):
    sum = 0
    count = 1


def get_result_field(oper, field):
    if oper == GroupByOperations.sum:
        return "sum(%s)" % field
    elif oper == GroupByOperations.count:
        return "count(%s)" % field


groupby_operations_map = {'sum' : GroupByOperations.sum,
                          'count' : GroupByOperations.count}

# FIXME if this mapping is ever changed in the c++ API
# it will mess up the join types
class JoinType(object):
    left_semi = 0
    right_semi = 1
    left_anti = 2
    right_anti = 3
    inner = 4
    left_outer = 5
    right_outer = 6
    full_outer = 7


join_type_map = {'left_semi' : JoinType.left_semi,
                 'right_semi' : JoinType.right_semi,
                 'left_anti' : JoinType.left_anti,
                 'right_anti' : JoinType.right_anti,
                 'inner' : JoinType.inner,
                 'left_outer' : JoinType.left_outer,
                 'right_outer' : JoinType.right_outer,
                 'full_outer' : JoinType.full_outer}


def lookup_join_type(type_str):
    return join_type_map.get(type_str, -1)


def lookup_aggregation(agg_fn):
    return groupby_operations_map.get(agg_fn, -1)


class Interface(object):
    def __init__(self):
        pass

    def delete_stencil(self, name):
        raise NotImplementedError('delete_stencil called from base class')

    def evaluate_stencil(self, stencil):
        raise NotImplementedError('evaluate_stencil called from base class')

    def get(self, stencil_name, field_name):
        raise NotImplementedError('get called from base class')


class DummyInterface(Interface):
    def __init__(self):
        pass

    def read_parquet(self, table_name, file_path):
        pass

    def fetch_table(self, table_name):
        pass


class DebugInterface(Interface):
    def __init__(self):
        pass

    def read_parquet(self, table_name, file_path):
        pass

    def fetch_table(self, table_name):
        pass
    

class CCSInterface(Interface):
    def __init__(self, server_ip, server_port, odf=4, lb_period=5, activity_timeout=60):
        self.server = Server(server_ip, server_port)
        self.server.connect()
        self.epoch = -1
        self.group_epoch = 0
        self.activity_timeout = activity_timeout
        self.timer = None
        cmd = to_bytes(odf, 'i')
        cmd += to_bytes(lb_period, 'i')
        self.send_command(Handlers.connection_handler, cmd, reply_size=1)
        self.reset_timer()

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        cmd = to_bytes(True, 'B')
        self.send_command_async(Handlers.disconnection_handler, cmd)

    def get_header(self, epoch):
        return to_bytes(epoch, 'i')

    def read_parquet(self, table_name, file_path):
        self.activity_handler()
        cmd = self.get_header(self.epoch)
        
        gcmd = to_bytes(Operations.read, 'i')
        gcmd += to_bytes(table_name, 'i')
        gcmd += string_bytes(file_path)

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd

        self.send_command_async(Handlers.async_handler, cmd)

    def fetch_table(self, table_name):
        self.activity_handler()
        cmd = self.get_header(self.epoch)

        gcmd = to_bytes(Operations.fetch, 'i')
        gcmd += to_bytes(table_name, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd

        return self.send_command(Handlers.sync_handler, cmd, reply_type='table')

    def join_tables(self, t1, t2, res, k1_list, k2_list, type):
        self.activity_handler()
        cmd = self.get_header(self.group_epoch)

        gcmd = to_bytes(Operations.join, 'i')
        gcmd += to_bytes(t1, 'i')
        gcmd += to_bytes(t2, 'i')
        gcmd += to_bytes(res, 'i')

        gcmd += to_bytes(len(k1_list), 'i')
        for k1, k2 in zip(k1_list, k2_list):
            gcmd += string_bytes(k1)
            gcmd += string_bytes(k2)

        gcmd += to_bytes(type, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_group_handler, cmd)
        self.group_epoch += 1

    def set_column(self, table_name, field, rhs):
        self.activity_handler()
        cmd = self.get_header(self.epoch)

        gcmd = to_bytes(Operations.set_column, 'i')
        gcmd += to_bytes(table_name, 'i')
        gcmd += string_bytes(field)
        gcmd += rhs.graph.identifier

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_handler, cmd)

    def filter(self, table_name, rhs, result):
        self.activity_handler()
        cmd = self.get_header(self.epoch)

        gcmd = to_bytes(Operations.filter, 'i')
        gcmd += to_bytes(table_name, 'i')
        gcmd += to_bytes(result.name, 'i')
        gcmd += rhs.graph.identifier

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_handler, cmd)

    def groupby(self, table_name, keys, aggs, result_name):
        self.activity_handler()
        cmd = self.get_header(self.epoch)

        gcmd = to_bytes(Operations.groupby, 'i')
        gcmd += to_bytes(table_name, 'i')
        gcmd += to_bytes(result_name, 'i')

        opts_cmd = to_bytes(len(keys), 'i')
        
        for key in keys:
            opts_cmd += string_bytes(key)

        opts_cmd += to_bytes(len(aggs), 'i')
        for field, agg_type, result_field in aggs:
            opts_cmd += to_bytes(agg_type, 'i')
            opts_cmd += string_bytes(field)
            opts_cmd += string_bytes(result_field)

        gcmd += to_bytes(len(opts_cmd), 'i')
        gcmd += opts_cmd
        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_handler, cmd)

    def print_table(self, name):
        self.activity_handler()
        cmd = self.get_header(self.epoch)

        gcmd = to_bytes(Operations.print, 'i')
        gcmd += to_bytes(name, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_handler, cmd)

    def concat_tables(self, tables, res):
        self.activity_handler()
        cmd = self.get_header(self.epoch)

        gcmd = to_bytes(Operations.concat, 'i')
        gcmd += to_bytes(len(tables), 'i')

        for t in tables:
            gcmd += to_bytes(t.name, 'i')

        gcmd += to_bytes(res, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_handler, cmd)

    def rescale(self, new_procs):
        cmd = to_bytes(self.epoch, 'i')

        cmd += to_bytes(new_procs, 'i')
        gcmd = to_bytes(Operations.rescale, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd

        self.send_command_async(Handlers.rescale_handler, cmd, skip_timer=True)

    def send_command_raw(self, handler, msg, reply_size):
        self.epoch += 1
        self.server.send_request(handler, 0, msg)
        return self.server.receive_response(reply_size)
    
    def send_command_raw_var(self, handler, msg):
        self.epoch += 1
        self.server.send_request(handler, 0, msg)
        res = self.server.receive_response_msg()
        print(res)
        return res

    def send_command(self, handler, msg, reply_size=None, reply_type='B', skip_timer=False):
        if not skip_timer:
            self.reset_timer()
        if reply_size is None:
            return from_bytes(self.send_command_raw_var(handler, msg), reply_type)
        else:
            return from_bytes(self.send_command_raw(handler, msg, reply_size), reply_type)

    def send_command_async(self, handler, msg, skip_timer=False):
        if not skip_timer:
            self.reset_timer()
        self.epoch += 1
        self.server.send_request(handler, 0, msg)

    def inactivity_handler(self):
        pass

    def activity_handler(self):
        pass

    def get(self, stencil_name, field_name):
        pass

    def reset_timer(self):
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(self.activity_timeout, self.inactivity_handler)
        self.timer.start()


class LocalCluster(CCSInterface):
    def __init__(self, min_pes=1, max_pes=1, odf=4, activity_timeout=60):
        self.min_pes = min_pes
        self.max_pes = max_pes
        self.logfile = open("server.log", "w")
        self._write_nodelist(max_pes)
        self._run_server()
        super().__init__("127.0.0.1", 1234, odf=odf, activity_timeout=activity_timeout)

    def _write_nodelist(self, num_pes):
        nodestr = "host localhost\n" * num_pes
        with open("localnodelist", "w") as f:
            f.write(nodestr)

    def inactivity_handler(self):
        self.current_pes = self.min_pes
        if self.min_pes != self.max_pes:
            self.rescale(self.min_pes)

    def activity_handler(self):
        if self.current_pes == self.min_pes and self.min_pes != self.max_pes:
            self.rescale(self.max_pes)
            self.current_pes = self.max_pes

    def _run_server(self):
        self.process = subprocess.Popen(['/home/adityapb1546/charm/charmpandas/src/charmrun +p%i '
                                '/home/adityapb1546/charm/charmpandas/src/server.out +balancer MetisLB +LBDebug 3'
                                ' ++server ++server-port 1234 ++nodelist ./localnodelist' % self.max_pes], 
                                shell=True, text=True, stdout=self.logfile, stderr=subprocess.STDOUT)
        time.sleep(5)
        self.current_pes = self.max_pes
        