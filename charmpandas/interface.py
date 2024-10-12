import struct
from pyccs import Server


def to_bytes(value, dtype='I'):
    return struct.pack(dtype, value)


def from_bytes(bvalue, dtype='I'):
    return struct.unpack(dtype, bvalue)[0]


class Handlers(object):
    connection_handler = b'connect'
    disconnection_handler = b'disconnect'
    sync_handler = b'sync'
    async_handler = b'async'


class Operations(object):
    read = 0
    fetch = 1
    add_column = 2
    groupby = 3
    join = 4
    print = 5
    concat = 6


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
    def __init__(self, server_ip, server_port, odf=4):
        self.server = Server(server_ip, server_port)
        self.server.connect()
        self.epoch = -1
        self.client_id = self.send_command(Handlers.connection_handler, to_bytes(odf, 'i'))

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        cmd = to_bytes(self.client_id, 'B')
        #self.send_command_async(Handlers.disconnection_handler, cmd)

    def get_header(self):
        cmd = to_bytes(self.client_id, 'B')
        cmd += to_bytes(self.epoch, 'i')
        return cmd

    def read_parquet(self, table_name, file_path):
        cmd = self.get_header()
        
        gcmd = to_bytes(Operations.read, 'i')
        gcmd += to_bytes(table_name, 'i')
        gcmd += to_bytes(len(file_path), 'i')
        gcmd += to_bytes(file_path.encode('utf-8'), '%is' % len(file_path))

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd

        self.send_command_async(Handlers.async_handler, cmd)

    def fetch_table(self, table_name):
        cmd = self.get_header()

        gcmd = to_bytes(Operations.fetch, 'i')
        gcmd += to_bytes(table_name, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd

        return self.send_command(Handlers.sync_handler, cmd, reply_type='i')

    def join_tables(self, t1, t2, res, k1, k2, type):
        cmd = self.get_header()

        gcmd = to_bytes(Operations.join, 'i')
        gcmd += to_bytes(t1, 'i')
        gcmd += to_bytes(t2, 'i')
        gcmd += to_bytes(res, 'i')

        gcmd += to_bytes(len(k1), 'i')
        gcmd += to_bytes(k1.encode('utf-8'), '%is' % len(k1))
        gcmd += to_bytes(len(k2), 'i')
        gcmd += to_bytes(k2.encode('utf-8'), '%is' % len(k2))

        gcmd += to_bytes(type, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_handler, cmd)

    def print_table(self, name):
        cmd = self.get_header()

        gcmd = to_bytes(Operations.print, 'i')
        gcmd += to_bytes(name, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_handler, cmd)

    def concat_tables(self, tables, res):
        cmd = self.get_header()

        gcmd = to_bytes(Operations.concat, 'i')
        gcmd += to_bytes(len(tables), 'i')

        for t in tables:
            gcmd += to_bytes(t.name, 'i')

        gcmd += to_bytes(res, 'i')

        cmd += to_bytes(len(gcmd), 'i')
        cmd += gcmd
        self.send_command_async(Handlers.async_handler, cmd)

    def send_command_raw(self, handler, msg, reply_size):
        self.epoch += 1
        self.server.send_request(handler, 0, msg)
        return self.server.receive_response(reply_size)
    
    def send_command_raw_var(self, handler, msg):
        self.epoch += 1
        self.server.send_request(handler, 0, msg)
        return self.server.receive_response_msg()

    def send_command(self, handler, msg, reply_size=1, reply_type='B'):
        if reply_size is None:
            return from_bytes(self.send_command_raw_var(handler, msg), reply_type)
        else:
            return from_bytes(self.send_command_raw(handler, msg, reply_size), reply_type)

    def send_command_async(self, handler, msg):
        self.epoch += 1
        self.server.send_request(handler, 0, msg)

    def get(self, stencil_name, field_name):
        pass

