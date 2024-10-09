import struct
from pyccs import Server


def to_bytes(value, dtype='I'):
    return struct.pack(dtype, value)


def from_bytes(bvalue, dtype='I'):
    return struct.unpack(dtype, bvalue)[0]


class Handlers(object):
    connection_handler = b'connect'
    disconnection_handler = b'disconnect'
    read_handler = b'read'


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


class DebugInterface(Interface):
    def __init__(self):
        pass

    def read_parquet(self, table_name, file_path):
        pass


class CCSInterface(Interface):
    def __init__(self, server_ip, server_port, odf=4):
        self.server = Server(server_ip, server_port)
        self.server.connect()
        self.epoch = 0
        self.client_id = self.send_command(Handlers.connection_handler, to_bytes(odf, 'i'))

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        cmd = to_bytes(self.client_id, 'B')
        #self.send_command_async(Handlers.disconnection_handler, cmd)

    def read_parquet(self, table_name, file_path):
        cmd = to_bytes(self.client_id, 'B')
        cmd += to_bytes(table_name, 'i')
        cmd += to_bytes(len(file_path), 'i')
        cmd += to_bytes(file_path.encode('utf-8'), '%is' % len(file_path))
        self.send_command_async(Handlers.read_handler, cmd)

    def send_command_raw(self, handler, msg, reply_size):
        self.server.send_request(handler, 0, msg)
        return self.server.receive_response(reply_size)

    def send_command(self, handler, msg, reply_size=1, reply_type='B'):
        return from_bytes(self.send_command_raw(handler, msg, reply_size), reply_type)

    def send_command_async(self, handler, msg):
        self.server.send_request(handler, 0, msg)

    def get(self, stencil_name, field_name):
        pass

