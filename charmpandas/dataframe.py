import sys
import warnings
import numpy as np
from charmstencil.interface import DummyInterface
from charmstencil.ast import StencilGraph, IterateGraph, BoundaryGraph, \
    FieldOperationNode, CreateFieldNode


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


class DataFrame(object):
    def __init__(self, data):
        interface = get_interface()
        self.name = get_table_name()
        if isinstance(data, str):
            interface.read_parquet(self.name, data)

