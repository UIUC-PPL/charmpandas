import numpy as np
import networkx as nx
from networkx.drawing.nx_pydot import graphviz_layout
import matplotlib.pyplot as plt
from ctypes import c_long
from charmstencil.interface import to_bytes


OPCODES = {'noop': 0, 'create': 1, '+': 2, '-': 3, '*': 4, 'norm': 5,
           'getitem': 6, 'setitem': 7, 'exchange_ghosts': 8}


INV_OPCODES = {v: k for k, v in OPCODES.items()}


OPERAND_TYPES = {'field': 0, 'slice': 1, 'tuple': 2, 'int': 3, 'double': 4}


class CreateFieldNode(object):
    def __init__(self, name, shape, **kwargs):
        self.name = name
        self.shape = shape
        self.ghost_depth = kwargs.pop('ghost_depth', 1)
        self.identifier = to_bytes(OPCODES.get('create'), 'B') + \
            to_bytes(self.name, 'B') + to_bytes(self.ghost_depth, 'B')
        # FIXME get ghost info from kwargs

    def get_identifier(self):
        return to_bytes(False, '?') + self.identifier

    def fill_plot(self, G, node_map={}, next_id=0, parent=None):
        G.add_node(next_id)
        G.add_edge(parent, next_id)
        node_map[next_id] = 'CreateField: f' + str(self.name)
        return next_id + 1


class FieldOperationNode(object):
    def __init__(self, operation, operands, save=False):
        from charmstencil.stencil import Field
        self.opcode = OPCODES.get(operation)
        self.operands = operands
        self.save = save
        self.identifier = to_bytes(OPERAND_TYPES.get('field'), 'B')
        self.identifier += to_bytes(self.opcode, 'B')
        if self.opcode == 0:
            self.identifier += to_bytes(operands[0].name, 'B')
        elif self.opcode == 8:
            self.identifier += to_bytes(len(operands), 'B')
            for op in operands:
                self.identifier += to_bytes(op.name, 'B')
        else:
            #self.identifier += to_bytes(len(operands), 'B')
            for op in operands:
                if isinstance(op, Field):
                    #self.identifier += to_bytes(
                    #    OPERAND_TYPES.get('field'), 'B'
                    #)
                    self.identifier += op.graph.identifier
                elif isinstance(op, slice):
                    self.identifier += to_bytes(OPERAND_TYPES.get('slice'), 'B')
                    self.identifier += self._slice_to_bytes(op)
                elif isinstance(op, tuple):
                    if isinstance(op[0], slice):
                        self.identifier += to_bytes(
                            OPERAND_TYPES.get('slice'), 'B'
                        )
                    elif isinstance(op[0], int):
                        self.identifier += to_bytes(
                            OPERAND_TYPES.get('int'), 'B'
                        )
                    for k in op:
                        if isinstance(k, slice):
                            self.identifier += self._slice_to_bytes(k)
                        elif isinstance(k, int):
                            self.identifier += to_bytes(k, 'i')
                elif isinstance(op, int):
                    self.identifier += to_bytes(
                        OPERAND_TYPES.get('int'), 'B'
                    )
                    self.identifier += to_bytes(0, 'B')
                    self.identifier += to_bytes(op, 'i')
                elif isinstance(op, float):
                    self.identifier += to_bytes(
                        OPERAND_TYPES.get('double'), 'B'
                    )
                    self.identifier += to_bytes(0, 'B')
                    self.identifier += to_bytes(op, 'd')
                else:
                    raise ValueError('unrecognized operation')

    def _slice_to_bytes(self, key):
        start = 0 if key.start is None else key.start
        stop = 0 if key.stop is None else key.stop
        step = 1 if key.step is None else key.step
        buf = to_bytes(start, 'i')
        buf += to_bytes(stop, 'i')
        buf += to_bytes(step, 'i')
        return buf

    def fill_plot(self, G, node_map={}, next_id=0, parent=None):
        from charmstencil.stencil import Field
        if self.opcode == 0:
            node_map[next_id] = 'f' + str(self.operands[0].name)
            G.add_node(next_id)
            if parent is not None:
                G.add_edge(parent, next_id)
            return next_id + 1
        opnode = next_id
        G.add_node(next_id)
        if parent is not None:
            G.add_edge(parent, next_id)
        node_map[next_id] = INV_OPCODES.get(self.opcode, '?')
        next_id += 1
        for op in self.operands:
            # an operand can also be a double
            if isinstance(op, Field):
                next_id = op.graph.fill_plot(G, node_map, next_id, opnode)
            elif isinstance(op, float) or isinstance(op, int) or \
                isinstance(op, slice) or isinstance(op, tuple):
                G.add_node(next_id)
                G.add_edge(opnode, next_id)
                node_map[next_id] = str(op)
                next_id += 1
        return next_id

    def plot(self):
        G = nx.Graph()
        node_map = {}
        self.fill_plot(G, node_map=node_map)
        pos = graphviz_layout(G, prog='dot')
        nx.draw(G, pos, labels=node_map, node_size=600, font_size=10)
        plt.show()


class ComputeGraph(object):
    def __init__(self):
        self.graph = []

    def is_empty(self):
        return len(self.graph) == 0

    def insert(self, node):
        self.graph.append(node)

    def get_identifier(self):
        return to_bytes(True, '?') + b''.join([node.identifier for node in self.graph])

    def fill_plot(self, G, node_map={}, next_id=0, parent=None):
        for g in self.graph:
            next_id = g.fill_plot(G, node_map=node_map, next_id=next_id,
                                  parent=parent)
        return next_id

    def plot(self):
        G = nx.Graph()
        node_map = {}
        next_id = 0
        self.fill_plot(G, node_map=node_map, next_id=next_id)
        pos = graphviz_layout(G, prog='dot')
        nx.draw(G, pos, labels=node_map, node_size=600, font_size=10)
        plt.show()


class IterateGraph(ComputeGraph):
    def __init__(self):
        super().__init__()


class BoundaryGraph(ComputeGraph):
    def __init__(self):
        super().__init__()


class StencilGraph(object):
    def __init__(self, stencil, max_epochs):
        self.unique_graphs = []
        self.graphs = []
        self.iterate_identifier_map = {}
        self.boundary_identifier_map = {}
        self.max_epochs = max_epochs
        self.stencil = stencil
        self.next_graph = 0
        self.epoch = 0

    def _insert_graph(self, graph, id_map):
        identifier = graph.get_identifier()
        if identifier not in id_map:
            id_map[identifier] = len(self.unique_graphs)
            self.unique_graphs.append(graph)
        self.graphs.append(id_map[identifier])

    def _insert_node(self, node):
        self.graphs.append(len(self.unique_graphs))
        self.unique_graphs.append(node)

    def is_empty(self):
        return len(self.graphs) == 0

    def flush(self):
        self.graphs = []
        self.epoch += 1

    def insert(self, obj):
        if isinstance(obj, IterateGraph):
            self._insert_graph(obj, self.iterate_identifier_map)
        elif isinstance(obj, BoundaryGraph):
            self._insert_graph(obj, self.boundary_identifier_map)
        elif isinstance(obj, CreateFieldNode):
            self._insert_node(obj)
        else:
            raise ValueError("incorrect type of obj")
        if len(self.graphs) >= self.max_epochs:
            self.evaluate()

    def evaluate(self):
        self.stencil.evaluate()

    def plot(self):
        G = nx.Graph()
        node_map = {}
        next_id = 0
        for i, g in enumerate(self.unique_graphs):
            G.add_node(next_id)
            node_map[next_id] = 'g%i' % i
            next_id += 1
            next_id = g.fill_plot(G, node_map=node_map, next_id=next_id, parent=next_id - 1)
        pos = graphviz_layout(G, prog='dot')
        nx.draw(G, pos, labels=node_map, node_size=600, font_size=10)
        print('Epoch\tGraph')
        for i, n in enumerate(self.graphs):
            print('%i\t%s' % (i, 'g%i' % n if isinstance(n, int) else n))
        plt.show()

