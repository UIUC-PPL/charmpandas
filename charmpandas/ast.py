from charmpandas import dataframe
from charmpandas.interface import to_bytes, string_bytes


class OperandTypes(object):
    field = 0
    integer = 1
    double = 2


class ArrayOperations(object):
    noop = 0
    add = 1
    multiply = 2
    sub = 3


class FieldOperationNode(object):
    def __init__(self, operation, operands):
        self.opcode = operation
        self.operands = operands
        self.identifier = to_bytes(self.opcode, 'i')

        #self.identifier += to_bytes(len(operands), 'B')
        if self.opcode == ArrayOperations.noop:
            self.identifier += to_bytes(OperandTypes.field, 'i')
            self.identifier += to_bytes(operands[0].df.name, 'i')
            self.identifier += string_bytes(operands[0].field)
        else:
            for op in operands:
                if isinstance(op, dataframe.DataFrameField):
                    self.identifier += op.graph.identifier
                elif isinstance(op, int):
                    self.identifier += to_bytes(ArrayOperations.noop, 'i')
                    self.identifier += to_bytes(
                        OperandTypes.integer, 'i'
                    )
                    self.identifier += to_bytes(op, 'i')
                elif isinstance(op, float):
                    self.identifier += to_bytes(ArrayOperations.noop, 'i')
                    self.identifier += to_bytes(
                        OperandTypes.double, 'i'
                    )
                    self.identifier += to_bytes(op, 'd')
                else:
                    raise ValueError('unrecognized operation')
