# coding: utf-8


import json
from inspect import isfunction


class node:
    def __init__(self, name, tp, id=0):  # operator node struct
        self.name = name
        self.type = tp
        self.id = id


class Operator:
    # operator define
    data = []
    name = None

    def __init__(self, name=None):  # name is user function
        if name is None:
            raise Exception("operator name needed")

        self.name = name
        self.data = Data(name=name,  tp="operator")

    def __call__(self, data):
        if self.name is None:
            raise Exception("operator name needed")

        return self.data


class Data:
    data = []

    def __init__(self, tp="", **kwargs):
        if kwargs is None:
            raise Exception("input data is needed")

        if len(kwargs) == 0:
            raise Exception("params missing")

        dt = None
        if tp == "":  # 考虑多输入，则name可改为k-v的json字符串
            tp = "data"
            dt = node(name=kwargs, tp=tp)
        elif tp == "operator":
            if len(kwargs) != 1:
                raise Exception("one operator per time")

            for key in kwargs:
                dt = node(name=kwargs[key], tp=tp)
        self.data.append(dt)

    def to_workflow(self):  # 调用栈
        id = 1
        nodes = []
        edges = []
        src_node = 0
        dest_node = 0

        while len(self.data) > 0:
            d = self.data.pop()
            one_node = {
                'name': d.name,
                'type': d.type,
                'id': id,
            }
            nodes.append(one_node)

            if src_node == 0:
                src_node = id
            else:
                dest_node = id
                one_edge = {
                    "src_node": src_node,
                    "desc_node": dest_node,
                }
                edges.append(one_edge)
                src_node = dest_node

            id += 1

        ret = {
            "nodes": nodes,
            "edges": edges,
        }
        print(json.dumps(ret))


def A(data):
    # 用户定义的虚拟方法，要求：用户定义方法须返回Data结构体
    return Data(data=data)


def B(data):
    # 用户定义的虚拟方法，要求：用户定义方法须返回Data结构体
    return Data(data=data)


if __name__ == "__main__":
    func_a = Operator(name='A')
    func_b = Operator(name='B')
    data = Data(url='hdfs://abc.txt', query="a=1&b=2")
    data_a = func_a(data)
    data_b = func_b(data_a)
    data_b.to_workflow()

    # Operator(name='B')(Operator(name='A')(Data(url='hdfs://abc.txt'))).to_workflow()
