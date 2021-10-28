from basis import graph, Graph

import MyNode, OtherNode

class MyGraph(Graph):
    nodes = [MyNode, OtherNode]
    # expose_inputs = [MyNode.myinput]
    # expose_outputs = [OtherNode.myoutput]

