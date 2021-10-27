from basis import Node, Table, Graph, Stream

from .othernode import OtherNode


class MyNode(Node):
    my_output_table = Table(schema="common.Transaction")

    def run(self, ctx):
        cust_table = ctx.get_table(OtherNode.customer_summary_table)
        ctx.append_to_table(self.my_output_table, my_record)







from marketplace import MyMarketpalceNode



class MyNode(MyMarketpalceNode):
    assigned_inputs = dict(customer_summary_table=my_customer_summary_table)


assign_inputs(MyMarketpalceNode, customer_summary_table=my_customer_summary_table)