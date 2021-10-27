from basis import node, Table, Stream, Context


# @node(
#     id=1,
#     inputs=[
#         Stream("source1_transactions", schema="common.Transaction"),
#         Table("customer_summary@output1", schema="stripe.Charge"),
#     ],
#     outputs=[
#         Table("customer_sales"),
#         Stream(
#             "customer_sales_stream",
#             schema={"field1": "Integer", "field2": "Text NotNull"},
#         ),
#     ],
#     parameters=[],
# )



from othernode.OtherNode import customer_summary_table


class MyNode(Node):
    inputs = [customer_summary_table]
    my_output_table = Table(schema="common.Transaction")

    def run(self, ctx):
        cust_table = ctx.get_table(customer_summary_table)
        ctx.append_to_table(self.my_output_table, my_record)



import MyNode, OtherNode

class MySubGraph(Graph):
    nodes = [MyNode, OtherNode]
    expose_inputs = [MyNode.myinput]
    expose_outputs = [OtherNode.myoutput]




from marketplace import MyMarketpalceNode



class MyNode(MyMarketpalceNode):
    assigned_inputs = dict(customer_summary_table=my_customer_summary_table)


assign_inputs(MyMarketpalceNode, customer_summary_table=my_customer_summary_table)