from basis import Node, Table, Graph, Stream, SqlNode

from sqlnode import MySqlNode


class OtherNode(Node):
    inputs = [MySqlNode]
    # Or? inputs = [SqlNode("sqlnode.sql")]
    customer_summary_table = Table(schema="common.Transaction")

    def run(self, ctx):
        ...
