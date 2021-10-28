from basis import Node, Table, Graph, Stream, SqlNode

from .othernode import OtherNode


class MySqlNode(SqlNode):
    customer_summary_input = Input(customer_summary_table)
    # customer_summary_table_output = Table(schema="common.Transaction")

    def sql(self):
        return f"""select * from {self.customer_summary_input.as_sql_table()}"""
