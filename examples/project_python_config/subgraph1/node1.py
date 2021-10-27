from basis import node, Table, Stream, Context


@node(
    inputs=[
        Stream("source1_transactions", schema="common.Transaction"),
        Table("customer_summary@output1", schema="stripe.Charge"),
    ],
    outputs=[
        Table("customer_sales"),
        Stream(
            "customer_sales_stream",
            schema={"field1": "Integer", "field2": "Text NotNull"},
        ),
    ],
    parameters=[],
)
def node1(ctx: Context):
    cust_table = ctx.get_table("customer_summary")
    cust_df = cust_table.as_dataframe()
    for record in ctx.get_records("source1_transactions"):
        # TODO:
        cust_ltv = cust_table.execute_sql(
            f"select ltv from customer_summary where customer_id = {record['customer_id']}"
        )
        cust_ltv = cust_df.find(record["customer_id"])["ltv"]
        record["ltv"] = cust_ltv
        ctx.emit_record(record)
        ctx.save_progress("source1_transactions")

