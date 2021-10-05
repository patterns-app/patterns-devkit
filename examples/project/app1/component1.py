from basis import component, Table, Stream, Context


def comp():
    ctx.append_records_to_table(txs)

@component(
    inputs=[Stream("source1_transactions"), Table("customer_summary")],
    outputs=[Table("customer_sales"), Stream("customer_sales_stream")],
    node_params=[]
)
def component1(ctx: Context):
    cust_table = ctx.get_table("customer_summary")
    cust_df = cust_table.as_dataframe()
    for record in ctx.get_records("source1_transactions"):
        # TODO:
        cust_ltv = cust_table.execute_sql(f"select ltv from customer_summary where customer_id = {record['customer_id']}")
        cust_ltv = cust_df.find(record["customer_id"])["ltv"]
        record["ltv"] = cust_ltv
        ctx.emit_record(record)
        ctx.save_progress("source1_transactions")
    # txs_df = txs_table.as_dataframe()
    # customer_sales_df = txs_df.group_by("customer").sum("amount").reindex()
    # ctx.write_table("customer_sales", customer_sales_df)


    # ctx.stream_records("customer_sales_stream", customer_sales_df)
    # ctx.append_to_table("customer_sales", single_record)

    # for record in ctx.get_records("txs"):
    #     new_record = do_something(record)
    #     ctx.emit_record(new_record)


@simple_streaming_component
def fn(source1_transactions, customer_summary):
    # customer_summary.as_dataframe()
    record["new_val"] = 1
    return record
