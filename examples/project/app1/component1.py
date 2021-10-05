from basis import component, Table, Record, Parameter, Secret, Context


@component(
    inputs=[Table("txs", schema="Transaction")],
    outputs=[Table()],
)
def component1(ctx: Context):
    txs_table = ctx.get_table("txs")
    txs_df = txs_table.as_dataframe()
    customer_sales_df = txs_df.group_by("customer").sum("amount").reindex()
    ctx.emit_table(customer_sales_df)
