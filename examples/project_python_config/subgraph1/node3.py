from basis import node, Table, Stream, Context, Port

node(
    name="node3",
    from_template="basis_bi.models.update_customer_ltv",
    inputs=[Table("my_customer_data") >> Port("customers")],
    outputs=[Port("ltv_table") >> Table("my_customer_data")],
)
