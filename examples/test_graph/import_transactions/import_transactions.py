from basis import node, table

@node
def node_function_testpy(ctx):
    my_input_table = input_table('clean_transactions.step1', schema='common.Transaction')
    my_input_df = my_input_table.as_dataframe()
    my_output_table = output_table('node_function_testpy.stream')
    my_output_table.write(my_input_df)
  

@node
def node_function_testpy(ctx, inputs, outputs, parameters):
    my_input_df = inputs.my_input_table.as_dataframe() inputs.get("my_input_table").as_records()
    other_thing.write(my_input_df)

@node
def node_function_testpy(ctx):
    my_input_df = ctx.get_input_table("my_input_table").as_dataframe()


select
    *
from my_input_table
