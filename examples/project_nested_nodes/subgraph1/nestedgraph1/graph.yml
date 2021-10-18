name: nestedgraph1
interface:
  parameters:
    limit: 100
  outputs:
    myoutput1: source1_table
    myotheroutput: stripe_transactions
    nested_stream: stripe_event_streams.refunds

graph:
  - node: basis_marketplace.stripe.stripe_event_streams
    node_params:
      resources:
        - transactions
        - refunds
    node_resources:
      storage: storage2
      cpu: 4
      memory: 16
    # Not doing these:
    # append_events_to_tables: true
    # insert_events_into_table: true
    # emit_event_stream: true
    default_output_configuration:
      storage: ${{ secrets.my_big_storage }}
    output_configurations:
      transactions:
        table: stripe_transactions # <-- this implies an aggregation step?
        stream: stripe_transactions_stream
        # storage not supported at this level

  # Alternative: aggregate is explicit node
  # - name: source1_table
  #   component: create_table_from_stream
  #   input: source1_transactions_stream
  #   # inputs:
  #   #   inputname: source1_transactions_stream
  #   # outputs:
  #   #   outputname:
  #   #     table: source1_table_alt
  - name: comp1
    node: component1.py
    node_inputs: source1_transactions
    # input: Stream(app1_source1__transactions)

    # input: app1.source1@transactions#table
    # input: source1
    # input: source1#table
    # input: source1@outputname
    # input: source1@outputname#table
    # input: app1.source1#table

  - name: comp2
    node: component2.py
    node_inputs: source1.refunds
    node_params:
      limit: ${{ parameters.limit }}
