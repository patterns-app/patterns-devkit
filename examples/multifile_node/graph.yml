name: nested_nodes_example
default_storage: ${{ secrets.my_primary_storage }}
nodes:
  - node_from_template: basis_marketplace.salesforce.extract.extract_contacts
    node_parameters:
      api_key: ${{ secrets.salesforce_api_key }}
    input_connections:
      - from_table: mynode@output1
        connect_to_input: customers
    output_names:
      - output: ltv_table
        rename_to: myltv_table

  - subgraph: subgraph2
  - reference: someother_public_env/output1
