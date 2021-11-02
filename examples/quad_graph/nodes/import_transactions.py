from basis import parameter, input_from_table, input_from_stream, output_to_table, output_to_stream
import requests

# Inputs
# raw_table = input_from_table("node.table")
# raw_stream = input_from_stream("node.stream")

# Outputs
txn_table = output_to_table("self.raw_transactions")
txn_stream = output_to_stream("self.new_transactions")

# Parameters
url = parameter("base_url")
api_key = parameter("api_key")

# Code
@node
def transaction_importer():
    response = requests.get(url, auth=(api_key))
    txn_table.write(response)
    txn_stream.append(response)