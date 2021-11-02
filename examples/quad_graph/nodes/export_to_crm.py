from basis import input_from_table, parameters, output_to_table
import requests

# Inputs
high_ltv_customers = input_from_table('subgraph3.step3')

#Outputs
notified_customers = output_to_table('self.notified_customers')

#Parameters
base_url = parameters('base_url')
api_key = parameeters('api_key')

def export_high_ltv_customers_to_crm():
    for customer in high_ltv_customers:
        requests.post(baseurl, api_key, customer)
        notified_customers.write(customer)