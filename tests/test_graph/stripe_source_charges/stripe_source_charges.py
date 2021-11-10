from basis_marketplace.stripe import import_stripe
from mynode import mynode

g = Graph()
n = g.new_node("node_reference")
n.set_parameter(api_key="..")
n2 = g.new_node(mynode)
n2.set_input(input1=n)


my_import_stripe = clone(import_stripe)
my_import_stripe.parameters.api_key = "xxxxxxx"
my_import_stripe.set_output(charges="stripe_charges")
