from basis import *


@node
def passthrough_node(
    source_stream=InputStream(
        description="in desc",
        schema="TestSchema",
    ),
    optional_stream=InputStream(required=False),
    passthrough_stream=OutputStream(description="out desc", schema="TestSchema2"),
    explicit_param=Parameter(description="param desc", type="bool", default=False),
    plain_param=Parameter,
    state_param=State,
    conn_param=Connection(domain="example.com", description="conn desc"),
):
    pass
