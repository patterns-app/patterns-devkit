from patterns import OutputTable, InputTable, Parameter, OutputStream, InputStream


def test_methods():
    OutputTable(description="description", schema="Schema")
    InputTable(description="description", schema="Schema", required=False)
    OutputStream(description="description", schema="Schema")
    InputStream(description="description", schema="Schema", required=False)
    Parameter(type=str, description="description", default="param")
