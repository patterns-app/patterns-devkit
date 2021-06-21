inputs = dict(
    input=dict(
        data="""
            k1,FIELD,json,Split name,order
            1,abc,{"1":2},1,1
            2,def,{"1":2},{"1":2},2
            3,abc,{"1":2},2,3
            4,,,"[1,2,3]",4
            1,1.0,{"1":{"a":"b"}},"[1,2,3]",5
            """,
        schema="core.CoreTestNamesSchema",
    )
)

outputs = dict(
    stdout=dict(
        data="""
            k1,FIELD,json,Split name,order
            1,1.0,{"1":{"a":"b"}},"[1,2,3]",5
            2,def,{"1":2},{"1":2},2
            3,abc,{"1":2},2,3
            4,,,"[1,2,3]",4
        """,
        schema="core.CoreTestNamesSchema",
    )
)
