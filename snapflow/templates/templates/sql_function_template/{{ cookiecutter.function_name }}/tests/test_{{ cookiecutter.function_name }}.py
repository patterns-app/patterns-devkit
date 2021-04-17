# One test case per file

# inputs = dict(
#     input=dict(
#         data="""
#             k1,k2,f1,f2,f3,f4
#             1,2,abc,1.1,1,2012-01-01
#             1,2,def,1.1,{"1":2},2012-01-02
#             1,3,abc,1.1,2,2012-01-01
#             1,4,,,"[1,2,3]",2012-01-01
#             2,2,1.0,2.1,"[1,2,3]",2012-01-01
#             """,
#         schema="core.CoreTestSchema",
#     )
# )

# outputs = dict(
#     default=dict(
#         data="""
#             k1,k2,f1,f2,f3,f4
#             1,2,def,1.1,{"1":2},2012-01-02
#             1,3,abc,1.1,2,2012-01-01
#             1,4,,,"[1,2,3]",2012-01-01
#             2,2,1.0,2.1,"[1,2,3]",2012-01-01
#         """,
#         schema="core.CoreTestSchema",
#     )
# )
