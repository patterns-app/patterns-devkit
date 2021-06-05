from __future__ import annotations

import pytest
from snapflow.core.persisted.data_block import DataBlockMetadata
from snapflow.core.declarative.base import load_yaml
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.operators import filter, latest, operator
from snapflow.core.persisted.state import DataBlockLog, DataFunctionLog, Direction
from snapflow.core.streams import (
    DataBlockStream,
    StreamBuilder,
    stream,
)
from tests.utils import (
    function_generic,
    function_t1_sink,
    function_t1_source,
    function_t1_to_t2,
    make_test_env,
    make_test_run_context,
)


class TestStreams:
    def setup(self):

        self.env = make_test_env()
        ctx = make_test_run_context(self.env)
        self.ctx = ctx
        self.sess = self.env.md_api.begin()
        self.sess.__enter__()
        # self.node_source = "function_source"
        # self.node1 = "function1"
        # self.node2 = "function2"
        # self.node3 = "function3"
        # gd = load_yaml(f"""
        # nodes:
        #   - key: {self.node_source}
        #     function: function_t1_source
        #   - key: {self.node1}
        #     function: function_t1_sink
        #     stdin: function_source
        #   - key: {self.node2}
        #     function: function_t1_to_t2
        #     stdin: function_source
        #   - key: {self.node3}
        #     function: function_t1_sink
        #     stdin: function_generic
        # """)
        # self.g = GraphCfg(**gd)
        self.dr1t1 = DataBlockMetadata(
            nominal_schema_key="_test.TestSchema1",
            realized_schema_key="_test.TestSchema1",
        )
        self.dr2t1 = DataBlockMetadata(
            nominal_schema_key="_test.TestSchema1",
            realized_schema_key="_test.TestSchema1",
        )
        self.dr1t2 = DataBlockMetadata(
            nominal_schema_key="_test.TestSchema2",
            realized_schema_key="_test.TestSchema2",
        )
        self.dr2t2 = DataBlockMetadata(
            nominal_schema_key="_test.TestSchema2",
            realized_schema_key="_test.TestSchema2",
        )
        self.node_source = GraphCfg(
            key="function_source", function="function_t1_source"
        )
        self.node1 = GraphCfg(
            key="function1", function="function_t1_sink", input="function_source"
        )
        self.node2 = GraphCfg(
            key="function2", function="function_t1_to_t2", input="function_source"
        )
        self.node3 = GraphCfg(
            key="function3", function="function_generic", input="function_source"
        )
        self.g = GraphCfg(nodes=[self.node_source, self.node1, self.node2, self.node3])
        self.env.md_api.add(self.dr1t1)
        self.env.md_api.add(self.dr2t1)
        self.env.md_api.add(self.dr1t2)
        self.env.md_api.add(self.dr2t2)
        # self.env.md_api.add(self.graph)

    def teardown(self):
        self.sess.__exit__(None, None, None)

    def test_stream_unprocessed_pristine(self):
        s = stream(nodes=[self.node_source.key])
        s = s.filter_unprocessed(self.node1.key)
        assert s.get_query_result(self.env).scalar_one_or_none() is None

    def test_stream_unprocessed_eligible(self):
        dfl = DataFunctionLog(
            node_key=self.node_source.key,
            function_key=self.node_source.function,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.env.md_api.add_all([dfl, drl])

        s = stream(nodes=[self.node_source.key])
        s = s.filter_unprocessed(self.node1.key)
        assert s.get_query_result(self.env).scalar_one_or_none() == self.dr1t1

    def test_stream_unprocessed_ineligible_already_input(self):
        dfl = DataFunctionLog(
            node_key=self.node_source.key,
            function_key=self.node_source.function,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        dfl2 = DataFunctionLog(
            node_key=self.node1.key,
            function_key=self.node1.function,
            runtime_url="test",
        )
        drl2 = DataBlockLog(
            function_log=dfl2, data_block=self.dr1t1, direction=Direction.INPUT,
        )
        self.env.md_api.add_all([dfl, drl, dfl2, drl2])

        s = stream(nodes=[self.node_source.key])
        s = s.filter_unprocessed(self.node1.key)
        assert s.get_query_result(self.env).scalar_one_or_none() is None

    # Hmmm when does a DF output the same datablock? I totally possible. I don't really get the scenario here? Maybe
    # made sense in an old paradigm (where upstream could be more general, not strictly just a node)
    # def test_stream_unprocessed_ineligible_already_output(self):
    #     """
    #     By default we don't input a block that has already been output by a DF, _even if that block was never input_,
    #     UNLESS input is a self reference (`this`). This is to prevent infinite loops.
    #     """
    #     dfl = DataFunctionLog(
    #         node_key=self.node_source.key,
    #         function_key=self.node_source.function,
    #         runtime_url="test",
    #     )
    #     drl = DataBlockLog(
    #         function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
    #     )
    #     dfl2 = DataFunctionLog(
    #         node_key=self.node1.key,
    #         function_key=self.node1.function,
    #         runtime_url="test",
    #     )
    #     drl2 = DataBlockLog(
    #         function_log=dfl2, data_block=self.dr1t1, direction=Direction.OUTPUT,
    #     )
    #     self.env.md_api.add_all([dfl, drl, dfl2, drl2])

    #     s = stream(nodes=[self.node_source.key])
    #     s1 = s.filter_unprocessed(self.node1.key)
    #     assert s1.get_query_result(self.env).scalar_one_or_none() is None

    #     # But ok with self reference
    #     s2 = s.filter_unprocessed(self.node1.key)
    #     assert s2.get_query_result(self.env).scalar_one_or_none() == self.dr1t1

    def test_stream_unprocessed_eligible_schema(self):
        dfl = DataFunctionLog(
            node_key=self.node_source.key,
            function_key=self.node_source.function,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.env.md_api.add_all([dfl, drl])

        s = stream(nodes=[self.node_source.key], schema="TestSchema1")
        s = s.filter_unprocessed(self.node1.key)
        assert s.get_query_result(self.env).scalar_one_or_none() == self.dr1t1

        s = stream(nodes=[self.node_source.key], schema="TestSchema2")
        s = s.filter_unprocessed(self.node1.key)
        assert s.get_query_result(self.env).scalar_one_or_none() is None

    def test_operators(self):
        dfl = DataFunctionLog(
            node_key=self.node_source.key,
            function_key=self.node_source.function,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        drl2 = DataBlockLog(
            function_log=dfl, data_block=self.dr2t1, direction=Direction.OUTPUT,
        )
        self.env.md_api.add_all([dfl, drl, drl2])

        self._cnt = 0

        @operator
        def count(stream: DataBlockStream) -> DataBlockStream:
            for db in stream:
                self._cnt += 1
                yield db

        sb = stream(nodes=[self.node_source.key])
        expected_cnt = sb.get_count(self.env)
        assert expected_cnt == 2
        list(count(sb).as_blocks(self.env))
        assert self._cnt == expected_cnt

        # Test composed operators
        self._cnt = 0
        list(count(latest(sb)).as_blocks(self.env))
        assert self._cnt == 1

        # Test kwargs
        self._cnt = 0
        list(count(filter(sb, function=lambda db: False)).as_blocks(self.env))
        assert self._cnt == 0

    # def test_managed_stream(self):
    #     dfl = DataFunctionLog(
    #         node_key=self.node_source.key,
    #         function_key=self.node_source.function,
    #         runtime_url="test",
    #     )
    #     drl = DataBlockLog(
    #         function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
    #     )
    #     dfl2 = DataFunctionLog(
    #         node_key=self.node1.key,
    #         function_key=self.node1.function,
    #         runtime_url="test",
    #     )
    #     drl2 = DataBlockLog(
    #         function_log=dfl2, data_block=self.dr1t1, direction=Direction.INPUT,
    #     )
    #     self.env.md_api.add_all([dfl, drl, dfl2, drl2])

    #     s = stream(nodes=[self.node_source.key])
    #     s = s.filter_unprocessed(self.node1.key)
    #     s.as_blocks()

    #     env = make_test_env()
    #     ctx = make_test_run_context(env)
    #     with env.md_api.begin():
    #         dbs = ManagedDataBlockStream(env, ctx, stream_builder=s)
    #         with pytest.raises(StopIteration):
    #             assert next(dbs) is None
