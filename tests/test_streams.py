from __future__ import annotations

import pytest
from snapflow.core.data_block import DataBlockMetadata
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, Direction, FunctionLog
from snapflow.core.operators import filter, latest, operator
from snapflow.core.streams import (
    DataBlockStream,
    ManagedDataBlockStream,
    StreamBuilder,
    stream,
)
from tests.utils import (
    make_test_run_context,
    function_generic,
    function_t1_sink,
    function_t1_source,
    function_t1_to_t2,
)


class TestStreams:
    def setup(self):
        ctx = make_test_run_context()
        self.ctx = ctx
        self.env = ctx.env
        self.sess = self.env.md_api.begin()
        self.sess.__enter__()
        self.g = Graph(self.env)
        self.graph = self.g.get_metadata_obj()
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
        self.node_source = self.g.create_node(
            key="function_source", function=function_t1_source
        )
        self.node1 = self.g.create_node(
            key="function1", function=function_t1_sink, input="function_source"
        )
        self.node2 = self.g.create_node(
            key="function2", function=function_t1_to_t2, input="function_source"
        )
        self.node3 = self.g.create_node(
            key="function3", function=function_generic, input="function_source"
        )
        self.env.md_api.add(self.dr1t1)
        self.env.md_api.add(self.dr2t1)
        self.env.md_api.add(self.dr1t2)
        self.env.md_api.add(self.dr2t2)
        self.env.md_api.add(self.graph)

    def teardown(self):
        self.sess.__exit__(None, None, None)

    def test_stream_unprocessed_pristine(self):
        s = stream(nodes=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_query_result(self.env).scalar_one_or_none() is None

    def test_stream_unprocessed_eligible(self):
        dfl = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            function_key=self.node_source.function.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.env.md_api.add_all([dfl, drl])

        s = stream(nodes=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_query_result(self.env).scalar_one_or_none() == self.dr1t1

    def test_stream_unprocessed_ineligible_already_input(self):
        dfl = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            function_key=self.node_source.function.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        dfl2 = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node1.key,
            function_key=self.node1.function.key,
            runtime_url="test",
        )
        drl2 = DataBlockLog(
            function_log=dfl2, data_block=self.dr1t1, direction=Direction.INPUT,
        )
        self.env.md_api.add_all([dfl, drl, dfl2, drl2])

        s = stream(nodes=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_query_result(self.env).scalar_one_or_none() is None

    def test_stream_unprocessed_ineligible_already_output(self):
        """
        By default we don't input a block that has already been output by a DF, _even if that block was never input_,
        UNLESS input is a self reference (`this`). This is to prevent infinite loops.
        """
        dfl = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            function_key=self.node_source.function.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        dfl2 = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node1.key,
            function_key=self.node1.function.key,
            runtime_url="test",
        )
        drl2 = DataBlockLog(
            function_log=dfl2, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.env.md_api.add_all([dfl, drl, dfl2, drl2])

        s = stream(nodes=self.node_source)
        s1 = s.filter_unprocessed(self.node1)
        assert s1.get_query_result(self.env).scalar_one_or_none() is None

        # But ok with self reference
        s2 = s.filter_unprocessed(self.node1, allow_cycle=True)
        assert s2.get_query_result(self.env).scalar_one_or_none() == self.dr1t1

    def test_stream_unprocessed_eligible_schema(self):
        dfl = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            function_key=self.node_source.function.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.env.md_api.add_all([dfl, drl])

        s = stream(nodes=self.node_source, schema="TestSchema1")
        s = s.filter_unprocessed(self.node1)
        assert s.get_query_result(self.env).scalar_one_or_none() == self.dr1t1

        s = stream(nodes=self.node_source, schema="TestSchema2")
        s = s.filter_unprocessed(self.node1)
        assert s.get_query_result(self.env).scalar_one_or_none() is None

    def test_operators(self):
        dfl = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            function_key=self.node_source.function.key,
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

        sb = stream(nodes=self.node_source)
        expected_cnt = sb.get_count(self.env)
        assert expected_cnt == 2
        list(count(sb).as_managed_stream(self.ctx))
        assert self._cnt == expected_cnt

        # Test composed operators
        self._cnt = 0
        list(count(latest(sb)).as_managed_stream(self.ctx))
        assert self._cnt == 1

        # Test kwargs
        self._cnt = 0
        list(count(filter(sb, function=lambda db: False)).as_managed_stream(self.ctx))
        assert self._cnt == 0

    def test_managed_stream(self):
        dfl = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            function_key=self.node_source.function.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        dfl2 = FunctionLog(
            graph_id=self.graph.hash,
            node_key=self.node1.key,
            function_key=self.node1.function.key,
            runtime_url="test",
        )
        drl2 = DataBlockLog(
            function_log=dfl2, data_block=self.dr1t1, direction=Direction.INPUT,
        )
        self.env.md_api.add_all([dfl, drl, dfl2, drl2])

        s = stream(nodes=self.node_source)
        s = s.filter_unprocessed(self.node1)

        ctx = make_test_run_context()
        with ctx.env.md_api.begin():
            dbs = ManagedDataBlockStream(ctx, stream_builder=s)
            with pytest.raises(StopIteration):
                assert next(dbs) is None
