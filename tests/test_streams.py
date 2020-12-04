from __future__ import annotations

from snapflow.core.data_block import DataBlockMetadata
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, Direction, PipeLog
from snapflow.core.operators import filter, latest, operator
from snapflow.core.streams import DataBlockStream, StreamBuilder
from tests.utils import (
    make_test_execution_context,
    pipe_generic,
    pipe_t1_sink,
    pipe_t1_source,
    pipe_t1_to_t2,
)


class TestStreams:
    def setup(self):
        ctx = make_test_execution_context()
        self.ctx = ctx
        self.env = ctx.env
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
        # self.ds1db1 = DataSetMetadata(
        #     data_block=self.dr1t1,
        #     name="dataset1",
        #     expected_schema_key="_test.TestSchema1",
        #     realized_schema_key="_test.TestSchema1",
        # )
        self.node_source = self.g.create_node("pipe_source", pipe_t1_source)
        self.node1 = self.g.create_node("pipe1", pipe_t1_sink, upstream="pipe_source")
        self.node2 = self.g.create_node("pipe2", pipe_t1_to_t2, upstream="pipe_source")
        self.node3 = self.g.create_node("pipe3", pipe_generic, upstream="pipe_source")
        self.sess = ctx.metadata_session
        self.dr1t1 = ctx.merge(self.dr1t1)
        self.dr2t1 = ctx.merge(self.dr2t1)
        self.dr1t2 = ctx.merge(self.dr1t2)
        self.dr2t2 = ctx.merge(self.dr2t2)
        self.graph = ctx.merge(self.graph)
        # self.ds1db1 = ctx.merge(self.ds1db1)

    def test_stream_unprocessed_pristine(self):
        s = StreamBuilder(nodes=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_query(self.ctx).first() is None

    def test_stream_unprocessed_eligible(self):
        dfl = PipeLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            pipe_key=self.node_source.pipe.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            pipe_log=dfl,
            data_block=self.dr1t1,
            direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl])

        s = StreamBuilder(nodes=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_query(self.ctx).first() == self.dr1t1

    def test_stream_unprocessed_ineligible_already_input(self):
        dfl = PipeLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            pipe_key=self.node_source.pipe.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            pipe_log=dfl,
            data_block=self.dr1t1,
            direction=Direction.OUTPUT,
        )
        dfl2 = PipeLog(
            graph_id=self.graph.hash,
            node_key=self.node1.key,
            pipe_key=self.node1.pipe.key,
            runtime_url="test",
        )
        drl2 = DataBlockLog(
            pipe_log=dfl2,
            data_block=self.dr1t1,
            direction=Direction.INPUT,
        )
        self.sess.add_all([dfl, drl, dfl2, drl2])

        s = StreamBuilder(nodes=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_query(self.ctx).first() is None

    def test_stream_unprocessed_ineligible_already_output(self):
        """
        By default we don't input a block that has already been output by a DF, _even if that block was never input_,
        UNLESS input is a self reference (`this`). This is to prevent infinite loops.
        """
        dfl = PipeLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            pipe_key=self.node_source.pipe.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            pipe_log=dfl,
            data_block=self.dr1t1,
            direction=Direction.OUTPUT,
        )
        dfl2 = PipeLog(
            graph_id=self.graph.hash,
            node_key=self.node1.key,
            pipe_key=self.node1.pipe.key,
            runtime_url="test",
        )
        drl2 = DataBlockLog(
            pipe_log=dfl2,
            data_block=self.dr1t1,
            direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl, dfl2, drl2])

        s = StreamBuilder(nodes=self.node_source)
        s1 = s.filter_unprocessed(self.node1)
        assert s1.get_query(self.ctx).first() is None

        # But ok with self reference
        s2 = s.filter_unprocessed(self.node1, allow_cycle=True)
        assert s2.get_query(self.ctx).first() == self.dr1t1

    def test_stream_unprocessed_eligible_schema(self):
        dfl = PipeLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            pipe_key=self.node_source.pipe.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            pipe_log=dfl,
            data_block=self.dr1t1,
            direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl])

        s = StreamBuilder(nodes=self.node_source, schema="TestSchema1")
        s = s.filter_unprocessed(self.node1)
        assert s.get_query(self.ctx).first() == self.dr1t1

        s = StreamBuilder(nodes=self.node_source, schema="TestSchema2")
        s = s.filter_unprocessed(self.node1)
        assert s.get_query(self.ctx).first() is None

    def test_stream_unprocessed_eligible_dataset(self):
        dfl = PipeLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            pipe_key=self.node_source.pipe.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            pipe_log=dfl,
            data_block=self.dr1t1,
            direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl])

        # s = DataBlockStream(upstream=self.node_source)
        # s = s.filter_unprocessed(self.node1)
        # s = s.filter_dataset("dataset1")
        # assert s.get_next(self.ctx) == self.dr1t1

    def test_operators(self):
        dfl = PipeLog(
            graph_id=self.graph.hash,
            node_key=self.node_source.key,
            pipe_key=self.node_source.pipe.key,
            runtime_url="test",
        )
        drl = DataBlockLog(
            pipe_log=dfl,
            data_block=self.dr1t1,
            direction=Direction.OUTPUT,
        )
        drl2 = DataBlockLog(
            pipe_log=dfl,
            data_block=self.dr2t1,
            direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl, drl2])

        self._cnt = 0

        @operator
        def count(stream: DataBlockStream) -> DataBlockStream:
            for db in stream:
                self._cnt += 1
                yield db

        sb = StreamBuilder(nodes=self.node_source)
        expected_cnt = sb.get_query(self.ctx).count()
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
