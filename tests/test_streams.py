from __future__ import annotations

import pytest

from dags.core.data_block import DataBlockMetadata, StoredDataBlockMetadata
from dags.core.graph import Graph
from dags.core.node import DataBlockLog, Direction, PipeLog
from dags.core.streams import DataBlockStreamBuilder
from tests.utils import (
    TestSchema1,
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
        self.node_source = self.g.add_node("pipe_source", pipe_t1_source)
        self.node1 = self.g.add_node("pipe1", pipe_t1_sink)
        self.node2 = self.g.add_node("pipe2", pipe_t1_to_t2)
        self.node3 = self.g.add_node("pipe3", pipe_generic)
        self.sess = ctx.metadata_session
        self.dr1t1 = ctx.merge(self.dr1t1)
        self.dr2t1 = ctx.merge(self.dr2t1)
        self.dr1t2 = ctx.merge(self.dr1t2)
        self.dr2t2 = ctx.merge(self.dr2t2)
        self.graph = ctx.merge(self.graph)
        # self.ds1db1 = ctx.merge(self.ds1db1)

    def test_stream_unprocessed_pristine(self):
        s = DataBlockStreamBuilder(upstream=self.node_source)
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

        s = DataBlockStreamBuilder(upstream=self.node_source)
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

        s = DataBlockStreamBuilder(upstream=self.node_source)
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

        s = DataBlockStreamBuilder(upstream=self.node_source)
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

        s = DataBlockStreamBuilder(upstream=self.node_source, schema="TestSchema1")
        s = s.filter_unprocessed(self.node1)
        assert s.get_query(self.ctx).first() == self.dr1t1

        s = DataBlockStreamBuilder(upstream=self.node_source, schema="TestSchema2")
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

    # Deprecated for now
    # def test_stream_records_object(self):
    #     records = [{"a": 1, "b": 2}]
    #     s = DataBlockStream(raw_records_object=records, raw_records_schema=TestSchema1)
    #     block = s.get_next(self.ctx)
    #     self.ctx.metadata_session.commit()
    #     assert block is not None
    #     block = block.as_managed_data_block(self.ctx)
    #     assert block.as_records_list() == records
