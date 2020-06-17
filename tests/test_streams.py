from __future__ import annotations

import pytest

from basis.core.data_block import DataBlockMetadata, StoredDataBlockMetadata
from basis.core.function_node import DataBlockLog, DataFunctionLog, Direction
from basis.core.streams import DataBlockStream
from tests.utils import (
    TestType1,
    df_generic,
    df_t1_sink,
    df_t1_source,
    df_t1_to_t2,
    make_test_execution_context,
)


class TestStreams:
    def setup(self):
        ctx = make_test_execution_context()
        self.ctx = ctx
        self.env = ctx.env
        self.dr1t1 = DataBlockMetadata(
            expected_otype_uri="_test.TestType1", realized_otype_uri="_test.TestType1"
        )
        self.dr2t1 = DataBlockMetadata(
            expected_otype_uri="_test.TestType1", realized_otype_uri="_test.TestType1"
        )
        self.dr1t2 = DataBlockMetadata(
            expected_otype_uri="_test.TestType2", realized_otype_uri="_test.TestType2"
        )
        self.dr2t2 = DataBlockMetadata(
            expected_otype_uri="_test.TestType2", realized_otype_uri="_test.TestType2"
        )
        self.node_source = self.env.add_node("df_source", df_t1_source)
        self.node1 = self.env.add_node("df1", df_t1_sink)
        self.node2 = self.env.add_node("df2", df_t1_to_t2)
        self.node3 = self.env.add_node("df3", df_generic)
        self.sess = ctx.metadata_session
        self.dr1t1 = ctx.merge(self.dr1t1)
        self.dr2t1 = ctx.merge(self.dr2t1)
        self.dr1t2 = ctx.merge(self.dr1t2)
        self.dr2t2 = ctx.merge(self.dr2t2)

    def test_stream_unprocessed_pristine(self):
        s = DataBlockStream(upstream=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_next(self.ctx) is None

    def test_stream_unprocessed_eligible(self):
        dfl = DataFunctionLog(
            function_node_name=self.node_source.name,
            data_function_uri=self.node_source.datafunction.uri,
            runtime_url="test",
        )
        drl = DataBlockLog(
            data_function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl])

        s = DataBlockStream(upstream=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_next(self.ctx) == self.dr1t1

    def test_stream_unprocessed_ineligible_already_input(self):
        dfl = DataFunctionLog(
            function_node_name=self.node_source.name,
            data_function_uri=self.node_source.datafunction.uri,
            runtime_url="test",
        )
        drl = DataBlockLog(
            data_function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        dfl2 = DataFunctionLog(
            function_node_name=self.node1.name,
            data_function_uri=self.node1.datafunction.uri,
            runtime_url="test",
        )
        drl2 = DataBlockLog(
            data_function_log=dfl2, data_block=self.dr1t1, direction=Direction.INPUT,
        )
        self.sess.add_all([dfl, drl, dfl2, drl2])

        s = DataBlockStream(upstream=self.node_source)
        s = s.filter_unprocessed(self.node1)
        assert s.get_next(self.ctx) is None

    def test_stream_unprocessed_ineligible_already_output(self):
        """
        By default we don't input a DB that has already been output by a DF, _even if that DB was never input_,
        UNLESS input is a self reference (`this`). This is to prevent infinite loops.
        """
        dfl = DataFunctionLog(
            function_node_name=self.node_source.name,
            data_function_uri=self.node_source.datafunction.uri,
            runtime_url="test",
        )
        drl = DataBlockLog(
            data_function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        dfl2 = DataFunctionLog(
            function_node_name=self.node1.name,
            data_function_uri=self.node1.datafunction.uri,
            runtime_url="test",
        )
        drl2 = DataBlockLog(
            data_function_log=dfl2, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl, dfl2, drl2])

        s = DataBlockStream(upstream=self.node_source)
        s1 = s.filter_unprocessed(self.node1)
        assert s1.get_next(self.ctx) is None

        # But ok with self reference
        s2 = s.filter_unprocessed(self.node1, allow_cycle=True)
        assert s2.get_next(self.ctx) == self.dr1t1

    def test_stream_unprocessed_eligible_otype(self):
        dfl = DataFunctionLog(
            function_node_name=self.node_source.name,
            data_function_uri=self.node_source.datafunction.uri,
            runtime_url="test",
        )
        drl = DataBlockLog(
            data_function_log=dfl, data_block=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl])

        s = DataBlockStream(upstream=self.node_source, otype="TestType1")
        s = s.filter_unprocessed(self.node1)
        assert s.get_next(self.ctx) == self.dr1t1

        s = DataBlockStream(upstream=self.node_source, otype="TestType2")
        s = s.filter_unprocessed(self.node1)
        assert s.get_next(self.ctx) is None

    # Deprecated for now
    # def test_stream_records_object(self):
    #     records = [{"a": 1, "b": 2}]
    #     s = DataBlockStream(raw_records_object=records, raw_records_otype=TestType1)
    #     db = s.get_next(self.ctx)
    #     self.ctx.metadata_session.commit()
    #     assert db is not None
    #     db = db.as_managed_data_block(self.ctx)
    #     assert db.as_records_list() == records
