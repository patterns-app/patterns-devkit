from __future__ import annotations

import pytest

from basis.core.data_function import DataFunctionLog, DataResourceLog, Direction
from basis.core.data_resource import DataResourceMetadata
from basis.core.streams import DataResourceStream
from tests.utils import (
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
        self.dr1t1 = DataResourceMetadata(otype_uri="_test.TestType1",)
        self.dr2t1 = DataResourceMetadata(otype_uri="_test.TestType1",)
        self.dr1t2 = DataResourceMetadata(otype_uri="_test.TestType2",)
        self.dr2t2 = DataResourceMetadata(otype_uri="_test.TestType2",)
        self.cdf_source = self.env.node("df_source", df_t1_source)
        self.cdf1 = self.env.node("df1", df_t1_sink)
        self.cdf2 = self.env.node("df2", df_t1_to_t2)
        self.cdf3 = self.env.node("df3", df_generic)
        self.sess = ctx.metadata_session
        self.sess.add_all([self.dr1t1, self.dr1t2, self.dr2t1, self.dr2t2])

    def test_stream_unprocessed_pristine(self):
        s = DataResourceStream(upstream=self.cdf_source)
        s = s.filter_unprocessed(self.cdf1)
        assert s.get_next(self.ctx) is None

    def test_stream_unprocessed_eligible(self):
        dfl = DataFunctionLog(
            configured_data_function_key=self.cdf_source.key,
            runtime_resource_url="test",
        )
        drl = DataResourceLog(
            data_function_log=dfl, data_resource=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl])

        s = DataResourceStream(upstream=self.cdf_source)
        s = s.filter_unprocessed(self.cdf1)
        assert s.get_next(self.ctx) == self.dr1t1

    def test_stream_unprocessed_ineligible_already_input(self):
        dfl = DataFunctionLog(
            configured_data_function_key=self.cdf_source.key,
            runtime_resource_url="test",
        )
        drl = DataResourceLog(
            data_function_log=dfl, data_resource=self.dr1t1, direction=Direction.OUTPUT,
        )
        dfl2 = DataFunctionLog(
            configured_data_function_key=self.cdf1.key, runtime_resource_url="test",
        )
        drl2 = DataResourceLog(
            data_function_log=dfl2, data_resource=self.dr1t1, direction=Direction.INPUT,
        )
        self.sess.add_all([dfl, drl, dfl2, drl2])

        s = DataResourceStream(upstream=self.cdf_source)
        s = s.filter_unprocessed(self.cdf1)
        assert s.get_next(self.ctx) is None

    def test_stream_unprocessed_ineligible_already_output(self):
        """
        By default we don't input a DR that has already been output by a DF, _even if that DR was never input_,
        UNLESS input is a self reference (`this`). This is to prevent infinite loops.
        """
        dfl = DataFunctionLog(
            configured_data_function_key=self.cdf_source.key,
            runtime_resource_url="test",
        )
        drl = DataResourceLog(
            data_function_log=dfl, data_resource=self.dr1t1, direction=Direction.OUTPUT,
        )
        dfl2 = DataFunctionLog(
            configured_data_function_key=self.cdf1.key, runtime_resource_url="test",
        )
        drl2 = DataResourceLog(
            data_function_log=dfl2,
            data_resource=self.dr1t1,
            direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl, dfl2, drl2])

        s = DataResourceStream(upstream=self.cdf_source)
        s1 = s.filter_unprocessed(self.cdf1)
        assert s1.get_next(self.ctx) is None

        # But ok with self reference
        s2 = s.filter_unprocessed(self.cdf1, allow_cycle=True)
        assert s2.get_next(self.ctx) == self.dr1t1

    def test_stream_unprocessed_eligible_otype(self):
        dfl = DataFunctionLog(
            configured_data_function_key=self.cdf_source.key,
            runtime_resource_url="test",
        )
        drl = DataResourceLog(
            data_function_log=dfl, data_resource=self.dr1t1, direction=Direction.OUTPUT,
        )
        self.sess.add_all([dfl, drl])

        s = DataResourceStream(upstream=self.cdf_source, otype="TestType1")
        s = s.filter_unprocessed(self.cdf1)
        assert s.get_next(self.ctx) == self.dr1t1

        s = DataResourceStream(upstream=self.cdf_source, otype="TestType2")
        s = s.filter_unprocessed(self.cdf1)
        assert s.get_next(self.ctx) is None
