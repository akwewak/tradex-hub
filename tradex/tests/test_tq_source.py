"""SP-2026-10-13-001 测试：通达信 TQ 本地源（tq_fetchers + registry + tools）。

验证（不连通真通达信客户端，唯一 mock 点 = tq_fetchers._post）：
- 三个 fetch_fn 对固定 mock 响应的归一化输出（契约）
- 连接异常 / ErrorId≠0 / 字段缺失 → TQSourceUnavailable
- registry 注册：l2_stats / tdx_formula 新类型 + realtime_quote 的 tq 源实例
- 降级：tq 失败后 realtime_quote 自动落到 eltdx
- L3 工具 get_l2_stats / run_tdx_formula 注册与调用契约
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tradex.data_sources.tq_fetchers import (
    TQSourceUnavailable,
    fetch_l2_stats,
    fetch_realtime_quote_tq,
    fetch_tdx_formula,
)


@pytest.fixture(autouse=True)
def _ensure_registered():
    from tradex.data_sources import register_all_sources
    register_all_sources()


# 2026-10-13 实测响应样本（口径见 tq_fetchers 模块 docstring）
MOCK_SNAPSHOT = {
    "ErrorId": "0", "Now": "2.20", "LastClose": "2.21", "Open": "2.21",
    "Max": "2.23", "Min": "2.20", "ZAF": "-0.45", "Volume": "465225",
    "Amount": "10286.88", "Inside": "202879", "Outside": "262347", "NowVol": "17",
}
MOCK_MORE_INFO = {
    "ErrorId": "0",
    "Value": {"L2OrderNum": "22745", "L2TicNum": "9808",
              "BCancel": "535687.00", "SCancel": "462662.00", "Zsz": "195.49"},
}
MOCK_FORMULA = {
    "600170.SH": {"MA1": ["2.21", "2.20"], "MA2": [None, "2.19"]},
    "ErrorId": "0",
}


class TestRealtimeQuoteContract:
    """fetch_realtime_quote_tq 契约：归一化 + 数值类型。"""

    def test_normalizes_snapshot(self):
        with patch("tradex.data_sources.tq_fetchers._post", return_value=MOCK_SNAPSHOT):
            df = fetch_realtime_quote_tq(code="600170")
        assert isinstance(df, pd.DataFrame) and len(df) == 1
        row = df.iloc[0]
        assert row["代码"] == "600170.SH"
        assert row["最新价"] == 2.20 and row["昨收"] == 2.21
        assert row["涨跌额"] == -0.01          # 现算
        assert row["涨跌幅"] == -0.45          # 自算：(2.20-2.21)/2.21*100 ≈ -0.45，ZAF 字段时有时无不依赖
        assert row["成交量"] == 465225.0       # 手
        # 全部价格/量额字段均为数值类型（快照原始为字符串）
        for col in ("最新价", "成交量", "内盘", "外盘", "现手"):
            assert isinstance(row[col], (int, float)), f"{col} 应为数值"

    def test_symbol_alias(self):
        with patch("tradex.data_sources.tq_fetchers._post", return_value=MOCK_SNAPSHOT) as m:
            fetch_realtime_quote_tq(symbol="600170.SH")
        assert m.call_args[0][1]["stock_code"] == "600170.SH"

    def test_errorid_raises(self):
        bad = dict(MOCK_SNAPSHOT, ErrorId="7", Error="server return none")
        with patch("tradex.data_sources.tq_fetchers._post", return_value=bad):
            with pytest.raises(TQSourceUnavailable):
                fetch_realtime_quote_tq(code="600170")

    def test_blank_code_raises(self):
        with pytest.raises(ValueError):
            fetch_realtime_quote_tq()


class TestL2StatsContract:
    """fetch_l2_stats 契约：Value 包装内取四字段并数值化。"""

    def test_normalizes_l2_fields(self):
        with patch("tradex.data_sources.tq_fetchers._post", return_value=MOCK_MORE_INFO):
            out = fetch_l2_stats(code="600170.SH")
        assert out["代码"] == "600170.SH"
        assert out["L2OrderNum"] == 22745.0
        assert out["L2TicNum"] == 9808.0
        assert out["BCancel"] == 535687.0
        assert out["SCancel"] == 462662.0

    def test_missing_value_wrapper_raises(self):
        with patch("tradex.data_sources.tq_fetchers._post",
                   return_value={"ErrorId": "0", "Value": None}):
            with pytest.raises(TQSourceUnavailable):
                fetch_l2_stats(code="600170")

    def test_missing_l2_field_raises(self):
        partial = {"ErrorId": "0", "Value": {"L2OrderNum": "1"}}
        with patch("tradex.data_sources.tq_fetchers._post", return_value=partial):
            with pytest.raises(TQSourceUnavailable, match="L2 字段缺失"):
                fetch_l2_stats(code="600170")


class TestFormulaContract:
    """fetch_tdx_formula 契约：多票归一化 + ErrorId 跳过 + 数值转换。"""

    def test_normalizes_formula_result(self):
        with patch("tradex.data_sources.tq_fetchers._post", return_value=MOCK_FORMULA):
            out = fetch_tdx_formula(formula="MA", stock_list=["600170"])
        assert "600170.SH" in out and "ErrorId" not in out
        assert out["600170.SH"]["MA1"] == [2.21, 2.20]
        assert out["600170.SH"]["MA2"] == [None, 2.19]  # None 保留

    def test_formula_not_exists_raises(self):
        bad = {"Error": "获取公式失败或公式不存在。", "ErrorId": "9"}
        with patch("tradex.data_sources.tq_fetchers._post", return_value=bad):
            with pytest.raises(TQSourceUnavailable):
                fetch_tdx_formula(formula="L2_AMO", stock_list=["600170.SH"])

    def test_empty_args_raise(self):
        with pytest.raises(ValueError):
            fetch_tdx_formula(formula="  ", stock_list=["600170"])
        with pytest.raises(ValueError):
            fetch_tdx_formula(formula="MA", stock_list=[])


class TestUnavailablePath:
    """_post 出口异常统一包装为 TQSourceUnavailable（客户端没开是常态）。"""

    def test_connection_refused(self):
        import httpx
        with patch("tradex.data_sources.tq_fetchers._get_client",
                   side_effect=httpx.ConnectError("connection refused")):
            with pytest.raises(TQSourceUnavailable):
                fetch_realtime_quote_tq(code="600170")


class TestRegistryRegistration:
    """注册断言：新类型 + realtime_quote 的 tq 源实例。"""

    def test_new_types_registered(self):
        from tradex.data_sources import get_router
        r = get_router()
        for dtype in ("l2_stats", "tdx_formula"):
            assert dtype in r._sources, f"{dtype} 未注册"

    def test_realtime_quote_has_tq_source(self):
        from tradex.data_sources import get_router
        sources = get_router()._sources.get("realtime_quote", [])
        tq_entries = [s for s in sources if s[0] == "tq"]
        assert len(tq_entries) == 1
        assert tq_entries[0][2] == 1  # priority=1（与 eltdx/tdx_mcp 平级）
        assert tq_entries[0][3] is False  # 非独占，可降级


class TestFailover:
    """tq 失败后 realtime_quote 自动落到 eltdx（健康分降级）。"""

    def test_tq_fail_falls_back_to_eltdx(self):
        from tradex.data_sources import get_router
        from tradex.data_sources import eltdx_fetchers as ef
        from tradex.data_sources import tq_fetchers as tqf

        fake = pd.DataFrame([{"代码": "600170.SH", "最新价": 2.20}])
        with patch.object(tqf, "fetch_realtime_quote_tq",
                          side_effect=TQSourceUnavailable("客户端未运行")), \
             patch.object(ef, "fetch_realtime_quote", return_value=fake):
            data, src = get_router().route("realtime_quote", code="600170")
        assert src == "eltdx"
        assert isinstance(data, pd.DataFrame)


class TestL3Tools:
    """L3 工具注册与调用契约（mock route）。"""

    def _get_tool_fn(self, mcp_instance, name: str):
        return mcp_instance._tool_manager._tools[name].fn

    def _make_mcp(self):
        from mcp.server.fastmcp import FastMCP
        from tradex.tools.tdx_tq import register
        mcp = FastMCP("tq_test")
        register(mcp)
        return mcp

    def test_tools_registered(self):
        mcp = self._make_mcp()
        registered = set(mcp._tool_manager._tools.keys())
        assert {"get_l2_stats", "run_tdx_formula"} <= registered

    def test_get_l2_stats_returns_json(self):
        with patch("tradex.tools.tdx_tq._router") as mock_router:
            mock_router.route.return_value = (
                {"代码": "600170.SH", "L2OrderNum": 22745.0}, "tq")
            fn = self._get_tool_fn(self._make_mcp(), "get_l2_stats")
            out = asyncio.run(fn("600170"))
        data = json.loads(out)
        assert data["L2OrderNum"] == 22745.0

    def test_run_tdx_formula_returns_json(self):
        with patch("tradex.tools.tdx_tq._router") as mock_router:
            mock_router.route.return_value = (
                {"600170.SH": {"MA1": [2.21]}}, "tq")
            fn = self._get_tool_fn(self._make_mcp(), "run_tdx_formula")
            out = asyncio.run(fn("MA", "600170,601868"))
        data = json.loads(out)
        assert "600170.SH" in data

    def test_unavailable_returns_error_json(self):
        """客户端没开 → route 抛异常 → error_response 兜底，不外泄异常。"""
        with patch("tradex.tools.tdx_tq._router") as mock_router:
            mock_router.route.side_effect = TQSourceUnavailable("TQ 客户端未运行")
            fn = self._get_tool_fn(self._make_mcp(), "get_l2_stats")
            out = asyncio.run(fn("000001"))  # 换 code，避开上一用例写入的缓存键
        data = json.loads(out)
        assert "error" in json.dumps(data) or data.get("success") is False
