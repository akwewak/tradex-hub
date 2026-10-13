"""
tq_fetchers.py - 通达信 TQ 本地源 fetch_fn 包装器（SP-2026-10-13-001）

============================================================
接入 tradex-hub 多源中台的「本地第一梯队」数据源。

定位（老板 2026-10-13 拍板 · SP-2026-10-13-001）：
  - 与 eltdx / tdx_mcp 同属 realtime_quote 的 priority=1，互为备份。
  - 通过 SmartRouter 健康分动态互备：eltdx 故障时自动切到本源，恢复后自动切回。
  - 通道一律走 HTTP JSON-RPC（127.0.0.1:17709），不碰 DLL/venv（实测与 DLL 能力等价，~11ms）。

传输协议（2026-10-13 实测）：
  POST http://127.0.0.1:17709/
  body: {"jsonrpc":"2.0","id":N,"method":"<tqcenter函数名>","params":{...}}
  方法空间 = tqcenter 函数名白名单，不支持的方法返回 JSON-RPC error（-32601）。
  依赖通达信客户端运行中；未运行时连接失败 → TQSourceUnavailable → 标准降级。

数据口径（实测）：
  - get_market_snapshot：五档+内外盘，数值均为字符串；成交量/内外盘/现手=手，成交额=万元；
    快照无涨跌额字段，由 最新价-昨收 现算；涨跌幅取快照 ZAF 字段。
  - get_more_info：result 顶层为 {"ErrorId","Value"} 包装，L2 统计字段在 Value 内
    （L2OrderNum 逐笔委托数 / L2TicNum 逐笔成交数 / BCancel 买撤单额 / SCancel 卖撤单额）。
  - formula_process_mul_zb：批量执行通达信内置公式；result 为 {"<code>":{线名:[值]},...,"ErrorId"};
    stock_period 必须显式传（缺省报 periodstr error）；L2 系公式取决于客户端公式系统是否可用。

铁律（对齐 data_sources/__init__.py）：
  仅本文件允许直接访问本地 17709 端点。
  所有 L1 工具必须通过 SmartRouter.route() 获取数据，不得直接 import 本模块。
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Optional

import httpx
import pandas as pd

logger = logging.getLogger("tradex.tq")

# 默认本地端点（通达信客户端 MCP/JSON-RPC 监听口，实测固定 17709）
_DEFAULT_ENDPOINT = "http://127.0.0.1:17709/"
_DEFAULT_TIMEOUT = 8.0

# 模块级客户端缓存（SmartRouter fetch_fn 在线程池并发执行，需加锁）
_client_lock = threading.Lock()
_client: Optional[httpx.Client] = None
_req_id = 0
_req_id_lock = threading.Lock()


class TQSourceUnavailable(RuntimeError):
    """通达信 TQ 本地源不可用（客户端未运行 / 超时 / 方法被拒 / 业务 ErrorId≠0）。

    SmartRouter 的 route() 把它当普通源失败处理：健康分扣减、自动降级。
    客户端未运行是常态（开源用户环境），日志记 DEBUG 级避免刷屏（老板拍板 Q4）。
    """


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                # 铁律：国内接口直连。trust_env=False 不读 HTTP(S)_PROXY 环境变量。
                _client = httpx.Client(
                    timeout=float(os.getenv("TQ_TIMEOUT", _DEFAULT_TIMEOUT)),
                    trust_env=False,
                )
    return _client


def _post(method: str, params: dict | None = None) -> dict:
    """唯一 HTTP 出口：POST JSON-RPC 2.0 到本地通达信 17709。

    任何失败统一抛 TQSourceUnavailable（SmartRouter 据此扣健康分并降级）。
    测试接缝：单测 mock 本函数即可覆盖全部 fetch_fn（不连通真客户端）。
    """
    global _req_id
    endpoint = os.getenv("TQ_ENDPOINT", _DEFAULT_ENDPOINT)
    with _req_id_lock:
        _req_id += 1
        req_id = _req_id
    payload = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or {}}
    try:
        resp = _get_client().post(endpoint, json=payload)
    except (httpx.HTTPError, OSError) as e:
        # 连接拒绝/超时 → 客户端大概率没开，属常态：DEBUG 级，不刷 WARNING
        logger.debug("TQ source unavailable (%s): %s", method, e)
        raise TQSourceUnavailable(f"TQ 客户端未运行或不可达: {e}") from e
    if resp.status_code != 200:
        logger.debug("TQ source HTTP %s on %s", resp.status_code, method)
        raise TQSourceUnavailable(f"TQ HTTP {resp.status_code}")
    try:
        body = resp.json()
    except ValueError as e:
        raise TQSourceUnavailable(f"TQ 返回非 JSON: {e}") from e
    if "error" in body:  # JSON-RPC 协议层错误（如 -32601 方法不在白名单）
        logger.debug("TQ method %s rejected: %s", method, body["error"])
        raise TQSourceUnavailable(f"TQ 方法被拒: {body['error']}")
    result = body.get("result")
    if not isinstance(result, dict):
        raise TQSourceUnavailable(f"TQ 返回缺少 result: {body}")
    return result


def _to_num(v: Any, default: float | None = None) -> float | None:
    """快照/公式返回的字符串数值 → float（失败回退 default）。"""
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _norm_code(code: str = "", symbol: str = "") -> str:
    """归一化到通达信带后缀格式（600519 / sh600519 / 600519.SH → 600519.SH）。"""
    raw = str(symbol or code or "").strip()
    if not raw:
        raise ValueError("code/symbol 不能为空")
    raw = raw.lower().removeprefix("sh").removeprefix("sz").removeprefix("bj")
    code6 = raw.split(".")[0].zfill(6)
    if code6.startswith(("6", "9", "5")):
        return f"{code6}.SH"
    if code6.startswith(("8", "4")):
        return f"{code6}.BJ"
    return f"{code6}.SZ"


# ── fetch_fn：realtime_quote（与 eltdx/tdx_mcp 平级互备） ──

def fetch_realtime_quote_tq(code: str = "", symbol: str = "", **kwargs):
    """实时行情（tq 本地源）。返回单行 DataFrame，中文列名对齐 eltdx 版。

    单位口径：成交量/内盘/外盘/现手=手，成交额=万元（通达信快照口径）。
    """
    norm = _norm_code(code=code, symbol=symbol)
    snap = _post("get_market_snapshot", {"stock_code": norm})
    if str(snap.get("ErrorId", "0")) != "0":
        raise TQSourceUnavailable(f"TQ snapshot ErrorId={snap.get('ErrorId')}: {snap.get('Error')}")
    last = _to_num(snap.get("Now"))
    pre = _to_num(snap.get("LastClose"))
    change = round(last - pre, 3) if (last is not None and pre is not None) else None
    # 涨跌幅自算：快照 ZAF 字段时有时无（2026-10-13 两次实测对比确认），不可依赖
    chg_pct = (round(change / pre * 100, 2)
               if (change is not None and pre) else _to_num(snap.get("ZAF")))
    return pd.DataFrame([{
        "代码": norm,
        "最新价": last,
        "昨收": pre,
        "今开": _to_num(snap.get("Open")),
        "最高": _to_num(snap.get("Max")),
        "最低": _to_num(snap.get("Min")),
        "涨跌额": change,
        "涨跌幅": chg_pct,
        "成交量": _to_num(snap.get("Volume")),      # 手
        "成交额": _to_num(snap.get("Amount")),      # 万元
        "内盘": _to_num(snap.get("Inside")),        # 手
        "外盘": _to_num(snap.get("Outside")),       # 手
        "现手": _to_num(snap.get("NowVol")),        # 手
    }])


# ── fetch_fn：l2_stats（tq 独占增量，无其他源） ──

_L2_FIELDS = ("L2OrderNum", "L2TicNum", "BCancel", "SCancel")


def fetch_l2_stats(code: str = "", symbol: str = "", **kwargs) -> dict:
    """当日 L2 统计（tq 本地源独占）：逐笔委托数/逐笔成交数/买撤单额/卖撤单额。

    返回 dict：{"代码", "L2OrderNum", "L2TicNum", "BCancel", "SCancel"}（数值类型）。
    撤单动力学分析原料；数据随盘中累计，当日口径。
    """
    norm = _norm_code(code=code, symbol=symbol)
    result = _post("get_more_info", {"stock_code": norm})
    if str(result.get("ErrorId", "0")) != "0":
        raise TQSourceUnavailable(f"TQ more_info ErrorId={result.get('ErrorId')}: {result.get('Error')}")
    value = result.get("Value")
    if not isinstance(value, dict):
        raise TQSourceUnavailable("TQ more_info 返回缺少 Value 包装")
    missing = [k for k in _L2_FIELDS if k not in value]
    if missing:
        raise TQSourceUnavailable(f"TQ L2 字段缺失（客户端环境无 L2 数据？）: {missing}")
    return {
        "代码": norm,
        "L2OrderNum": _to_num(value["L2OrderNum"], 0),
        "L2TicNum": _to_num(value["L2TicNum"], 0),
        "BCancel": _to_num(value["BCancel"], 0.0),
        "SCancel": _to_num(value["SCancel"], 0.0),
    }


# ── fetch_fn：tdx_formula（通达信公式生态桥，tq 独占增量） ──

def fetch_tdx_formula(formula: str = "", stock_list: list | None = None,
                      return_count: int = 1, stock_period: str = "1d", **kwargs) -> dict:
    """批量执行通达信内置公式（如 MA/MACD/KDJ 等任意客户端可用公式）。

    返回 dict：{"<code>": {"<线名>": [值, ...], ...}, ...}（数值类型，None 保留）。
    注意：L2 系公式（L2_AMO 等）取决于客户端公式系统是否提供，不存在时报
    ErrorId=9 → TQSourceUnavailable（2026-10-13 实测：当前开心果版无 L2_AMO）。
    """
    if not formula or not formula.strip():
        raise ValueError("formula 不能为空")
    codes = [_norm_code(code=c) for c in (stock_list or [])]
    if not codes:
        raise ValueError("stock_list 不能为空")
    result = _post("formula_process_mul_zb", {
        "formula_name": formula.strip(),
        "stock_list": codes,
        "stock_period": stock_period or "1d",
        "return_count": max(1, int(return_count)),
    })
    if str(result.get("ErrorId", "0")) != "0":
        raise TQSourceUnavailable(
            f"TQ formula '{formula}' ErrorId={result.get('ErrorId')}: {result.get('Error')}"
        )
    out: dict[str, dict[str, list]] = {}
    for key, lines in result.items():
        if key == "ErrorId" or not isinstance(lines, dict):
            continue  # result 内 ErrorId 与票代码键混放（实测），跳过非数据键
        out[key] = {
            line: [None if v is None else _to_num(v, v) for v in vals]
            for line, vals in lines.items() if isinstance(vals, list)
        }
    if not out:
        raise TQSourceUnavailable(f"TQ formula '{formula}' 返回无数据")
    return out
