# SP-2026-10-13-001 | 通达信 TQ 本地源接入 tradex-hub（第一梯队行情备份 + L2 统计 + 公式桥）

> **状态**：Spec（老板 2026-10-13 六问全按推荐拍板）
> **日期**：2026-10-13
> **项目**：tradex-hub（包名 `tradex`，v3.6.0，166 工具）
> **前置侦查**：Obsidian《通达信TQ》三份报告（00/01/02）已核对，关键结论当日实测复核：
> - SDK `tqcenter.py` v1.0.16（4118 行），函数清单与报告一致
> - **HTTP 通道 `POST http://127.0.0.1:17709/`（JSON-RPC 2.0）与 DLL 通道能力等价**（`get_more_info` 88 字段 L2 四项同值），~11ms
> - `subscribe_hq` 盘中实测（11:09）：推送到达但为**空壳通知**（仅 `Code`+`ErrorId`，约 5s/条，无行情载荷）→ "通知-拉取"模式，**对集成无价值，轮询快照为准**
> - 无逐笔明细（穷尽侦查实锤，勿再投入）

---

## 一、背景与目标

通达信客户端（开心果整合版）本地监听 17709 端口，暴露与内嵌 SDK `tqcenter` 同源的 JSON-RPC 方法空间。它给 tradex-hub 带来三块现有源没有/不够好的能力：

| 能力 | 现状痛点 | TQ 方案 |
|---|---|---|
| 第一梯队行情备份 | eltdx 单点（同机协议库），tdx_mcp 走云端需 token+限流 0.8s | 本地 11ms、不封 IP、零 token |
| L2 统计字段 | 无任何源提供逐笔委托数/成交数/买卖撤单额 | `get_more_info` 的 L2OrderNum/L2TicNum/BCancel/SCancel |
| 通达信公式生态 | 技术指标仅 6 个手写 | `formula_process_mul_zb` 可调 200+ 内置公式（含 L2_AMO 等 L2 资金函数） |

**目标**：新增 `tq` 数据源模块接入 SmartRouter 三梯队体系，新增 2 个数据类型 + 2 个 L3 工具，行情快照获得本地低延迟备份。

## 二、要做的行为（使用者视角）

1. **用户在跑通达信客户端时**调用 `get_realtime_quote`：SmartRouter 按健康分在 eltdx / tdx_mcp / **tq** 三个 priority=1 源中自动选优；eltdx 故障时自动切 tq，恢复后自动切回，用户无感。
2. 调用新工具 `get_l2_stats(code)`：返回该票当日 L2 统计（逐笔委托数/逐笔成交数/买撤单额/卖撤单额），供撤单动力学分析。
3. 调用新工具 `run_tdx_formula(formula, codes)`：把通达信公式（如 `L2_AMO`）发给本地客户端计算，返回各票结果序列。
4. **用户没在跑通达信客户端时**：tq 源的 fetch 抛 `TQSourceUnavailable` → SmartRouter 标准失败降级（健康分扣减、120s 冷却半开探测），其余源正常服务；日志记 **DEBUG** 级（区分"缺环境"与"真故障"，不刷 WARNING）。

## 三、决策与取舍（2026-10-13 老板拍板，六问全按推荐）

| # | 决策项 | 结论 | 被否决的替代方案 + 理由 |
|---|---|---|---|
| Q1 | **范围** | 仅数据源接入；**交易通道（order_stock 全家桶）不进本轮**，后续单独 spec 走模拟盘 | 本轮带上交易：未测真金 + 开心果第三方改版，风险应隔离 |
| Q2 | **通道** | 一律 **HTTP 127.0.0.1:17709**（JSON-RPC），不碰 DLL | DLL 通道：需 venv/ctypes/客户端 GUI 前置，且能力已实证与 HTTP 等价 |
| Q3 | **tdx_mcp 收缩** | **本轮不动**云端 MCP 的 realtime_quote/kline 注册；TQ 上线观察健康分后再单独收缩（记入 §七后续项） | 本轮同时收缩：一次变更两个变量，健康分排序互相干扰难归因 |
| Q4 | **降级语义** | `TQSourceUnavailable` 走标准失败降级 + DEBUG 日志；无静默禁用、无特殊开关 | 静默禁用/健康检查开关：增加特殊逻辑，健康分机制天然覆盖该场景 |
| Q5 | **数据类型清单** | `realtime_quote` 备份源（priority=1）+ `l2_stats`（新）+ `tdx_formula`（新）；**K线不注册** | K线：eltdx+akshare 已双备，且 TQ K线需先在客户端手动下载盘后数据，价值低 |
| Q6 | **L3 工具暴露** | 新增 `get_l2_stats(code)`、`run_tdx_formula(formula, codes)` 两个工具；realtime_quote 不加新工具（走现有工具自动选源） | 为 realtime_quote 加透传工具：破坏"工具不指定源"的分层约定 |

**技术事实约束**（侦查实证，非决策）：
- `get_more_info` 返回 `{"ErrorId","Value"}` 包装，L2 字段在 `Value` 内；field_list 过滤参数 `_normalize_field_list` 有坑，fetcher 取全量后自行挑字段。
- HTTP 方法空间 = tqcenter 函数名白名单，不支持的方法返回 `-32601`；无方法发现机制。
- 函数清单实证存在：`get_market_snapshot` / `get_more_info` / `formula_process_mul_zb`。
- 快照返回字符串型数值（"2.20"/"139627"），fetcher 需转数值类型归一化。

## 四、测试决策（接缝）

**唯一接缝：`tq_fetchers.py` 模块级 `_post(method, params)`——唯一 HTTP 出口。**

- 测试全部 mock `_post()`，**不连通真通达信**（CI/开源用户机器可跑）；一个 mock 点覆盖全部 fetch_fn。
- 测什么（只测外部行为，对齐 `test_new_sources_registration.py` / `test_smart_router_failover.py` 既有模式）：
  1. **契约**：三个 fetch_fn 对固定 mock 响应的归一化输出（字段名、数值类型转换、ErrorId≠0 → 抛 `TQSourceUnavailable`）
  2. **不可用路径**：`_post` 抛连接异常 → `TQSourceUnavailable`（不抛裸 httpx 异常）
  3. **注册断言**：`l2_stats`/`tdx_formula` 类型已在 router；`realtime_quote` 含 `tq` 源实例且 priority=1；总类型/源实例基线数更新
  4. **降级**：tq 源失败后 `realtime_quote` 自动落到 eltdx（复用 failover 测试模式，mock 两个源）
- 参照文件：`tests/test_new_sources_registration.py`（注册基线）、`tests/test_smart_router_failover.py`（降级）、`tests/test_new_tools_contract.py`（工具契约）。

## 五、交付物

| 文件 | 内容 |
|---|---|
| `tradex/src/tradex/data_sources/tq_fetchers.py`（新） | `_post()` JSON-RPC 出口（httpx，`trust_env=False` 直连铁律）+ 3 个 fetch_fn + `TQSourceUnavailable`；endpoint 走 env `TQ_ENDPOINT`（默认 `http://127.0.0.1:17709/`），客户端未运行 → 异常即降级 |
| `tradex/src/tradex/data_sources/registry.py`（改） | import tq_fetchers + 3 行注册 + 数据源矩阵注释更新 |
| `tradex/src/tradex/tools/tdx_tq.py`（新） | `get_l2_stats` / `run_tdx_formula` 两个 L3 工具（经 `router.route()`，不直接 import fetchers——铁律对齐 data_sources/__init__.py） |
| `tools/registry.py`（改） | 注册 2 个新工具 |
| `tradex/tests/test_tq_source.py`（新） | §四 全部用例 |
| `README.md` / `docs/MCP_TOOLS.md`（改） | 数据源矩阵表加 tq 行；工具清单 166→168 |

**约定**：先开发不发版（2026-09-21 铁令）——commit+push 保成果，版本号/tag/Release/打包一律不动。

## 六、验收标准

- [ ] `pytest tradex/tests/test_tq_source.py` 全绿（不依赖通达信客户端运行）
- [ ] 既有测试全绿（尤其 `test_new_sources_registration.py` 基线数更新后）
- [ ] 实机验收（通达信客户端运行中）：`get_realtime_quote` 能被路由到 tq 源（`route()` 返回 source_name="tq"），延迟 <100ms
- [ ] 实机验收：`get_l2_stats("600170.SH")` 返回 L2OrderNum/L2TicNum/BCancel/SCancel 四字段且为数值类型
- [ ] 实机验收：`run_tdx_formula("L2_AMO", ["600170.SH"])` 返回公式结果
- [ ] 关掉通达信客户端 → `get_realtime_quote` 自动落 eltdx，日志仅 DEBUG 级，无 WARNING 刷屏
- [ ] 不发版：无版本号 bump、无 tag、无 Release

## 七、范围外与后续项

**明确不做（本轮）**：
- 交易通道（order_stock / cancel_order_stock / query_stock_asset / orders / positions）——后续独立 spec，先模拟盘验证，真金前加二次确认护栏
- `subscribe_hq` 推送集成——实测定论空壳通知，无价值
- DLL 通道——HTTP 等价且更简
- K线 / 财务 / 板块 / 打新 / 可转债等 TQ 其他数据——现有源已覆盖或价值低，需要时再加注册行（fetcher 模块已留扩展位）

**后续项（观察后决策）**：
- tdx_mcp 的 realtime_quote/historical_kline 注册行收缩（TQ 健康分稳定运行一段时间后）
- `get_market_data` K线备份源（若 eltdx K线出问题再启用）

---
*wolfjkd · 2026-10-13 · 侦查与拍板同日完成，标注 ready-for-agent*
