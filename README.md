# Routing Path Dynamics Analysis

本仓库用于从已经重建完成的 traceroute AS 路径与 ASGeo 路径中筛选长期稳定的路径，并为后续通过 `/24` 关联 Ping 质量数据建立固定研究对象。

## 当前分析边界

- 数据源只使用 traceroute；Ping 不参与稳定路径筛选。
- 直接读取最新的 `as_path_text` 与 `asgeo_path_text`，不从旧路径字段反推，也不二次改写路径语义。
- 因此路径中的数据中心起点、目标末端、`AS0`、`*`、共享地址和保留地址标签均按上游重建结果原样参与统计。
- 默认研究窗口为 UTC `[2024-06-22, 2025-01-10)`；2024-06 与 2025-01 是不完整月，严格长期比较使用 2024-07 至 2024-12 六个完整月。

## 三层稳定性分析

1. `/24` 层面：按月统计每个 `/24` 的 AS/ASGeo 路径分布、主路径占比以及相邻月份主路径是否改变。
2. 路径层面：统计每条路径出现的月份、每月观测次数及全周期复现程度。
3. 路径覆盖层面：统计每条路径每月覆盖的 `/24` 集合，并用相邻月份 Jaccard 系数衡量覆盖集合是否稳定。

脚本同时分析 AS 与 ASGeo 两种完整路径字符串。主路径出现并列时保留并列标记，不任意选择其中一条。

## 运行

安装依赖：

```bash
python -m pip install -r requirements-analysis.txt
```

通过环境变量提供非秘密连接参数，密码在运行时交互输入，不写入代码、命令或日志：

```bash
export CLICKHOUSE_HOST=your-clickhouse-host
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=your-readonly-user
export CLICKHOUSE_DATABASE=default
export CLICKHOUSE_TABLE=database.traceroute_table
python scripts/analyze_path_stability_three_layers.py
```

生成的 CSV、图形和摘要写入 `outputs/path_stability_three_layers_<UTC时间>/`。`outputs/`、缓存、环境文件和凭据均不进入 Git。

## 后续 Ping 关联原则

稳定路径集合冻结后，再按 `目标IP → /24 → 稳定路径` 关联 Ping。Ping 只用于衡量已筛选路径的质量，不能反向参与路径稳定性的定义。建议先聚合到 `/24-月`，再聚合到 `路径-月`，避免探测样本较多的 `/24` 对结果产生过大权重。
