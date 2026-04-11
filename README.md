# SmartHomeEnergyAnalysis

**32 months of real smart home data from a single-family house in Nordwijk, NL —
built into a full analytical pipeline from raw files to insight.**

Developed in collaboration with **Statistics Netherlands (CBS)**.

---

## What This Project Is

A household in Nordwijk, the Netherlands had its electricity meter, gas meter, and
~40 smart home devices (motion sensors, temperature sensors, door sensors, smart plugs,
etc.) generating data continuously from early 2022 through early 2025. This project
takes that raw, multi-format, overlapping data and turns it into a clean, queryable
database and a series of reproducible analyses.

The goal is twofold: demonstrate a complete data engineering + analytics workflow on
real-world messy data, and produce findings relevant to CBS's energy transition research.

---

## Dataset

| Source | Format | Resolution | Period | Rows (after dedup) |
|--------|--------|-----------|--------|--------------------|
| P1 electricity meter | CSV / CSV.gz | 15 min | Mar 2022 – Mar 2025 | 106,013 |
| P1 gas meter | CSV / CSV.gz | 15 min | Mar 2022 – Mar 2025 | 106,018 |
| SmartThings hub | TSV.gz | Event-driven | Oct 2022 – Apr 2025 | 1,725,906 |
| Open-Meteo weather | JSON (API) | 1 hour | Jan 2022 – Apr 2025 | 28,464 |

Raw files contain significant overlap between collection periods. The ETL pipeline
handles deduplication automatically — 52% of P1 rows and 27% of SmartThings rows
were duplicates across files.

---

## Technical Stack

```
Raw Files (CSV / TSV / JSON)
        │
        ▼
  ETL Layer  (Python CLI tools, Click)
  ├─ p1e.py              electricity ingestion
  ├─ p1g.py              gas ingestion
  ├─ smartthings.py      device message ingestion
  └─ openweathermap.py   weather fetch + ingestion
        │
        ▼
  SQLite Database  (SQLAlchemy ORM)
  └─ home_messages_db.py — sole database interface
        │
        ▼
  Analysis Layer  (Pandas, Matplotlib, Jupyter)
  └─ report_*.ipynb
```

**Languages & Libraries:** Python 3.13 · pandas 3.0 · SQLAlchemy 2.0 · Click · Matplotlib · Plotly · Dash · requests

---

## Project Structure

```
SmartHomeEnergyAnalysis/
├── home_messages_db.py            Database access class (only entry point to the DB)
├── p1e.py                         CLI: load electricity data
├── p1g.py                         CLI: load gas data
├── smartthings.py                 CLI: load SmartThings device messages
├── openweathermap.py              CLI: fetch and store weather data
├── report_data_quality.ipynb      Data quality audit ✅
├── report_energy_analysis.ipynb   Comprehensive energy analysis ✅
├── ANALYSIS_LOG.md                Decision and reflection log
├── requirements.txt
└── data/data/
    ├── P1e/                       Raw electricity files
    ├── P1g/                       Raw gas files
    └── smartthings/               Raw device message files
```

---

## Setup

```bash
git clone https://github.com/SnoozeJournalZzz/SmartHomeEnergyAnalysis.git
cd SmartHomeEnergyAnalysis

python3 -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Loading Data

Each tool is idempotent — re-running it skips already-loaded rows.

```bash
python p1e.py          -d sqlite:///myhome.db data/data/P1e/P1e-*.csv.gz data/data/P1e/P1e-*.csv
python p1g.py          -d sqlite:///myhome.db data/data/P1g/P1g-*.csv.gz data/data/P1g/P1g-*.csv
python smartthings.py  -d sqlite:///myhome.db data/data/smartthings/smartthings.*.tsv.gz
python openweathermap.py -d sqlite:///myhome.db --start 2022-01-01 --end 2025-03-31
```

Each tool supports `--dry-run`, `--info`, `-v`, and `--help`.

---

## Analyses

### `report_data_quality.ipynb` ✅
Full data audit across all four sources.
- One P1 meter outage (Jan 29–31 2024, 36.8 h) confirmed by cross-source epoch alignment
- SmartThings starts 7 months after P1 meter — common analysis window: Oct 2022 onward
- 8 devices went offline before dataset end; documented with exact dates and likely causes
- Weather (ERA5) complete and physically plausible; validated against in-situ garden sensor
- Closes with a quality scorecard and justified analytical roadmap

### `report_energy_analysis.ipynb` ✅
**Central question: what drives this household's energy consumption — weather, routine, or occupancy?**

Five-part decomposition over the 29-month aligned window (Oct 2022 – Mar 2025):

1. **Baseline patterns** — electricity and gas time series; daily/weekly heatmaps reveal stable two-peak routine
2. **Weather regression** — ERA5 validated against in-situ garden sensor (r=0.957, RMSE=2.2°C); HDD model explains **80.6%** of daily gas variance (slope: 0.55 m³/day per degree-day below 15.5°C)
3. **Behavioural patterns** — motion sensor heatmaps and door-open profiles confirm weekday departure/return clusters (08:00–09:00 / 17:00–18:00); consistent with electricity consumption peaks
4. **Occupancy detection** — K-means (K=6 by silhouette score) on 5-sensor hourly motion counts; low-activity cluster consumes **0.28 kWh/h** vs high-activity clusters at **0.67 kWh/h** (2.4× difference); DBSCAN confirms occupancy is a continuum rather than a binary switch
5. **Synthesis** — variance decomposition: routine explains 15.4% of hourly electricity variance; adding occupancy state raises this to 23.3% (+7.9 pp); temperature adds a further 1.0 pp; 75.7% remains appliance-level noise not capturable at hourly resolution

---

## Engineering Notes

**UTC epoch storage.** All timestamps converted to UTC integer seconds at ingestion.
Timezone logic (Europe/Amsterdam) is centralised in one utility function.

**Database-layer deduplication.** `INSERT OR IGNORE` on `PRIMARY KEY` / `UNIQUE` constraints.
O(1) per record; no memory-based pre-filtering needed even at 1.7M rows.

**SQLite batch limit.** SQLite caps SQL bound variables at 999. Batch size is computed
dynamically as `⌊999 / n_columns⌋` to stay within the limit for any table width.

**pandas 3.0 compatibility.** The default datetime precision changed from nanoseconds
(`datetime64[ns]`) to microseconds (`datetime64[us]`) in pandas 3.0. Epoch conversion
uses `.dt.tz_localize(None).astype("datetime64[s]").astype("int64")` to produce
correct second-precision integers regardless of the underlying precision.

---

## Skills Demonstrated

| Area | Details |
|------|---------|
| Data engineering | Multi-format ETL (CSV, TSV, JSON, gzip), schema design, deduplication |
| Python | SQLAlchemy ORM, Click CLI, modular architecture, virtual environment |
| Data cleaning | Timezone / DST handling, gap detection, anomaly classification |
| SQL | SQLite, parameterised queries, aggregate statistics |
| Data analysis | pandas, time-series, cross-source alignment |
| Visualisation | Matplotlib, Plotly Dash |
| Statistics | OLS regression, HDD model, residual diagnostics, K-means, DBSCAN, silhouette scoring, variance decomposition |
| Version control | Git, conventional commits |

---

---

---

# 中文说明

## 项目简介

本项目基于荷兰 Nordwijk 一户真实家庭的智能家居数据，数据跨度 32 个月（2022 年 3 月至 2025 年 3 月），由本项目与荷兰统计局（CBS）合作收集。

数据来源包括：P1 智能电表（电力 + 燃气，15 分钟分辨率）、约 40 个 SmartThings 智能家居设备（运动传感器、温度传感器、门磁、智能插座等），以及 Open-Meteo 提供的历史气象数据。

项目完整覆盖数据工程到分析的全流程：多格式原始文件 → ETL 清洗去重 → SQLite 关系型数据库 → 可复现的 Jupyter 分析报告。

---

## 数据规模

| 数据源 | 去重后行数 | 时间跨度 |
|--------|-----------|---------|
| 电力读数 | 106,013 | 2022-03 → 2025-03 |
| 燃气读数 | 106,018 | 2022-03 → 2025-03 |
| 智能设备消息 | 1,725,906 | 2022-10 → 2025-04 |
| 气象数据 | 28,464 | 2022-01 → 2025-04 |

原始文件之间存在大量时间段重叠，ETL 管道自动处理去重——P1 原始解析行中约 52% 为跨文件重复。

---

## 技术栈

- **语言：** Python 3.13
- **数据处理：** pandas 3.0、SQLAlchemy 2.0
- **CLI 工具：** Click
- **可视化：** Matplotlib
- **数据库：** SQLite
- **版本控制：** Git

---

## 分析

### `report_data_quality.ipynb` 数据质量审计

对四个数据源进行全面质量检验，主要发现：

- **P1 计量器断点**：2024 年 1 月 29–31 日，持续约 36.8 小时，经电力与燃气数据的 epoch 交叉比对确认为同一硬件故障
- **数据源对齐问题**：SmartThings 数据比 P1 数据晚启动约 7 个月，跨源分析的公共起点为 2022 年 10 月
- **设备生命周期**：8 台设备在数据集结束前已下线，均有明确日期记录和合理解释（如季节性圣诞树插座、被更换的锅炉）
- **气象数据**：ERA5 再分析数据完整无缺口，已与花园传感器实地比对验证（r=0.957，RMSE=2.2°C）

### `report_energy_analysis.ipynb` 综合能耗分析

**核心问题：这个家庭的能耗，究竟由天气、作息规律还是在家状态驱动？**

五部分递进式分析，分析窗口为 2022 年 10 月至 2025 年 3 月：

1. **基线模式**：电力与燃气时间序列，小时×星期热力图揭示稳定的双峰作息规律
2. **天气回归**：供暖度日（HDD）模型解释每日燃气用量方差的 **80.6%**（斜率：15.5°C 以下每度日增加 0.55 m³/天）
3. **行为模式**：运动传感器与门磁事件分布，确认工作日出门（08:00–09:00）与回家（17:00–18:00）规律
4. **在家状态检测**：5 个运动传感器的小时事件矩阵作为特征，K-means（K=6）聚类；低活跃簇用电 0.28 kWh/小时，高活跃簇达 0.67 kWh/小时（相差 2.4 倍）；DBSCAN 验证在家/不在家是连续谱而非二值开关
5. **综合分解**：作息规律解释小时电力方差 15.4%；叠加在家状态后升至 23.3%（+7.9 pp）；温度再贡献 1.0 pp；剩余 75.7% 为家电级随机性

---

## 核心工程决策（体现代码规范意识）

- **时间戳全部存为 UTC epoch 整数**，时区转换逻辑集中在 ETL 层，分析层不接触任何时区处理
- **去重在数据库层完成**（`INSERT OR IGNORE` + 唯一约束），O(1) 查找，不需要把已有数据加载进内存对比
- **分批写入**解决 SQLite 的 999 变量上限，批次大小动态计算
- **兼容 pandas 3.0**：处理了 `datetime64[us]` 精度变更带来的 epoch 计算错误（使用 `.astype("datetime64[s]").astype("int64")` 替代 `// 10**9`）

---

## 关于 CBS（荷兰统计局）

CBS 不同于传统意义上的政府统计机构——它非常市场化，提供开放数据 API，与商业机构合作，产出的统计数据直接服务于荷兰政府政策，包括《气候协议》（*Klimaatakkoord*）中的能源转型目标。本项目构建的方法论，设计上可推广至荷兰约 800 万装有法定智能电表的独栋住宅，这也是 CBS 能源数据工作的核心场景之一。
