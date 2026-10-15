# 航天器故障知识库 (Spacecraft Fault Knowledge Base)

结构化的航天器故障知识条目,统一 `ontology + simulation` JSON 格式
(与 `spacecraft-fault-001.json` 相同 schema),覆盖:

- **通用故障**:姿态控制 / 电源 / 热控 / 测控 / 推进 / 太阳翼 / 星载计算机
- **空间环境故障**:辐射 / 单粒子翻转(SEU)/ 静电充电
- **真实案例**:Mars Climate Orbiter、Hubble FGS、Galileo HGA、
  Ariane 5 Flight 501、Apollo 13、GEO 通信卫星

## 文件

- `spacecraft-fault-001.json` — 通用在轨故障本体(基础 schema 模板)
- `spacecraft-fault-002..012-*.json` — 各故障/案例条目(统一格式)
- `spacecraft-fault-merged.json` — **合并知识库**(全部条目 + 去重本体 +
  故障目录 + 仿真模型 + 场景 + 全局规则/交叉引用)

## 条目格式 (五层)

| 层 | 内容 |
|----|------|
| `provenance` | 来源 / 引用 / 任务元数据 / 可信度 |
| `ontology` | 类 / 实例 / 属性 / 关系(支撑推理) |
| `fault_models` | 故障类型 / 现象 / 原因 / 处置 |
| `simulation` | 状态空间 / 参数化模型 / 场景模板(可跑仿真) |
| `rules` | 告警 / 诊断规则 |

## 索引

| ID | 条目 | 类型 |
|----|------|------|
| 001 | 通用在轨故障本体 | 通用 |
| 002 | Mars Climate Orbiter(1999) | 真实案例 |
| 003 | Hubble FGS 轴承异常 | 真实案例 |
| 004 | Galileo 高增益天线部署失败 | 真实案例 |
| 005 | 推进子系统故障 | 通用 |
| 006 | 太阳翼/供电故障 | 通用 |
| 007 | 辐射 / SEU / 充电 | 空间环境 |
| 008 | Ariane 5 Flight 501 | 真实案例 |
| 009 | Apollo 13 氧气罐爆炸 | 真实案例 |
| 010 | 测控/通信链路故障 | 通用 |
| 011 | 星载计算机故障 | 通用 |
| 012 | GEO 通信卫星业务故障 | 真实案例 |

## 用法建议

- 知识推理:读 `ontology` 的 classes/relations 建本体图
- 故障诊断:按 `fault_models` 的类型/现象/原因/处置查询
- 仿真:用 `simulation.models` 的参数化模型跑遥测曲线
- 告警:用 `rules` 接规则引擎
