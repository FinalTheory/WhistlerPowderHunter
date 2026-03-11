# Whistler Powder Hunter

[DEMO](https://blog.finaltheory.me/whistler/)

English | [中文](#中文)

## English

Whistler Powder Hunter is a ski-focused weather decision project that turns raw forecast charts into actionable ski decisions. Refer to [An In-Depth Guide to Whistler Weather Forecasts](https://blog.finaltheory.me/en/research/whistler-weather-forecast.html) for more background knowledge.

The product is designed around two decision horizons:
- Tactical decisions (0-72h): first chair / first tracks timing, alpine wind-closure risk, snowline/phase impact on snow quality.
- Strategic planning (4-14d): whether a persistent dry regime is likely to break and when.

### Design Approach

The intended architecture is scheduler + specialist analysis:
- Stage 1 (routing): choose which tasks are worth running from RWDI synopsis and Avalanche Canada charts.
- Stage 2 (analysis): request only the minimum required images and produce bilingual conclusions.

Task dimensions:
- `PATTERN_TASK`
- `PRECIP_EVENT_TASK`
- `THERMAL_PHASE_TASK`
- `WIND_OPERATION_TASK`

### Core Principles

- Decision-first output over generic weather commentary.
- Trend-first interpretation over single deterministic numbers.
- Minimum necessary inputs to avoid redundant context.
- Explicit uncertainty and model divergence in every conclusion.

### Scope and Boundaries

- This project supports in-bounds ski weather decisions; it does not provide backcountry avalanche travel guidance.
- The system prioritizes trend and risk interpretation rather than deterministic single-point forecasts.

### Roadmap

- Stabilize task routing behavior across different weather regimes.
- Add strict image-budget and on-demand fetch control.
- Build verification loops against observations and outcomes.
- Incorporate community reports to improve local signal quality.

---

## 中文

Whistler Powder Hunter 是一个面向滑雪决策的天气分析项目，目标是把「看天气图」变成「可执行的滑雪决策」。请参考[惠斯勒天气预报深度指南](https://blog.finaltheory.me/research/whistler-weather-forecast.html)了解更多背景知识。

项目核心关注两类问题：
- 短期战术决策（0-72h）：是否值得顶门、是否可能强风关 alpine、雪线/相态是否恶化雪质。
- 中期规划决策（4-14d）：在持续少雪窗口下，是否存在明确的天气形势转折信号。

### 设计思路

系统采用任务调度 + 专项分析的思路：
- 第一层（调度）：根据 RWDI 文本和 Avalanche Canada 图，选择最值得分析的任务。
- 第二层（分析）：按任务请求最小必要图像，输出中英文总结与更新频率建议。

当前任务维度：
- `PATTERN_TASK`
- `PRECIP_EVENT_TASK`
- `THERMAL_PHASE_TASK`
- `WIND_OPERATION_TASK`

### 核心原则

- 决策优先：优先输出对滑雪决策真正有价值的信息，而非泛化天气描述。
- 趋势优先：强调系统演变与时间窗口，不执着单点数值。
- 最小必要信息：按需使用图像与数据，避免冗余输入。
- 显式不确定性：结论必须包含模型分歧与不确定边界。

### 项目边界

- 本项目面向站内滑雪决策信息聚合，不提供 backcountry 雪崩行动建议。
- 本项目强调趋势判断与风险识别，不承诺单点定量预报。

### 路线图

- 完成稳定的任务调度层，提升不同天气情境下的任务选择一致性。
- 完善图像预算与按需请求机制，控制上下文成本。
- 建立历史复盘体系，持续校正模型偏差。
- 加入社区实况反馈，提升局地判断质量。
