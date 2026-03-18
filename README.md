# Whistler Powder Hunter

English | [中文](#中文)

[DEMO](https://blog.finaltheory.me/whistler/)

<img src="https://raw.githubusercontent.com/FinalTheory/WhistlerPowderHunter/refs/heads/main/demo.jpg" style="zoom:30%;" />

## English

Whistler Powder Hunter is a ski-focused weather decision project that turns raw forecast charts into actionable ski decisions. It comes from the idea that modern multimodal LLMs seem remarkably good at extracting structured meteorological meaning from forecast images.

When given multiple sources, explicit time labels, and a narrow decision scope, they can often synthesize storm evolution, model convergence, and the risk factors that actually matter for skiing in a way that approaches manual expert adjustment rather than generic consumer weather products.

For unstable PNW storm systems, this AI expert system is often more useful than relying on "scam" snowfall websites that compress complex weather dynamics into a few neat but misleading numbers.

Refer to [An In-Depth Guide to Whistler Weather Forecasts](https://blog.finaltheory.me/en/research/whistler-weather-forecast.html) for more background knowledge.

The product is designed around two decision horizons:
- Tactical decisions (0-72h): first chair / first tracks timing, alpine wind-closure risk, snowline/phase impact on snow quality.
- Strategic planning (4-14d): whether a persistent dry regime is likely to break and when.

### Design Approach

System structure:
- Frontend: a lightweight validation UI for grouped forecast images and generated conclusions.
- Backend: a Python workflow that fetches model/sensor data, routes tasks, calls the LLM for analysis, and renders output to `index.html`.

The backend architecture is scheduler + specialist analysis:
- Stage 1 (routing): choose which tasks are worth running from RWDI synopsis and Avalanche Canada forecast images.
- Stage 2 (analysis): request only the minimum required images and produce bilingual conclusions.

Task dimensions:
- [`PATTERN_TASK`](prompt/PATTERN_TASK.txt)
- [`PRECIP_EVENT_TASK`](prompt/PRECIP_EVENT_TASK.txt)
- [`THERMAL_PHASE_TASK`](prompt/THERMAL_PHASE_TASK.txt)
- [`WIND_OPERATION_TASK`](prompt/WIND_OPERATION_TASK.txt)
- [`DECISION_TASK`](prompt/DECISION_TASK.txt)

### Core Principles

- Decision-first output over generic weather commentary.
- Trend-first interpretation over single deterministic numbers.
- Minimum necessary inputs to avoid redundant context.
- Explicit uncertainty and model divergence in every conclusion.

### UI For Fast Validation

The UI is intentionally designed for quick human double-check after LLM output:
- A top summary panel presents the model conclusion in plain language, including status and uncertainty.
- Forecast images of multiple models are grouped by decision dimension (regional precipitation, wind trend, global pattern) so users can verify the conclusion against the exact evidence.
- Frames are labeled with model run and valid time metadata in both UTC and PST, to reduce misread risk and make duplicate or missing inputs obvious.
- Time slider and side-by-side layout let users quickly inspect trend continuity and model divergence without digging through raw files.

The practical goal is simple: let users do a rapid second-pass sanity check.

Human brains do not consume tokens anyway :)

### Forecast Philosophy

- Read trends, not isolated numbers. In coastal mountains, exact values like snowfall totals or freezing-level snapshots can be misleading if detached from system evolution.
- Treat freezing level as a structural signal, not a single truth number. We care more about air-mass movement, trend speed, and persistence than one timestamp value.
- Prioritize physically consistent signals: synoptic pattern, phase trend, wind evolution, and terrain interaction.
- Use model convergence as a practical confidence cue. If timing/track/thermal signals converge across models, decision confidence improves; if they diverge, keep the range wide.
- Keep a strong separation between forecast model output and observed reality. Models are guidance; station observations and actual accumulation are the final truth.
- Focus on Whistler-relevant first chair decisions: tomorrow daytime ski quality, tomorrow-night precipitation behavior, and conditions into daytime of the following day.

Reference context for users:
- For short-term tactical reads, many local skiers follow updates from `@powderpicker`.
- RWDI synopsis (Peak Live) is treated as a useful professional short-term source and should be distinguished from generic tourist-facing forecasts.

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

Whistler Powder Hunter 是一个面向滑雪决策的天气分析项目，目标是把「看天气图」变成「可执行的滑雪决策」。如今的多模态 LLM 非常擅长从气象预报图像中提取结构化语义信息。只要给它多个信息源、清晰的时间标签，以及足够收敛的问题范围，它往往就能把风暴系统演变、模型收敛程度和真正影响滑雪决策的风险因素整合起来，表现得接近人工经验修正，而不是普通消费级天气产品那种“看数字说话”。对于 PNW 这种风暴路径和相态变化都很不稳定的区域，这种AI专家系统比单纯把复杂天气变化压缩成几个整齐数字的“诈骗”网站更有参考价值。

请阅读[惠斯勒天气预报深度指南](https://blog.finaltheory.me/research/whistler-weather-forecast.html)了解更多背景知识。

项目核心关注两类问题：
- 短期战术决策（0-72h）：是否值得顶门、是否可能强风关 alpine、雪线/相态是否恶化雪质。
- 中期规划决策（4-14d）：在持续少雪窗口下，是否存在明确的天气形势转折信号，是否建议规划一趟欧洲/日本旅行。

### 设计思路

系统结构：
- 前端：用于快速校验结论的轻量 Web UI，展示分组气象图与分析结果。
- 后端：Python 流水线，负责抓取模型/传感器数据、任务路由、调用 LLM 分析，并渲染输出到 `index.html`。

后端系统采用任务调度 + 专项分析的思路：
- 第一层（调度）：根据 RWDI 文本和 Avalanche Canada 图像，选择最值得分析的任务。
- 第二层（分析）：按任务请求最小必要图像，输入到LLM，输出中英文气象总结与更新频率建议。

当前任务维度：
- [`PATTERN_TASK`](prompt/PATTERN_TASK.txt)
- [`PRECIP_EVENT_TASK`](prompt/PRECIP_EVENT_TASK.txt)
- [`THERMAL_PHASE_TASK`](prompt/THERMAL_PHASE_TASK.txt)
- [`WIND_OPERATION_TASK`](prompt/WIND_OPERATION_TASK.txt)
- [`DECISION_TASK`](prompt/DECISION_TASK.txt)

### 核心原则

- 决策优先：优先输出对滑雪决策真正有价值的信息，而非泛化天气描述。
- 趋势优先：强调系统演变与时间窗口，不执着单点数值。
- 最小必要信息：按需使用图像与数据，避免冗余输入。
- 显式不确定性：结论必须包含模型分歧与不确定边界。

### 用于快速检验结论的 Web UI

这个 UI 的设计目标是让用户在看完 LLM 结论后，能够快速做一次人工复核：
- 顶部总结区先给出结论、状态和不确定性，先看结论再看证据。
- 多模型图像按决策维度分组（区域降水、风场趋势、全球背景场），方便用户针对性核验。
- 每张图都带 model run 和 valid time（UTC + PST）标签，能快速发现图像重复、缺失或时间戳错位。
- 时间滑块 + 并排布局可以快速检查趋势连续性和多个模型发散程度，而不必肉眼逐个对比。

目标很简单：让结论可被快速二次校验。毕竟人脑不消耗 token :)

### 预报方法论

- 读趋势而不是用数值诈骗。沿海山脉环境下，脱离系统演变去看降雪量、雪线等单点数字，容易误判。
- 雪线是结构信号，不是单一真值。更重要的是冷暖气团推进方向、速度和持续性。
- 优先关注物理上连贯的信号：大尺度背景、相态趋势、风场变化与传感器读数结合。
- 把模型收敛当作实用置信度信号：多模型在时间、路径、温度趋势上趋同，判断才更稳；发散就要扩大区间。
- 明确区分模型与实况：模型是参考，山顶传感器与 snow ruler 实际降雪才是最终事实。
- 聚焦 Whistler 顶门的真实决策窗口：明天白天雪况、明晚降水行为，以及到后天白天为止的条件判断。

用户参考语境：
- 短期战术判断中，很多本地滑雪者会参考 `@powderpicker` 的更新。
- Peak Live 上的 RWDI Synopsis 可作为相对专业的短期信息源，需要与游客导向预报区分看待。

### 项目边界

- 本项目面向站内滑雪决策信息聚合，不提供 backcountry 雪崩行动建议。
- 本项目强调趋势判断与风险识别，不承诺单点定量预报。

### 路线图

- 完成稳定的任务调度层，提升不同天气情境下的任务选择一致性。
- 完善图像预算与按需请求机制，控制上下文成本。
- 建立历史复盘体系，持续校正模型偏差。
- 加入社区实况反馈，提升局地判断质量。
