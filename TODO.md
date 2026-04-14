# TODO

## 流水线重构方向

- 整体分析流程往“瘦路由 + 按需补证据”收敛。
- 避免在前两轮里一次性塞入全部图像和全部辅助上下文。
- 当前 Responses/Codex 方案应按“客户端自管上下文”理解，而不是依赖服务端长期记忆。

## Router 阶段

- 让 router 阶段更轻。
- 第一轮优先只给：
  - router developer prompt
  - RWDI 文本预报
  - 简短的 lift history 摘要
  - 可选的 sensor 摘要
  - 可选的轻量 data inventory 摘要
- `init_images` 应变成可选项，或者只保留 1-2 张总览图。
- 可以尝试先跑一轮纯文本 router，再决定是否需要图像。

## Task 执行方式

- 不再把所有已选 task 的 prompt 和图像一次性混在同一轮分析里。
- 当前形态：
  - `router -> 所有 task 一起分析 -> 一次性最终输出`
- 目标形态：
  - `router -> task A -> task B -> final synthesis`
- 每个 task 单独执行时，只给：
  - task 自己的 prompt
  - 这个 task 最小必要的图像
  - 与该 task 真正相关的观测数据
- 每个 task 都应该产出一份结构化中间结果，不要只依赖模型会话记忆把这些判断带到最后。
- task 输出要由 Python 侧显式保存，而不是只停留在会话上下文里。
- 这些中间结果后续应作为 final synthesis 的明确输入重新喂给模型。

## Init Images 独立阶段

- 把 `init_images` 从 router 附带输入改成一个独立阶段。
- 目标流程可以收敛成四段：
  - 阶段 1：Router, text only
  - 阶段 2：Init image interpretation
  - 阶段 3：Per-task analysis
  - 阶段 4：Final synthesis
- 这样流程会比“router 时顺手塞图，后面再把所有 task 一次性灌进去”更清楚。

## 四阶段目标流程

- 阶段 1：Router, text only
  - 输入：
    - router developer prompt
    - RWDI 文本
    - 少量必要背景文字
  - 输出：
    - `tasks`
    - `reason`
    - `debug`
    - 可选的 `need_init_images`
  - 目标：
    - 只决定“接下来分析什么”

- 阶段 2：Init image interpretation
  - 输入：
    - 一条专门的 developer prompt，只做全局天气图景建立
    - `init_images`
  - 输出：
    - `synoptic_overview`
    - `timing_overview`
    - `confidence_notes`
    - `debug`
  - 目标：
    - 形成后续 task 共用的背景，不直接负责最终 summary

- 阶段 3：Per-task analysis
  - 输入：
    - task developer prompt
    - 该 task 的最小必要图像
    - 必要时补 sensor / lift / obs
    - 前面 router + init-stage 的结果
  - 输出：
    - 每个 task 一份结构化结果
  - 典型字段可以包括：
    - `task`
    - `status`
    - `summary_en`
    - `summary_zh`
    - `confidence`
    - `key_signals`
    - `risks`
    - `debug`

- 阶段 4：Final synthesis
  - 输入：
    - synthesis developer prompt
    - Python 侧收集到的所有 task outputs
    - 可选的 init-stage output
  - 输出：
    - `summary.en`
    - `summary.zh`
    - `run_again`
    - `debug`
  - 目标：
    - 只做整合，不重新做大规模底层读图

## Python 侧管理中间结果

- 每个 task 的输出必须在 Python 侧持久化成变量。
- 不要只指望它们留在 conversation 里。
- 这样会直接带来：
  - 可调试性
  - 可复盘性
  - 可比较性
  - 可渲染性
- 未来也可以把 UI 扩展成不仅展示 final summary，还展示各 task 的子结论。

## 最终总结阶段

- 把 final summary 独立成最后一轮。
- 前几轮只做 task-specific reasoning。
- 最后一轮只负责：
  - 综合前面 task 输出
  - 生成 `summary.en` 和 `summary.zh`
  - 决定 `run_again`
- final synthesis 默认不要重新喂原始图像。
- 默认只给 task outputs 和 init-stage output。
- 让 final synthesis 专注于整合，而不是重新陷入原始证据洪流。

## Evidence Reading 与 Synthesis 分离

- 把每一轮分成两类：
  - evidence-reading step
  - synthesis step
- task 阶段属于 evidence-reading。
- final 阶段属于 synthesis。
- 这两类工作尽量不要混在同一轮里。

## 证据按需注入

- 全局背景信息不要默认全部注入。
- 不要每次都完整注入：
  - `lift_history`
  - `sensor_data`
  - `data_inventory`
- 更合理的分配方式：
  - `lift_history` 主要服务 `DECISION_TASK`
  - `sensor_data` 主要服务 `THERMAL_PHASE_TASK` 和 `DECISION_TASK`
  - `data_inventory` 只在模型需要知道“还有哪些证据没看”时再给

## 把 need 接入真实控制流

- 让 `need` 从调试字段变成真正的流程控制信号。
- 理想流程：
  - 先基于文本和极少量图像做初判
  - 若模型认为证据不足，则在 `need` 中声明需要什么
  - 系统再决定是否补图并继续分析
- 初版不必全自动，也可以先做成半人工流程：
  - 模型输出缺失证据
  - 写入 debug
  - 人工判断是否补图

## 强化 Router 输出

- Router 输出不应只包含 `tasks`。
- 可以考虑增加：
  - `confidence`
  - `missing_evidence`
  - `why_not_selected`
- 这样第二阶段可以更有针对性，而不是默认对所有选中 task 都重火力分析。

## 图像上下文策略

- 最重要的不是“事后压缩图像”，而是“事前少发图像”。
- 可以把图像分成三类：
  - router-only
  - task-only
  - long-lived evidence
- 不要假设图像能被简单压缩成文本摘要而不丢失气象结构信息。
- 当前 Codex 后端没有可靠的服务端视觉持久记忆，因此图像注入必须更节制。

## 提升缓存友好度

- 让共享前缀尽量稳定。
- 尽量减少早期消息里的动态内容。
- 时间字段继续保持到小时级，不要保留分钟级抖动。
- 重点关注这些会破坏缓存前缀的动态内容：
  - 当前本地时间
  - 实时 RWDI 文本
  - 实时 lift history
  - 实时 sensor data
  - 会变化的 tool schema 或 prompt 内容

## Codex 后端约束

- 当前 Codex 后端应按以下事实处理：
  - `store=false` 是硬约束
  - `previous_response_id` 不可靠
  - `conversation` 目前没有稳定暴露
- 代码里可以保留 `conversation_id`，以便未来后端能力变化时兼容。
- 但当前正确性不能依赖它，真正的上下文来源仍应是本地 `messages`。

## 调试交互体验

- 增加一个可选调试模式：每轮运行结束后，在下一次定时任务开始前，保留一个临时交互对话窗口。
- 这个窗口复用当前天气分析会话，方便人工追问、核验状态、检查上下文是否符合预期。
- 到下一次定时运行时间点后，自动结束这个临时对话窗口。
