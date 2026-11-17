# DESIGN——window-join

> 按节填写；标注"无"的节保留标题。本方案通过设计评审（pass/fail）并经用户门禁1确认后方可进入实现。
> 排版纪律（评审项，不达标退回重排）：分点优先——每个要点一条bullet、只说一个事实、带源码锚点（`类#方法`＋仓库相对路径），单点不超过两行；不写连续散文段；含多个裁决的结论拆成裁决列表；表格单元格只放事实与锚点，不塞推理过程。
> 内容纪律（评审项）：只写核心方向与要点；agent的搜索、对比、思考过程不属于方案内容。
> 依据纪律（评审项）：每条结论带一句依据（源码出处/裁决日期/先例）；"已存在/零改动"类现状声明必须源码核实。
> 实现契约基准：`docs/gfvbot/shared/stateful-operator-implementation.md`——§3契约对照逐项过，该文档的陷阱表是§8风险的必查清单；引用其节名用原文（后端契约/状态句柄选型/分步实现/陷阱）。
> 不适用惯例：条目不适用写"不适用＋一行理由"，不删行不留空——复用型任务大量命中。

## 1. 任务与输入

| 项 | 内容 |
|---|---|
| 目标算子 | `WindowJoin`（Flink语义对应`StreamExecWindowJoin`——serde注册名两侧一致为`StreamWindowJoinNode`） |
| 公式 | 新WindowJoin＝`GroupAggregator` keyed state骨架（backend握手＋KeyTable/StateMap＋probe() key解析＋snapshot/restore＋批处理生命周期）＋双输入形态＋(剩余join key, windowEnd)二维寻址的双侧行列表状态＋双输入min-watermark驱动的**到期索引**＋到期窗口双侧物化嵌套循环（六型配对，输出恒`+I`） |
| SPEC | `tasks/window-join/architect/SPEC.md`（2026-11-11原地刷新版，feasible with constraints，14条伴随约束） |
| 实现契约 | `docs/gfvbot/shared/stateful-operator-implementation.md` |
| 参照算子 | `GroupAggregator`（typed五步/七步范式见契约文档；分支基点上为其typed迁移先例——本地快照`agg/GroupAggregator.cpp:38-46`仍走旧`StreamOperatorStateHandler`平面，切基点后生效） |
| 参考对 | `D:\Flink\window-join-design.md`（2026-10-27成文的机制分析，本设计结论层依据；锚点已按本地仓复核实测） |
| 前置基点 | 三仓同名分支＝group-agg PR分支终态（velox bigo-sg/velox#86 head＋配套velox4j/gluten分支），hash开工时确定；依赖顺序keyed state框架PR #79（已合入本地）→ group-agg PR → 本工作 |

锚点仓前缀约定沿用SPEC：velox＝`velox/experimental/stateful/`，gluten＝`gluten-flink`仓，flink＝Flink仓，velox4j＝velox4j仓。

## 2. 现状调研

翻译链三段诚实分级——半成品（死代码/硬编码/NYI）逐个file:line举证；"已存在/零改动"声明必须源码核实后才能写：

| 段 | 分级（齐全/半成品/缺位） | 证据（路径:行号） |
|---|---|---|
| gluten遮蔽翻译（`StatefulPlanner#transformStatefulOperators`分派链与ExecNode影子覆盖现状） | 半成品 | 翻译块既有（gluten `planner StreamExecWindowJoin.java:177-242`），双输入桥既有（gluten `runtime GlutenTwoInputOperator.java:165-260`），但条件翻译/元数据/遗留keyed声明三处缺位（下方举证） |
| velox4j bean＋C++ serde（`StatefulPlanNode.h`既有节点类） | 半成品 | 节点两侧字段齐全（`StatefulPlanNode.h:362-439`＋velox4j `plan/StreamWindowJoinNode.java:28-58`），serde注册名两侧一致（`StatefulPlanNode.cpp:86`＋velox4j `ISerializableRegistry.java:216`），但不携带`KeyedStateSpec` |
| C++算子（`experimental/stateful`现状） | 半成品 | 壳与继承既有（`WindowJoin.h:31-32`），planner构造既有（`StatefulPlanner.cpp:284-320`），timer注册windowEnd-1与Flink同值既有（`WindowJoin.cpp:110`），核心执行全NYI（下方举证） |

watermark环路已闭合（零改动，源码核实）：`GlutenTwoInputOperator.processWatermark1/2`→`task.notifyWatermark(ts,0/1)`（gluten `runtime GlutenTwoInputOperator.java:241-258`）→`StatefulTask::notifyWatermark`→算子链`processWatermark(ts,index)`（`StatefulTask.cpp:199-201`）→基类经`CombinedWatermarkStatus`合并双输入min后`emitWatermark(combined)`（`StatefulOperator.cpp:193-207`）。

半成品点逐个举证（分点，按段归属）：

- gluten段：仅翻译equi条件，q5残余非等值条件`num >= maxn`丢失（gluten `planner StreamExecWindowJoin.java:186-190`＋`q5.sql:38`）
- gluten段：`filterNulls`未消费——`Utils.analyzeJoinKeys(leftInputType, leftJoinKey, List.of())`第三参传空表（gluten `planner StreamExecWindowJoin.java:186-190`），null key/null-safe语义缺位
- gluten段：numPartitions硬编码1024（gluten `planner StreamExecWindowJoin.java:228`）
- gluten段：残留Flink keyed声明`setStateKeySelectors`违背契约铁律（同文件:257-265）——gluten生产影子先例`StreamExecGroupAggregate.java`无`setStateKeySelectors`调用
- velox4j/C++ serde段：节点不携带`KeyedStateSpec`——两侧字段清单实测仅sources/两侧PartFuncSpec/probe/outputType/numPartitions/两侧windowEndIndex（`StatefulPlanNode.h:364-384`）
- velox4j段：无`KeyedStateSpec`镜像类（`stateful/`目录实测）；C++ serde已注册（`StatefulPlanNode.cpp:98`）——两侧不对称
- C++ serde段：`exec::HashPartitionFunctionSpec::keyChannels_`私有无访问器（`velox/exec/HashPartitionFunction.h:98-101`）
- C++算子段：`initializeState`走旧stateHandler路径fail loud（`WindowJoin.cpp:47-58`）
- C++算子段：`addInput`为`VELOX_NYI`（`WindowJoin.cpp:64-66`）
- C++算子段：`advance()`拉取驱动＋自认"not equal to Flink's streaming join"（`WindowJoin.cpp:82-95`）
- C++算子段：迟到过滤`filterWindowFiredRows`stub直通（`WindowJoin.cpp:116-118`）
- C++算子段：`partitionWindowData`只读第0行windowEnd、整批归一个窗口（`WindowJoin.cpp:120-130`，TODO"just a example"）
- C++算子段：`join()`仅inner、TODO clear state（`WindowJoin.cpp:137-158`）
- C++算子段：`processWatermarkInternal`定义但全仓grep无调用点，watermark不达timer（`WindowJoin.cpp:160-162`）
- C++算子段：旧`ListState<uint32_t,...>` key＝partition hash（哈希碰撞静默合并；KeySelector deprecated partition()路径——`WindowJoin.h:94-95`＋`StatefulPlanner.cpp:302-309`）
- 测试段：velox侧无任何join算子UT（`experimental/stateful/tests/`目录实测16文件）

分支基点核验清单（本地快照无法核实、开工切group-agg分支基点后必查并回填锚点）：

- 核验①：GroupAggregationNode的keyedStateSpec通道先例（本地`StatefulPlanNode.h:906-947`仍为keySelectorSpec——PartitionFunctionSpec）
- 核验②：GroupAggregator typed五步先例（本地`agg/GroupAggregator.cpp:38-46`仍走旧平面）
- 核验③：typed snapshot/restore挂载通道（本地举证缺失：`HeapKeyedStateBackend<K>::snapshot()`无调用方——`state/HeapKeyedStateBackend.h:263-295`；Java侧编排断链——gluten `runtime GlutenTwoInputOperator.java:381-385,392`返回值丢弃/restore传null）
- 核验④：NexmarkTest marker白名单（本地`NexmarkTest.java`无容忍机制——逐查询全执行）
- 核验⑤：resolveKeys批内key解析范式与行序列化器设施（acc行serde同族；本地全仓无`resolveKeys`符号）

复用处置枚举——每项给处置；不采用的设施也列出附理由，避免后来者重复考察：

| 设施 | 处置（直接复用/改造复用/新建/不采用） | 说明（改造点或否决理由） |
|---|---|---|
| StatefulOperator生命周期钩子（initialize/initializeStateBackend/addInput/advance/close） | 直接复用 | `StatefulOperator.h:51-98`，基类零改动 |
| 基类processWatermark(ts,index)合并watermark＋emitWatermark发射 | 直接复用 | `StatefulOperator.cpp:193-207`——合并语义单一状态源，算子只覆写emitWatermark（§5） |
| CombinedWatermarkStatus | 直接复用 | 基类既有实例承担合并（`StatefulOperator.cpp:194-205`），算子不建副本 |
| keyed backend创建入口`createKeyedStateBackend<K>` | 直接复用 | `state/HeapKeyedStateBackend.h:462-480`；五步范式＝契约文档"分步实现"；ROCKSDB声明即VELOX_UNSUPPORTED |
| getOrCreateListState<N,T> typed句柄API | 直接复用 | `state/HeapKeyedStateBackend.h:220-233` |
| `HeapListState<K,N,T>`（含snapshot/restore逐元素帧） | 直接复用 | `state/HeapListState.h:38-155`——窗口join状态主件，元素TypeSerializer用行序列化器（与聚合acc行serde同族设施，分支基点核验⑤） |
| KeySelector probe()精确key路径＋新式构造 | 直接复用 | `KeySelector.h:64,78-99`——keys()/distinctKeys()/groupRows()消费 |
| KeySelector deprecated partition()构造与partition()方法 | 不采用 | hash分区id当key碰撞合并（`KeySelector.h:116-119`deprecated注释），与Flink keyed语义冲突——SPEC约束3 |
| `NestedLoopJoinNode` probe计划节点与`NestedLoopJoinProbe`执行器 | 直接复用 | 影子构造既有（gluten `planner StreamExecWindowJoin.java:211-218`），joinType已过链（`Utils.toVLJoinType`——:214）；到期配对执行件（§5） |
| gluten影子翻译StreamExecWindowJoin | 改造复用 | KeyedStateSpec构造＋nonEquiCondition并入joinCondition＋filterNulls过链＋移除setStateKeySelectors（gluten `planner StreamExecWindowJoin.java:186-190,228,257-265`） |
| GlutenTwoInputOperator双输入运行时桥 | 直接复用 | 双队列桥＋watermark按index转native＋checkpoint旁路全部既有（gluten `runtime GlutenTwoInputOperator.java:165-260,368-385`） |
| WindowJoin算子壳（继承结构与构造） | 改造复用 | 壳既有（`WindowJoin.h:31-42`），内部全部重写：五步/批处理/迟到过滤/emitWatermark覆写/六型配对输出/快照接线 |
| StreamWindowJoinNode（C++＋velox4j两侧） | 改造复用 | serde链路既有，改造点＝携带KeyedStateSpec（三仓，照GroupAggregationNode先例——分支基点核验①；§10裁决①） |
| StatefulPlanner transformStreamWindowJoinOperator分支 | 改造复用 | 分派既有（`StatefulPlanner.cpp:152-155`），构造段重写：probe() KeySelector×2＋spec经节点读取（同文件:284-320） |
| 窗口namespace（Namespace子类＋NamespaceSerializerTraits特化） | 新建 | 插点既有（`state/Namespace.h:27-57`＋`state/NamespaceSerializer.h:45-53`），新建WindowEndNamespace约15行 |
| 到期索引（dueTs→{(key, windowEnd)}有序映射） | 新建 | 算子内部，不改框架；可从状态条目推导（restore免持久化）——§5 |
| `TimeWindowUtil`窗口数学 | 直接复用 | `window/TimeWindowUtil.h:41,47`——isWindowFired/toEpochMillsForTimer纯函数 |
| EventTimeTimerService/TimerHeapInternalTimer/Triggerable族 | 不采用 | 绑旧stateHandler消费形态、纯内存队列无快照（`EventTimeTimerService.h:36-104`）；到期索引替代——§10裁决④ |
| JoinRecordStateView（join/目录旧辅助） | 不采用 | uint32_t key旧平面接口（`join/JoinRecordStateView.h:23-30`），typed迁移后无角色 |
| StreamOperatorStateHandler旧状态平面 | 不采用 | typed算子不走（契约陷阱表"误用旧状态API"行）；`initializeState`的NYI块与stateHandler()调用删除（`WindowJoin.cpp:47-58`） |

语义差距：Flink的WindowJoinOperator是keyed双侧ListState＋event-time timer驱动的窗口fire六型join（§1.4行为表锚点见§5对齐清单）；velox现状壳在而核心NYI、状态走旧平面、watermark不达timer、翻译层条件/filterNulls/KeyedStateSpec断链——SPEC A档已定档A2栈内补偿。

## 3. 契约对照（逐项过实现契约文档）

### 3.1 状态句柄选型

| 项 | 取值 | 依据（契约文档§状态句柄选型） |
|---|---|---|
| 句柄 | ListState（左侧"left-records"＋右侧"right-records"两个） | "每key追加序列"行＋Flink基线双侧ListState（flink `WindowJoinOperator.java:82-83,154-167`，状态名同款） |
| namespace | 自定义`WindowEndNamespace`（承载windowEnd int64） | "同key多逻辑分区（窗口）用自定义Namespace"行＋Flink基线namespace=windowEnd（flink `WindowListState`泛型Long） |
| 值形态 | 行序列化blob的行粒度列表（每条目vector<行blob>） | ListState行"vector<T>"＋元素经行序列化器编码（与聚合acc行serde同族设施，分支基点核验⑤）；行粒度对齐Flink逐行add（flink `WindowJoinOperator.java:226`） |

行粒度vs批粒度裁决记录（随设计生效的自定项，可推翻）：批粒度存整向量虽有重组开销优势，但元素序列化器缺位且窗口切分易错（旧骨架"整批归首窗"教训——`WindowJoin.cpp:120-130`）；行粒度对齐Flink ListState语义，写入前按windowEnd分窗杜绝错窗。

### 3.2 生命周期范式落位

- `initialize()`建的无状态设施：probe裸指针提取、两侧windowEnd列索引与key标量类型CHECK、迟到drop计数器×2、到期索引空容器、`lastCombinedWatermark_`初始Long.MIN_VALUE——无状态阶段不碰state（契约§分步实现生命周期表）
- `initializeStateBackend()`五步落位：
  - 第1步：`createKeyedStateBackend<RowContainerStateKey>(*stateBackend, *node_->keyedStateSpec(), memoryPool())`——唯一创建入口，maxParallelism与key-group范围由入口内读task参数（`state/HeapKeyedStateBackend.h:462-480`）
  - 第2步：`KeySelector(backend->keyIndex(), channels)`×2——左/右各自probe通道＝各自剩余equi key（空key形状见§10裁决②；`KeySelector.h:64`）
  - 第3步：`ListStateDescriptor<行blob>("left-records", 行序列化器, pool)`＋`getOrCreateListState<WindowEndNamespace>(desc)`×2，右侧同构换右输入类型serializer（`state/HeapKeyedStateBackend.h:220-233`）
  - 第4步：不适用——AggregatingState接线（setAllocator/setOffsets）为Aggregating句柄专属，ListState值是vector非RowContainer行（`state/HeapListState.h:38-63`无allocator语义）
  - 第5步：调`StatefulOperator::initializeStateBackend(stateBackend)`转发下游（`StatefulOperator.h:98`）
- `addInput()`七步落位：七步逻辑落位于`advance()`拉取批处理——双输入算子经拉取驱动（`StatefulOperator.cpp:108-112`对numInputs>1直接advance），WindowJoin计划sources为TableScan占位、`addInput`在生产链路不可达，逐条见§5
- `addInput(StreamElementPtr)`接口处置：覆写为fail loud＋TODO(L6)（§9），理由＝生产链路不可达（上条）
- 铁律对照（非keyed）：Flink侧transformation保持**非keyed**——影子翻译移除`setStateKeySelectors`/`setStateKeyType`（gluten `planner StreamExecWindowJoin.java:257-265`）；gluten生产影子先例`StreamExecGroupAggregate.java`无`setStateKeySelectors`调用，本设计移除两者、比先例更严格
- 铁律对照（typed API）：状态一律走`HeapKeyedStateBackend`typed API，`initializeState`的NYI块与stateHandler()调用删除（`WindowJoin.cpp:47-58`）

### 3.3 计划与分派

- 计划节点：velox节点类`StreamWindowJoinNode`（`StatefulPlanNode.h:362-439`）＋velox4j镜像类（`plan/StreamWindowJoinNode.java:28`）＋serde注册名`StreamWindowJoinNode`（两侧一致——`StatefulPlanNode.cpp:86`＋velox4j `ISerializableRegistry.java:216`）——改造点＝节点加`KeyedStateSpec`字段（三仓，照GroupAggregationNode先例——分支基点核验①；§10裁决①）
- `KeyedStateSpec`：key通道＝剩余equi key（q8＝左[id]/右[seller]——`q8.sql:27`；q5＝空equi key形状见§10裁决②），key类型随列（均BIGINT）；C++ serde已注册（`StatefulPlanNode.cpp:98`），velox4j镜像新建
- planner分派：`transformStreamWindowJoinOperator`分支要点——分支既有（`StatefulPlanner.cpp:152-155`），构造段改为probe() KeySelector×2（绑backend keyIndex＋各自probe通道）＋spec经节点读取（同文件:284-320重写）
- gluten接线：影子覆盖`StreamExecWindowJoin`，operator用`GlutenTwoInputOperator`（既有——gluten `planner StreamExecWindowJoin.java:231-241`，双输入不用GlutenOneInputOperator）

## 4. 受影响模块与文件

| 仓 | 文件 | 改动 |
|---|---|---|
| velox | `velox/experimental/stateful/WindowJoin.h` | 算子声明重写：typed成员（backend/双selector/双侧ListState/到期索引/lastCombinedWatermark_/迟到计数×2）、Triggerable与旧timer成员移除、覆写`emitWatermark`声明 |
| velox | `velox/experimental/stateful/WindowJoin.cpp` | 内部实现重写：删除initializeState NYI块、五步、批处理（rowKind校验/probe/迟到/分窗入状态/索引登记）、emitWatermark先弹后传、六型配对输出、close逆序reset（§5逐条） |
| velox | `velox/experimental/stateful/state/Namespace.h` | 新增`WindowEndNamespace`（equals/hash承载int64 windowEnd，与VoidNamespace并列） |
| velox | `velox/experimental/stateful/state/NamespaceSerializer.h` | `WindowEndNamespaceSerializer`＋`NamespaceSerializerTraits<WindowEndNamespace>`特化 |
| velox | `velox/experimental/stateful/StatefulPlanNode.h`＋`StatefulPlanNode.cpp` | StreamWindowJoinNode加KeyedStateSpec字段＋serialize/create（§10裁决①） |
| velox | `velox/experimental/stateful/StatefulPlanner.cpp` | transformStreamWindowJoinOperator构造段：probe() KeySelector×2＋spec传递（:284-320重写） |
| velox | `velox/experimental/stateful/tests/WindowJoinTest.cpp`（新增）＋`tests/CMakeLists.txt` | §7.1单测矩阵＋注册（add_executable/add_test惯例——`tests/CMakeLists.txt:15-61`先例） |
| velox4j | `plan/StreamWindowJoinNode.java` | 镜像加keyedStateSpec字段＋@JsonGetter（§10裁决①） |
| velox4j | `stateful/KeyedStateSpec.java`（新增）＋`serializable/ISerializableRegistry.java` | KeyedStateSpec镜像类＋NAME_REGISTRY注册（对齐C++注册名——`StatefulPlanNode.cpp:98`） |
| gluten | `planner/.../stream/StreamExecWindowJoin.java` | KeyedStateSpec构造、nonEquiCondition并入joinCondition（:186-190）、filterNulls过链、移除setStateKeySelectors（:257-265） |

零改动声明（源码核实）：

- gluten `runtime GlutenTwoInputOperator.java`零改动——双队列桥、watermark按index 0/1转`notifyWatermark`（:241-258）、processElement1/2、checkpoint旁路钩子全部既有（:165-260,368-385）
- velox4j `query/SerialTask.java`零改动——`notifyWatermark(long,int)` JNI既有
- velox `StatefulOperator.cpp`零改动——双输入min合并与emitWatermark链路既有（:193-207）
- shuffle路由零改动——双侧`HashPartitionFunctionSpec(剩余key)`已在（gluten `planner StreamExecWindowJoin.java:204-210`）；空key时planner给SINGLETON、上游并行度经`leftTransform.getParallelism()`继承（gluten :253），全行落单subtask，语义与Flink等价

## 5. 执行与数据流

- 驱动链（七步的执行上下文）：`GlutenTwoInputOperator.processElement1/2`塞ExternalStream双队列（gluten `runtime GlutenTwoInputOperator.java:165-189`）→drain循环`task.advance()`→`StatefulTask::next`→`advanceWithFuture`→`WindowJoin::advance()`（`StatefulTask.cpp:117-173`＋`StatefulOperator.cpp:108-112`）
- 七步落点（落位于`advance()`拉取批，每侧批＝一次TableScan getOutput）：
  - 第1步 rowKind校验：拉取侧RowVector含`$row_kind`尾列（`FlinkRowToVLVectorConvertor.java:60-64`，C++常量`RowKind.h:32`），UPDATE_BEFORE(1)/DELETE(3)即`VELOX_USER_ERROR` fail loud（契约七步第1步＋flink `WindowJoinOperator.java:225-231`）
  - 第2步 probe：`sideSelector->probe(rowVector)`——左/右各自selector绑同一backend keyIndex（`KeySelector.h:78`），结果消费`keys()`/`distinctKeys()`/`groupRows()`（同文件:83-93）
  - 第3步 迟到过滤＋状态解析：逐行读windowEnd列，`isWindowFired(windowEnd, lastCombinedWatermark_, shiftTimeZone)`即drop＋分侧计数（`window/TimeWindowUtil.h:41`同源公式）；存活行按(distinctKey, windowEnd)分桶——杜绝旧骨架"整批归首窗"（`WindowJoin.cpp:120-130`教训）
  - 第4步 业务写状态：每桶逐行序列化blob→`listState->add(key, ns, blob)`（miss自动建list——`state/HeapListState.h:141-148`）＋到期索引登记`dueTs = toEpochMillsForTimer(windowEnd-1, shiftTimeZone)`→(key, windowEnd)（幂等，多行同窗一项）
  - 第5步 changelog规划：不适用——WindowJoin无updating流，输出恒INSERT（flink `WindowJoinOperator.java:357`），无+I/-U/+U区分
  - 第6步 输出（fire时发生）：emitWatermark覆写内弹dueTs≤wm的项，逐(key, windowEnd)——双侧`get`→各物化RowVector（行blob列式重组，行序＝入状态序）→按join类型配对发射（恒+I）→`pushOutput`→双侧`remove`——发射与清理原子于同一弹射项（flink `WindowJoinOperator.java:244-258`同构）
  - 第7步 批边界：每侧批处理末`keyedBackend_->advance()`回收死key行（`state/HeapKeyedStateBackend.h:125-127`）
- 六型配对机制（§5第6步展开，行为对齐Flink六型行为表——flink `WindowJoinOperator.java`各子类）：
  - 配对与条件求值复用`NestedLoopJoinProbe`（build＝右向量，input＝左向量；条件＝过链后的equi＋nonEqui velox表达式——equi部分在key分组下恒真，语义等价）；joinType已在`NestedLoopJoinNode`（gluten `planner StreamExecWindowJoin.java:214`）
  - 输出行构造按类型：inner/outer＝左右列拼接（outer padding＝null常量列，对齐Flink `GenericRowData`全null——flink `WindowJoinOperator.java:393-398`）；semi/anti＝左行投影（输出schema仅左侧）；full outer＝左扫描＋未匹配右补发（Flink `IdentityHashMap`已发射右行标记用匹配位图等价实现，补发序最后——flink `WindowJoinOperator.java:551-573`）
  - 发射行rowKind恒`+I`（无changelog语义）
- null key语义（普通等值filterNulls=true）：key行anyNull的条目在到期配对时条件恒false——inner/semi丢弃、anti视为未匹配发射、outer走padding（与Flink`JoinConditionWithNullFilters`的anyNull短路同构——flink `JoinConditionWithNullFilters.java:28-65`）；null-safe等值（filterNulls=false）首批fail loud（L1）
- 对齐清单（Flink行为file:line＋本设计处理＋对齐度）：

| # | Flink行为（file:line） | 本设计处理 | 对齐度 |
|---|---|---|---|
| 1 | 触发时间戳＝toEpochMillsForTimer(windowEnd-1, shiftTimeZone)，DST修正 | 同公式（`window/TimeWindowUtil.h:47`） | 精确 |
| 2 | 迟到：isWindowFired→丢＋分侧计数 | 同式同指标（第3步） | 精确 |
| 3 | windowEnd==Long.MAX_VALUE永不触发 | 同特判（flink `TimeWindowUtil.java:178-180`） | 精确 |
| 4 | 输入仅accumulate，回撤抛错 | 同款fail loud断言（第1步；无需撤回内核前置，输入本就append-only） | 精确 |
| 5 | 输出恒+I（含outer null padding） | 全+I | 精确 |
| 6 | 状态＝双ListState，ns=windowEnd，行粒度 | HeapListState×2同构（§3.1） | 精确 |
| 7 | per-element注册timer（service去重） | 到期索引幂等登记（多行同窗一项） | 等价（论证E2） |
| 8 | onEventTime：读两侧→join→清两侧（原子） | 同（同一弹射项内发射＋清理，第6步） | 精确 |
| 9 | 双输入min watermark＋idle语义 | 基类CombinedWatermarkStatus（`StatefulOperator.cpp:193-207`已核实链路） | 精确 |
| 10 | fire先于watermark传播 | emitWatermark覆写先弹后传 | 精确 |
| 11 | 六型行为表（inner/semi/anti/left/right/full） | 六型配对机制（本节展开） | 精确 |
| 12 | full outer：已发射右行标记＋未匹配右补发（序最后） | 匹配位图等价实现，补发序同 | 精确 |
| 13 | semi/anti输出仅左行 | 左行投影同 | 精确 |
| 14 | 窗口等值谓词不进key/条件（结构性保证） | 同（ns=windowEnd承载；两侧windowSpec相同由planner保证——flink `WindowJoinUtil.scala:88-156`剔除后重建） | 精确 |
| 15 | null key（普通等值）：条件anyNull短路→inner丢/outer padding | 同构短路（本节null key语义） | 精确 |
| 16 | null-safe等值（IS_NOT_DISTINCT_FROM） | 首批fail loud（L1） | 分阶段 |
| 17 | 空key合法（SINGLETON，allowEmptyKey） | 空key形状见§10裁决②；上游SINGLETON并行度继承（gluten :253） | 等价（论证E6） |
| 18 | 两侧windowSpec/时间属性不同→WARN回退regular join | planner行为，遮蔽类继承（不属算子边界） | 一致 |
| 19 | session（unaligned）：按windowEnd配对（上游无测试覆盖） | 同构复刻（ns=windowEnd天然同款，不修正） | 精确（含上游既有形态） |
| 20 | proc-time窗口join：planner抛TableException | 遮蔽类同款校验已在（gluten `planner StreamExecWindowJoin.java:144-146`） | 一致 |
| 21 | 条件＝剩余nonEqui（equi由key分组保证） | probe条件＝equi＋nonEqui（equi部分恒真语义等价；nonEqui补链§3.3） | 等价（论证E5） |

- 等价性论证（对齐清单表格外单列，编号对应）：
  - E1（对齐1/6/8）：状态是(key,windowEnd)→行集的append monoid，Flink逐条add与velox批内分组add终态同构；fire在watermark推进时才发生（两侧批均已入桶），输出集合不变——批粒度只影响吞吐不影响结果集
  - E2（对齐7）：Flink的timer注册经(key,namespace,timestamp)三元组去重为集（flink `InternalTimerServiceImpl`堆队列等值语义），到期索引按distinct(key,windowEnd)幂等登记同一dueTs，两集合按桶去重后逐项相等
  - E3（对齐1/2）：GFV链路windowEnd列已是epoch millis且nexmark时间属性非LTZ（nexmark `ddl_gen.sql`——person/auction/bid三处`dateTime` TIMESTAMP(3)，q5/q8窗口TVF经DESCRIPTOR(dateTime)消费），`toEpochMillsForTimer`在UTC分支恒等返回（flink `TimeWindowUtil.java:69-73`），判据退化为windowEnd-1<=watermark，与本设计同公式
  - E4（对齐9/10）：emitWatermark收到的wm即基类CombinedWatermarkStatus合并产物（`StatefulOperator.cpp:193-207`单一状态源），算子无第二份合并副本；`lastCombinedWatermark_`仅为该值的算子侧缓存（迟到判据用）
  - E5（对齐21）：Flink的join条件＝codegen的equi＋nonEqui合取（equi部分在key分组下恒真），本设计probe条件＝equi＋nonEqui的velox表达式——两侧条件在相同行组合上求值结果一致
  - E6（对齐17，若裁决②取方案A）：窗口等值条件是planner抽离的join必要条件（flink `WindowJoinUtil.scala`抽离语义），可join行必同窗口故同window_start，跨window_start行本就不可能join——补偿通道输出集合与空key严格等价，且任何给定windowEnd的窗口start唯一（窗口size固定）
- watermark处理：覆写`emitWatermark(int64_t wm)`——理由：基类processWatermark(ts,index)合并双输入min后调虚emitWatermark（`StatefulOperator.cpp:193-207`），算子在覆写内先弹dueTs≤wm的到期项（fire产生输出pushOutput）再调基类`StatefulOperator::emitWatermark(wm)`传播——记录先于watermark下发，对齐Flink timer先于watermark传播；`lastCombinedWatermark_`随覆写更新作迟到判据数据源
- 覆写体流程：`emitWatermark(wm)`→更新`lastCombinedWatermark_`→弹到期索引dueTs≤wm的项（第6步）→`StatefulOperator::emitWatermark(wm)`
- 批边界动作：`advance()`在两侧拉取处理完各调一次`keyedBackend_->advance()`——批粒度＝每processElement1/2触发的drain一次（单行RowVector——`FlinkRowToVLVectorConvertor.java:60-63`allocateNew(1)），正确性无虞吞吐可后续优化（不在本任务范围）

## 6. checkpoint语义

- 状态条目与schema：两个ListState（"left-records"/"right-records"），key schema＝KeyedStateSpec keyTypes，namespace schema＝WindowEndNamespaceSerializer schemaId（`state/HeapListState.h:121-127`）
- value schema：`list<行serde>`——元素经行序列化器逐元素帧序列化（`state/HeapListState.h:125-127`＋`TypeSerializer.h`同族设施）
- 快照布局：header(maxParallelism/key-group范围/flags)→schema表→按key group升序逐状态条目→CRC32尾（`state/HeapKeyedStateBackend.h:252-295`）
- 挂载通道：分支基点既有（GroupAgg typed迁移交付的typed snapshot/restore挂载——§2基点核验③）；若基点核验发现通道缺失，后备方案＝StatefulSnapshotAdapter（包住NestedLoopJoinProbe转发exec::Operator接口，覆写snapshotState/restoreState绑定backend——velox `exec/Operator.h:237-242`虚接口＋`StatefulOperator.cpp:249`walk调用点），不改基类与状态后端机制，对齐SPEC B2定档
- 到期索引不设独立快照格式：可从状态条目推导，restore逐条遍历（`HeapListState::restoreEntry`）顺带重建索引，已到期项立即排发
- 恢复约束：maxParallelism全生命周期固定（restore校验相等——`state/HeapKeyedStateBackend.h:329-334`），先注册后恢复（`StatefulTask.cpp:221-229`顺序），rescale分片恢复（范围相交即收范围外跳过——同文件:380-395）
- 生产链路边界（诚实声明）：snapshot链路经`GlutenTwoInputOperator.snapshotState`→`task.snapshotState`→walk通（gluten `runtime GlutenTwoInputOperator.java:368-383`），Java侧返回值丢弃、restore传null（§2已举证）——q5/q8验收不注入故障恢复，往返断言在C++单测闭环（§7.1），Java侧断链记限制L2

## 7. 测试策略

### 7.1 单元测试矩阵（参照`GroupAggregatorTest`fixture，摘录见`docs/gfvbot/shared/stateful-operator-implementation.md`）

fixture：`OperatorTestBase`＋`StatefulTask::create`＋`Driver::testingCreate`＋`DriverCtx`（本仓同构先例`StatefulOperatorCheckpointTest.cpp:124-142`），task级backend用`HashMapStateBackend`＋`KeyedStateBackendParameters(HEAP,...)`（契约§6 fixture摘录），下游收集型StatefulOperator。注意tests/目录现无可抄的typed算子测试样板（16个测试文件实测无GroupAggregatorTest）。

| 用例 | 输入 | 期望 | 覆盖意图 |
|---|---|---|---|
| appendOnly | 双侧无$row_kind尾列的行批＋watermark推进 | fire后输出＝预期cross product | 无rowKind列的主路径 |
| 显式INSERT | 双侧$row_kind全0 | 同上 | rowKind校验通过路径 |
| 混合kinds | 混入UPDATE_AFTER(2)批＋单独批混UPDATE_BEFORE(1)/DELETE(3) | UPDATE_AFTER按accumulate接受，UPDATE_BEFORE/DELETE触发VELOX_USER_ERROR | 契约七步第1步fail loud |
| changelog断言 | 正常双侧输入fire | 所有输出StreamRecord appendOnly（无rowKind列＝INSERT） | INSERT-only输出（无updating流） |
| 双侧多批累加 | 跨批入同窗口的行＋watermark推进 | 发射时完整参与配对（结果集＝全批合并配对） | SPEC标准5；论证E1 |
| 触发时序 | 双侧数据＋交替推watermark（一侧停滞） | fire输出先于watermark向下游传播；min驱动一侧停滞不触发 | SPEC标准5；对齐9/10 |
| 迟到丢弃 | windowEnd-1<=lastCombinedWatermark_的行 | 行被drop、对应侧计数递增、状态不含该行；Long.MAX_VALUE窗口永不触发 | SPEC标准5；对齐2/3 |
| 空窗静默 | 单侧无数据窗口fire | inner无输出无崩溃；outer走padding | flink `WindowJoinOperator.java:351-353` |
| 六型×四形态 | inner/semi/anti/left/right/full × {左空,右空,部分匹配,全匹配} | 按六型行为表；semi/anti仅左行；full未匹配右补发在最后 | SPEC标准5；对齐11-13 |
| null key | 普通等值key含null行 | inner丢/anti发/outer padding | SPEC标准5；对齐15 |
| 空key形状 | q5形输入（裁决②机制）＋残余非等值条件 | num<maxn的组合不输出、num>=maxn的组合输出 | SPEC标准5；对齐17；论证E6 |
| 多key隔离与规模 | 千级key×百级窗口 | 各(key,windowEnd)桶互不污染；状态与生命周期承压通过 | SPEC标准5 |
| fail loud | null-safe等值形状构造 | 抛错带TODO(L1) | SPEC标准5；对齐16 |
| snapshot/restore往返 | 喂数→snapshot()→新算子restore()→继续喂数＋推watermark | 双侧ListState状态内容往返后fire输出与不中断连续执行等价；restore后已到期窗口立即排发（索引重建） | SPEC标准5 |
| rescale恢复 | 快照流key-group范围[A,B)，恢复backend范围与之一部分相交 | 相交段条目恢复、范围外跳过不报错 | 分片恢复语义（`state/HeapKeyedStateBackend.h:380-395`） |
| fire后清状态 | 单窗口双侧数据fire后snapshot | 两侧该(key,windowEnd)条目不存在 | SPEC标准5；对齐8 |

gluten ut补充：proc-time窗口join断言planner抛TableException（遮蔽类校验——gluten `planner StreamExecWindowJoin.java:144-146`）。

### 7.2 e2e

| 用例 | SQL/查询 | 输入构造 | 对比基线 |
|---|---|---|---|
| q8单列equi key | nexmark q8（`/usr/local/nexmark/queries/`原始SQL verbatim——TUMBLE 10s双侧＋ON P.id=A.seller＋窗口等值；本地资源文本`ut/src/test/resources/nexmark/q8.sql`，出入以安装目录为准并记录） | 集群nexmark双源（提交方式verify阶段确认——SPEC验证计划） | Python oracle重实现两侧TVF窗口聚合＋窗口join语义＋迟到规则，读C++事件dump构造期望集，多重集100%精确比对（行序不承诺，SPEC行为规格） |
| q5空key＋残余条件 | nexmark q5（同上verbatim——HOP 2s/10s＋空equi key＋num>=maxn；窗口聚合段由基点交付承载） | 同上 | 同上 |
| wj-e1~wj-e8 | 自定义矩阵（SPEC验证计划矩阵表：六型×窗口形态×key形态） | nexmark双源参数化（事件数/TPS/无warmup，区间常量依据实测事件时间跨度） | 同上 |
| 回归 | 全部已通过nexmark查询＋e2e回归库（q0/q3/q17） | 既有 | 结果与归档一致；e2e库由本任务verify阶段首次建立 |

并行两档：1（基线）与4（验证key路由；输出按窗口/键多重集比对天然免疫跨subtask交错）。每用例硬性约束：C++ dump齐全（写明行数）、作业FINISHED、TM日志零异常。

## 8. 风险（契约陷阱表逐项核对，行数与契约表一致）

| 陷阱（契约文档§陷阱） | 是否适用 | 规避落点 |
|---|---|---|
| Flink侧被声明keyed | 适用（现状残留） | 影子翻译移除setStateKeySelectors（gluten `planner StreamExecWindowJoin.java:257-265`），对齐`StreamExecGroupAggregate.java`先例 |
| setAllocator/setOffsets接线时机 | 不适用 | ListState值形态为vector非RowContainer行，无allocator接线需求（`state/HeapListState.h:38-63`） |
| descriptor闭包生命周期 | 不适用 | ListStateDescriptor持serializer值无算子闭包（`state/StateDescriptor.h:153-168`）；close()仍逆序reset句柄/selector/backend |
| 误用旧状态API | 适用（现状即旧路径） | 删除initializeState NYI块与stateHandler()调用，五步typed API（`WindowJoin.cpp:47-58`重写） |
| 初始化即`VELOX_UNSUPPORTED: The RocksDB keyed state backend is not implemented` | 适用 | task后端声明仅HEAP（SPEC约束10）；typed创建入口对ROCKSDB声明即fail loud（`state/HeapKeyedStateBackend.h:476-477`）；范围外处置见L5 |
| maxParallelism漂移 | 适用 | 全生命周期固定——restore校验相等（`state/HeapKeyedStateBackend.h:329-334`） |
| schema漂移/先注册后恢复 | 适用 | initializeStateBackend先于restoreState（`StatefulTask.cpp:221-229`），schema表逐项对照（`state/HeapKeyedStateBackend.h:346-379`） |
| key列含复杂类型 | 适用 | gluten影子翻译对非标量equi key fail loud；q8 id/seller与q5形状通道均BIGINT |
| 死key跨checkpoint | 适用 | 七步第7步批边界advance（§5）＋snapshot前自动回收（`state/HeapKeyedStateBackend.h:263-265`） |

## 9. 限制清单

| 编号 | 限制 | TODO（待解决的能力归属） |
|---|---|---|
| L1 | null-safe等值join key（filterNulls=false，SQL `IS NOT_DISTINCT_FROM`形态）：首批fail loud——TODO（gluten条件生成补null-safe等值表达＋算子侧取消anyNull短路） | 条件表达式能力——后续任务（解锁后补wj-e7 null-safe形态e2e） |
| L2 | Java侧checkpoint编排断链：task.snapshotState返回值丢弃＋restore传null（gluten `runtime GlutenOneInputOperator.java:339,353`、`GlutenTwoInputOperator.java:392`）——e2e级checkpoint/savepoint恢复不做（已裁定边界） | Java侧native records存取——gluten-flink runtime后续任务 |
| L3 | restore广播语义下多typed算子单链不安全：StatefulOperator::restoreState向链上每个算子广播全部records（`StatefulOperator.cpp:271-277`），本算子按单typed算子task假设消费——分支基点通道核验项③ | checkpoint record标记与按算子路由——velox stateful框架后续任务 |
| L4 | TIMESTAMP_LTZ窗口的时区偏移未过链：shiftTimeZone构造参数默认UTC（0），与window聚合算子同款TODO（`WindowAggregator.h:95`）；nexmark均TIMESTAMP(3)非LTZ不触达（论证E3） | LTZ窗口支持——全栈后续任务 |
| L5 | RocksDB后端不可用：typed创建入口对ROCKSDB声明即VELOX_UNSUPPORTED（`state/HeapKeyedStateBackend.h:476-477`） | RocksDB引擎——velox stateful框架后续任务 |
| L6 | addInput(StreamElementPtr)接口fail loud：双输入算子数据经advance()拉取，该接口在生产链路不可达（`StatefulOperator.cpp:108-112`） | 双输入推模式接入——velox stateful框架后续任务 |

实现时fail loud处一律带TODO注释，编号对应本表。

## 10. 待裁决项

裁决①——StreamWindowJoinNode的key声明方案：**已消解为方案A（节点携带KeyedStateSpec，三仓各一处）**。依据：契约文档§1"计划节点＋KeyedStateSpec"标准形态（2026-11-11，参考design D6/§2.5＋本地实测两侧字段清单——`StatefulPlanNode.h:364-384`）；C++ serde已注册（`StatefulPlanNode.cpp:98`）、velox4j镜像新建（§4）；方案B（planner从HashPartitionFunctionSpec推导）被否决——keyChannels_私有无访问器（`velox/exec/HashPartitionFunction.h:98-101`）且推导链隐蔽、空key补偿无法在构造期显式表达。

裁决②——空key（纯窗口等值，q5形状）的key形状策略：**待裁决**。

背景（自含）：q5抽离窗口等值（starttime/endtime）后equi key为空（`q5.sql:36-38`），Flink侧空key时RowDataKeySelector产出空BinaryRowData（全部行同key，SINGLETON分布——flink `StreamExecWindowJoin.java:155`）。本地`KeyedStateSpec`构造CHECK禁止空keyChannels（`KeyedStateSpec.h:47`"A keyed state needs a key channel"）。

| 维度 | 方案A：窗口列补偿通道（keyChannels=[window_start]） | 方案B：空key行（放宽KeyedStateSpec空通道约束） |
|---|---|---|
| 框架改动 | 零（window_start列已在输入） | KeyedStateSpec放宽CHECK＋KeyTable零列路径——改状态后端机制，违反SPEC B2定档 |
| 语义等价性 | 等价可论证（论证E6：可join行必同窗口故同window_start） | 与Flink同构（所有行同key） |
| 实现落点 | 仅gluten影子翻译一处分支（空equi key时取window_start作补偿通道） | state设施核心改动＋三仓serde语义变更 |
| 分支基点耦合 | 无 | 需基点GroupAgg已放宽（基点核验①顺带确认；若未放宽则本方案需改框架） |

建议：方案A。依据：零框架改动对齐B2定档，等价性论证E6成立（窗口等值是planner抽离的join必要条件），参考design对齐清单17的"空keyChannels→空key行"口径依赖基点框架放宽——未证实前方案A为不触框架的落地路径。

裁决③——numPartitions是否参数化：**已消解**。字段随deprecated partition()构造退出执行路径——serde字段保留（两侧兼容），gluten构造值不再作执行参数（probe()构造无numPartitions入参——`KeySelector.h:64`）；无新增配置项（SPEC接口规格）。依据：2026-11-11，参考design §2.5＋`KeySelector.h:64`构造签名。

裁决④——触发与迟到判据的watermark实现位置：**已消解并被参考设计取代**。不采用"算子自建CombinedWatermarkStatus实例＋覆写processWatermark(ts,index)"（初版方案A）；改为"到期索引＋覆写emitWatermark(wm)先弹后传"——`lastCombinedWatermark_`随覆写更新作迟到判据。依据：2026-11-11，基类双输入min合并链路实测已闭合（`StatefulOperator.cpp:193-207`），emitWatermark收到的wm即合并产物（单一状态源，论证E4）；到期索引可从状态推导、restore免持久化（§6），替代绑旧stateHandler、无快照支持的旧timer service（`EventTimeTimerService.h:36-104`）。

各裁决经用户门禁1逐条确认；已裁决项标注日期与结论已同步进契约对照各表依据列，合约级变化回填SPEC并注明日期、偏差记§11。

## 11. 对SPEC的偏差

- 无合约级偏差：KeyedStateSpec通道/六型配对/自定义矩阵/wj-e1~e8均在刷新版SPEC范围内（2026-11-11同步刷新）
- 机制自定项（可推翻，不触合约语义）：到期索引替代旧timer service（§10裁决④）；状态value用行序列化blob的行粒度列表（§3.1裁决记录）；`JoinRecordStateViews`不采用（§2复用表）；输出行组合由算子按join类型构造、probe仅做配对与条件求值（§5六型配对机制）
- 若裁决②经用户门禁确认为方案B（空key行）：key通道表述偏离SPEC行为规格"key＝剩余equi key"的字面口径——届时回填SPEC并注明日期
