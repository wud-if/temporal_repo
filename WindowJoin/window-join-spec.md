# SPEC — window-join

> 按节填写；条目无内容写"无"，不删节。

## 任务

- task-name：window-join
- 日期：2026-11-11（原地刷新：范围与验收口径对齐参考合约`D:\Flink\window-join-spec.md`——六型语义全覆盖、KeyedStateSpec通道、q8摘marker转全量；初版2026-11-09）
- 来源：用户任务描述——基于`plugins/stateful-operator-development/zh/workflow.md`为gluten-flink开发WindowJoin有状态算子，对齐Flink原生语义，集群运行nexmark query 8 & 5与Flink原生对比输出一致通过；2026-11-11用户指令按参考spec/design对原地优化本SPEC

## 可行性评估

- velox C++侧对目标语义的支持度：部分——算子壳、计划节点、serde、planner分派、gluten影子翻译链、双输入运行时桥、双输入watermark合并环路全部既有，断点集中在C++ runtime核心执行（`WindowJoin.cpp` initializeState挂NYI guard、addInput NYI、迟到stub、整批归首窗、仅inner、watermark死代码——`velox/experimental/stateful/WindowJoin.cpp:47-162`）
- Flink到velox的语义匹配：部分匹配·栈内可补偿（A2）——差距逐要点见下表，全部可在栈边界内补齐，无一项依赖velox核心、Flink发行版或社区合入
- 复用评估：可复用——keyed backend握手、HeapListState、NestedLoopJoinProbe、双输入watermark合并基座、TimeWindowUtil、serde三件套、gluten影子翻译、双输入桥。可扩展——窗口namespace、StreamWindowJoinNode通道字段、影子条件翻译补链。需新建——算子自有到期索引、velox4j KeyedStateSpec镜像类、WindowJoinTest。不采用——旧InternalTimerService族、JoinRecordStateViews、stateHandler旧平面
- 影响分层：velox experimental stateful（主要改动）＋gluten-flink planner（影子补链与遗留清理）＋velox4j（KeyedStateSpec镜像＋节点字段）＋gluten-flink runtime（零改动）
- 结论：`feasible with constraints`（A2×B2）

锚点仓前缀约定：flink＝`D:\Flink\repos\flink`，velox＝`D:\Flink\repos\velox`下的`velox/experimental/stateful/`，gluten＝`D:\Flink\repos\gluten\gluten-flink`，velox4j＝`D:\Flink\repos\velox4j`，q5.sql/q8.sql＝gluten仓`ut/src/test/resources/nexmark/`，GFVBot文档＝`D:\Flink\repos\GFVBot`。本地仓快照含keyed state框架PR #79（已合入）；分支基点＝group-agg PR分支终态，见伴随约束1。

语义映射要点（A档证据，每行：要点——Flink基线锚点——velox现状与锚点）：

| #   | 要点                                                                                              | Flink基线锚点                                                                         | velox现状与锚点                                                                                                                                                            |
| --- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | 状态寻址：keyed by剩余equi key，namespace=windowEnd，双侧ListState，fire后清双侧                  | `WindowJoinOperator.java:82-83,154-167,244-258`                                       | 旧骨架走旧stateHandler且NYI——`WindowJoin.cpp:47-58`；typed替代设施既有——`state/HeapListState.h:38-155`（含逐元素checkpoint帧）                                             |
| 2   | 触发与节奏：事件timer注册于windowEnd-1（DST修正），双输入min watermark驱动，fire先于watermark传播 | `SlicingWindowTimerServiceImpl.java:43-46`，`TimeWindowUtil.java:175-183`             | 基类双输入min合并→emitWatermark链路已闭合——`StatefulOperator.cpp:193-207`；旧骨架watermark不达timer（死代码）——`WindowJoin.cpp:160-162`；到期索引为算子内新建（design §5） |
| 3   | key身份：精确join key；空key（纯窗口等值）合法走SINGLETON                                         | `StreamExecWindowJoin.java:155,199-206`                                               | deprecated `partition()`按hash分区id当key碰撞合并，probe()精确路径既有——`KeySelector.h:64,116-119`；planner现用deprecated构造——`StatefulPlanner.cpp:302-309`               |
| 4   | join条件：equi进keying，残余nonEqui配对时求值（q5需`num >= maxn`），filterNulls承载null语义       | `WindowJoinOperator.java:356`，`JoinConditionWithNullFilters.java:28-65`，`q5.sql:38` | 影子仅译equi，nonEqui与filterNulls未过链——gluten `planner StreamExecWindowJoin.java:186-190`                                                                               |
| 5   | join类型：六型同构（inner/semi/anti/left/right/full），输出恒`+I`                                 | `WindowJoinOperatorBuilder.java:108-186`                                              | NestedLoopJoinProbe可复用（probe节点已接线），六型行为算子侧缺位（仅inner）——`WindowJoin.cpp:137-158`                                                                      |
| 6   | 边界行为：迟到drop分侧计数，retraction抛异常，windowEnd==Long.MAX_VALUE永不触发                   | `WindowJoinOperator.java:212-234`，`TimeWindowUtil.java:178-180`                      | 迟到过滤stub直通、rowKind校验无——`WindowJoin.cpp:116-118`                                                                                                                  |
| 7   | 计划通道：KeyedStateSpec挂keyed state框架（契约标准形态）                                         | 契约文档§1"handler子节点＋KeyedStateSpec"                                             | 节点两侧不携带KeyedStateSpec——`StatefulPlanNode.h:362-439`＋velox4j `plan/StreamWindowJoinNode.java:28-58`（实测9字段无spec）；C++ serde已注册——`StatefulPlanNode.cpp:98`  |
| 8   | 路由：双侧按剩余key shuffle，空key SINGLETON并行度继承                                            | `StreamExecWindowJoin.java:204-210`                                                   | 双侧HashPartitionFunctionSpec已在（零改动）——gluten `planner StreamExecWindowJoin.java:204-210`                                                                            |

A档定档依据：第1~8项差距均为栈内补偿（算子内部实现、钩子覆写、通道补链、条件翻译），无上游仓依赖——A2。

设施供给清单（B档证据，每行：设施——处置——锚点）：

| #   | 设施区    | 设施                                                        | 处置                                                      | 锚点                                                                                                                                           |
| --- | --------- | ----------------------------------------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | 状态句柄  | typed keyed backend创建入口＋KeyedStateSpec握手（五步范式） | 复用                                                      | `state/HeapKeyedStateBackend.h:462-480`；范式＝契约文档"分步实现"                                                                              |
| 2   | 状态句柄  | HeapListState<K,N,T>（含snapshot/restore逐元素帧）          | 复用                                                      | `state/HeapListState.h:38-155`                                                                                                                 |
| 3   | 状态句柄  | 窗口namespace（承载windowEnd第二维寻址）                    | 扩展——WindowEndNamespace＋serializer traits特化，插点既有 | `state/Namespace.h:27-57`，`state/NamespaceSerializer.h:44-53`                                                                                 |
| 4   | key解析   | KeySelector probe()精确key路径                              | 换用（弃deprecated partition()）                          | `KeySelector.h:64,116-119`                                                                                                                     |
| 5   | watermark | 双输入min合并（CombinedWatermarkStatus→emitWatermark）      | 复用——算子仅覆写emitWatermark                             | `StatefulOperator.cpp:193-207`，`CombinedWatermarkStatus.h`                                                                                    |
| 6   | 执行件    | NestedLoopJoinProbe（到期窗口配对与条件求值）               | 复用                                                      | `StatefulPlanner.cpp:300`，`WindowJoin.cpp:137-158`现用法                                                                                      |
| 7   | 窗口数学  | TimeWindowUtil（isWindowFired/toEpochMillsForTimer）        | 复用                                                      | `window/TimeWindowUtil.h:41,47`                                                                                                                |
| 8   | 计划节点  | StreamWindowJoinNode两侧＋serde注册＋planner分派            | 改造——增KeyedStateSpec字段（三仓）                        | `StatefulPlanNode.h:362-439`，velox4j `plan/StreamWindowJoinNode.java:28-58`，`StatefulPlanNode.cpp:86`，`StatefulPlanner.cpp:152-155,284-320` |
| 9   | serde     | KeyedStateSpec C++注册既有；velox4j镜像缺位                 | 复用＋镜像新建                                            | `StatefulPlanNode.cpp:98`；镜像缺位（stateful目录实测）                                                                                        |
| 10  | 快照通道  | typed snapshot/restore挂载                                  | 复用——分支基点既有（group-agg迁移交付）；本地快照无挂载点 | 基点核验项；本地举证——`state/HeapKeyedStateBackend.h:263-295`无调用方                                                                          |
| 11  | 触发件    | 到期索引（dueTs→(key,windowEnd)有序映射）                   | 新建——算子内部，不改框架                                  | 替代旧timer service（`EventTimeTimerService.h:36-104`纯内存无快照）                                                                            |

B档定档依据：算子内实现与通道补链均在既有扩展点内，不要求改StatefulTask、StatefulOperator基类或状态后端机制——B2。分支基点前置见伴随约束1。

矩阵定档：A2×B2查结论矩阵得`feasible with constraints`。调节规则1核查：未发现触发升档的任务级硬约束（验收为双端输出一致，不注入故障恢复，无状态规模上限要求）。调节规则2：伴随约束逐条记录如下。

伴随约束（逐条带锚点）：

- 分支基点＝group-agg PR分支终态（velox bigo-sg/velox#86 head＋配套velox4j/gluten分支，名称与hash开工时确定）：KeyedStateSpec通道先例（GroupAggregationNode）、typed五步先例（GroupAggregator）、snapshot/restore挂载、NexmarkTest marker白名单均属该基点——本地仓现状核验＝PR #79框架已含（本节各锚点实测）而group-agg PR未含（`agg/GroupAggregator.cpp:38-46`仍走旧StreamOperatorStateHandler平面），开工切基点后复核锚点
- 旧stateHandler路径已fail loud，typed keyed state迁移是硬前置——`WindowJoin.cpp:47-58`
- key身份必须走probe()精确key路径，deprecated `partition()`碰撞合并与Flink keyed语义冲突——`KeySelector.h:44-48`
- 触发必须由双输入min watermark驱动且记录先于watermark向下游传播（覆写emitWatermark先弹后传）；迟到判据用算子自持的最近合并水位——`StatefulOperator.cpp:193-207`
- 迟到过滤必须按同源公式（toEpochMillsForTimer(windowEnd-1, shiftTimeZone)）判据实现，现状stub直通——flink `TimeWindowUtil.java:175-183`，现状`WindowJoin.cpp:116-118`
- retraction/UPDATE_BEFORE输入必须fail loud，禁止静默丢弃——flink `WindowJoinOperator.java:225-231`＋契约addInput七步第1步
- null-safe等值（filterNulls=false，`IS NOT_DISTINCT_FROM`形态）首批fail loud带TODO——条件表达式能力未就绪（design限制L1）
- 空key（纯窗口等值，q5形状）需机制裁决：KeyedStateSpec非空CHECK vs Flink空key行口径——`KeyedStateSpec.h:47`，`q5.sql:36-38`（design裁决②）
- e2e级checkpoint/savepoint恢复不做——Java侧编排断链（gluten `runtime GlutenTwoInputOperator.java:381-385,392`返回值丢弃/restore传null）；算子级snapshot/restore往返由UT覆盖
- 仅HEAP后端可用，typed创建入口对ROCKSDB声明即fail loud——`shared/zh/docs/stateful-operator-implementation.md`陷阱表
- q5 e2e依赖窗口聚合链路（HOP聚合与窗口上再聚合）基点先行可用——`q5.sql:11-35`
- 集群/构建状态未验证，nexmark提交方式未定，verify阶段确认——`tasks/window-join/TASK_STATE.md:58`
- numPartitions硬编码1024随deprecated partition()构造退出执行路径——gluten `planner StreamExecWindowJoin.java:228`
- LTZ时区偏移未过链（shiftTimeZone默认UTC），window聚合算子同款TODO——`WindowAggregator.h:95`

## 目标与范围

- 目标：把窗口TVF join算子WindowJoin（planner节点`StreamExecWindowJoin`，`TABLE(窗口TVF)`两侧＋ON window_start/window_end等值＋其余条件语法路径）迁到keyed state框架，成为框架首个两输入keyed state有状态算子——六型join语义全覆盖（inner/semi/anti/left/right/full）、输出恒append-only（`+I`）；nexmark q8摘marker转全量验证（q5同口径），全部已通过查询不退化
- 范围内：
  - velox WindowJoin算子重写：双侧ListState、(剩余equi key, windowEnd)二维寻址、双输入min-watermark驱动的到期触发、迟到丢弃分侧计数、六型配对——`WindowJoin.cpp:47-158`（现状NYI骨架）
  - 窗口namespace新增：WindowEndNamespace与`NamespaceSerializerTraits`特化——`state/NamespaceSerializer.h:44-46`
  - KeyedStateSpec过链三仓：C++ serde字段、velox4j镜像类、gluten构造——`StatefulPlanNode.h:362-439`
  - gluten影子翻译补全：nonEquiCondition翻译、filterNulls过链、遗留stateKeySelector清理、numPartitions退出执行路径——gluten `planner StreamExecWindowJoin.java:186-190,228,257-265`
  - C++ KeySelector迁probe()构造——`StatefulPlanner.cpp:302-309`
  - C++单测与gluten-flink ut覆盖正常路径与边界
- 范围外：
  - 常规join（q7的`ON B.price = B1.maxprice`＋BETWEEN非等值段）——走`StreamExecJoin`→`StreamJoin`，另一算子另立工作项，marker容忍保留——`StreamJoin.cpp:45`
  - interval join、temporal join——语义另有算子承载，非裁剪
  - 处理时间窗口join——Flink原生即不支持——gluten遮蔽类同款校验已在（gluten `planner StreamExecWindowJoin.java:144-146`）
  - changelog输入——planner保证子节点INSERT_ONLY；算子侧fail loud双保险属实现细节（design）
  - null-safe等值join key（`IS NOT_DISTINCT_FROM`形态）——分阶段：首批fail loud带TODO，条件表达式能力就绪后解锁（design限制L1）
  - e2e级checkpoint/savepoint恢复——Java侧接线不存在（已裁定边界，design限制L2）
  - RocksDB后端——typed创建入口未实现——`shared/zh/docs/stateful-operator-implementation.md`陷阱表
  - q5的窗口聚合算子段行为（两侧HOP TVF聚合＝`LocalWindowAggregator`＋`WindowAggregator`——`StatefulPlanner.cpp:322-372`；右翼窗口上再聚合＝`GroupAggregator`——同文件:169-171；`GroupWindowAggregator`仅服务legacy GROUP WINDOW FUNCTION语法——flink `StreamExecGroupWindowAggregate.java:79-86`，q5不触达）——基点交付物，仅作回归对象与上游依赖——`shared/zh/docs/nexmark-queries.md` q5行

## 接口规格

- 算子/函数面：C++ `facebook::velox::stateful::WindowJoin`（StatefulOperator继承；serde注册名`StreamWindowJoinNode`两侧一致——`StatefulPlanNode.cpp:86`＋velox4j `ISerializableRegistry.java:216`）。输入＝两侧已含window_start/window_end BIGINT列的append-only行流（`WindowAttachedWindowingStrategy`列索引——gluten `planner StreamExecWindowJoin.java:157-158`）。输出＝NestedLoopJoinNode probe定义的outputType行，恒`+I`；六型由joinType分支承载（`Utils.toVLJoinType`既有——gluten `planner StreamExecWindowJoin.java:214`）
- 配置项：无新增。numPartitions字段随deprecated partition()构造退出执行路径（serde兼容保留，不再作执行参数）
- 跨层契约：planner侧影子`StreamExecWindowJoin#translateToPlanInternal`构造velox4j`StreamWindowJoinNode`（两侧HashPartitionFunctionSpec＋probe＋left/rightWindowEndIndex＋KeyedStateSpec）包进`StatefulPlanNode`经JNI下发——gluten `planner StreamExecWindowJoin.java:177-242`。runtime侧`GlutenTwoInputOperator`双ExternalStream队列桥接RowData，watermark按输入index经`SerialTask#notifyWatermark(ts,index)`转native——gluten `runtime GlutenTwoInputOperator.java:165-260`；native侧`StatefulTask#notifyWatermark`驱动基类合并双输入min后调算子emitWatermark——`StatefulTask.cpp:199-201`＋`StatefulOperator.cpp:193-207`

## 行为规格

- 触发与节奏：行到达即入对应侧状态并登记到期索引（幂等，多行同窗只占一项）；双输入min watermark越过窗口触发时间戳（windowEnd-1经DST修正）时弹射——读两侧、配对发射、清两侧，原子于同一弹射项；fire先于watermark向下游传播（flink `WindowJoinOperator.java:212-234,244-258`）
- 状态访问：双侧独立typed ListState（"left-records"/"right-records"——状态名对齐Flink `WindowJoinOperator.java:82-83`），key＝抽离窗口等值后的剩余equi key（q8为id/seller单列——`q8.sql:27`；q5为空key——`q5.sql:36-38`），namespace＝windowEnd，值＝行粒度blob列表；行序＝入状态序（Flink list add序同构）；句柄创建走`initializeStateBackend`五步——`shared/zh/docs/stateful-operator-implementation.md`
- watermark与timer语义：per-input watermark经基类CombinedWatermarkStatus合并双输入min——`StatefulOperator.cpp:193-207`；算子覆写emitWatermark先弹后传；触发时间戳＝`toEpochMillsForTimer(windowEnd-1, shiftTimeZone)`与迟到判定同源公式（flink `SlicingWindowTimerServiceImpl.java:43-46`＋`TimeWindowUtil.java:175-183`）；windowEnd==Long.MAX_VALUE永不触发
- 输出：恒INSERT StreamRecord（flink `WindowJoinOperator.java:357-359`）。六型行为表：inner＝双侧桶cross product过条件（equi＋残余nonEqui）；semi/anti＝仅左行投影（匹配/未匹配）；left/right＝未匹配侧null padding；full＝双侧padding＋未匹配右行补发在最后。行序不承诺与Flink逐行一致，验收按多重集比对
- 边界行为：迟到行（触发时间戳≤当前合并watermark）丢弃并分侧计数——flink `WindowJoinOperator.java:220-224`；retraction/UPDATE_BEFORE输入fail loud——`:225-231`；null join key（普通等值）条件恒false→inner丢/anti发/outer padding（flink `JoinConditionWithNullFilters.java:28-65`同构短路）；空key（纯窗口等值）合法（SINGLETON分布，并行度继承）；一侧桶空按类型走padding/短路行为表；session（unaligned）窗口按windowEnd配对行为如实复刻不修正（上游既有形态）

## 验收标准

- 标准1：nexmark q8在GFV集群运行至FINISHED且输出与Flink原生一致（append-only多重集100%精确比对，Python oracle读C++事件dump重实现两侧TVF窗口聚合＋窗口join语义＋迟到规则构造期望集，行数多一少一都是FAIL）——SQL基准`/usr/local/nexmark/queries/`原始SQL verbatim，唯一允许变更为输出connector换print
- 标准2：nexmark q5同口径转全量（HOP窗口＋空key＋残余非等值条件`num>=maxn`的真实查询触达；其窗口聚合段由基点交付承载）——`q5.sql`
- 标准3：自定义e2e矩阵wj-e1~wj-e8全部通过（六型×窗口形态×key形态，见验证计划矩阵表）＋并行两档（1基线与4验证key路由；输出按窗口/键多重集比对天然免疫跨subtask交错）
- 标准4：既有查询与单测不回归——velox stateful单测全量＋gluten-flink ut全量＋e2e回归用例库＋全量`velox_stateful` ctest 100%通过＋格式门（基线＝开工时确定的分支基点）；marker状态如实维护（摘WindowJoin相关容忍，q7 StreamJoin容忍保留）
- 标准5：新增WindowJoin C++单测钉住算子单测清单全部行为：双侧多批累加、触发时序（fire先于watermark传播、双输入min驱动一侧停滞不触发）、迟到（同源公式＋分侧计数＋MAX_VALUE特判）、空窗静默、六型×{左空,右空,部分匹配,全匹配}（semi/anti仅左行、full未匹配右补发在最后）、null key（inner丢/anti发/outer padding）、空key配对、多key隔离与规模（千级key×百级窗口）、fail loud（非`+I`输入、null-safe等值首批、proc-time planner抛错）、snapshot/restore往返（restore后已到期窗口立即排发）

## 验证计划

- 单元测试：velox新增WindowJoinTest，fixture照契约第6节（OperatorTestBase＋HashMapStateBackend＋收集型下游＋snapshot→restore→续喂等价断言）；tests/目录现无typed算子测试样板可抄（16个测试文件实测无GroupAggregatorTest）；gluten ut断言proc-time planner抛错路径
- 端到端：集群nexmark q8与q5双端运行（GFV vs Flink原生1.19）输出比对＋自定义矩阵；输入统一参数（nexmark双源、事件数/TPS/无warmup、区间常量依据实测事件时间跨度，开工复测只校准区间不改查询形态）；全部event-time确定性精确比对；判定规则＝Python oracle读C++事件dump按窗口数学＋迟到规则＋join类型重实现期望集与print输出逐行逐字段比对，禁覆盖率/子集宽松判断；oracle实现进验证框架（scripts下按查询拆模块既有机制）；每用例硬性约束＝C++ dump齐全（写明行数）、作业FINISHED、TM日志零异常；证据落`e2e/{sql,data,out,verify}`树并汇总至`e2e/verify/RESULTS.md`（提交方式verify阶段确认——`tasks/window-join/TASK_STATE.md:58`）
- 自定义e2e矩阵（编号wj-e<N>，SQL模板＝q8同构：双侧TVF聚合子查询＋等值＋窗口等值，仅换join类型/key/窗口函数/区间，列名与描述符以q8及验证框架DDL为准）：

| #     | 场景（窗口/join类型/key形态）               | 验证点                             |
| ----- | ------------------------------------------- | ---------------------------------- |
| wj-e1 | TUMBLE 10s，inner，多key多窗口              | 多重集精确比对；跨窗口隔离         |
| wj-e2 | TUMBLE，left outer（构造右窗空侧）          | padding行构造；恒`+I`              |
| wj-e3 | TUMBLE，full outer（双侧均有未匹配）        | 未匹配右补发在最后                 |
| wj-e4 | TUMBLE，semi与anti两查                      | 左行投影；匹配/未匹配判定          |
| wj-e5 | 纯窗口等值（无剩余key），inner              | 空key链路＋窗口内全量配对          |
| wj-e6 | 迟到注入（水位推进后补投早期窗口行），inner | 迟到丢弃、不污染已发射窗口         |
| wj-e7 | key含null行（普通等值），inner              | null短路：丢null-key行             |
| wj-e8 | HOP（或CUMULATE）窗口形态，inner            | 非TUMBLE窗口数学（q8只覆盖TUMBLE） |

- 补充条目：null-safe等值（wj-e7的`IS NOT_DISTINCT_FROM`形态）→UT断言抛错＋登记"L1解锁后补e2e"；changelog输入→无合法e2e上游形态→UT直构断言抛错
- 回归：velox stateful单测全量（StateBackendTest、CheckpointFormatTest、StatefulOperatorCheckpointTest等——`velox/experimental/stateful/tests/`）；gluten-flink ut全量；nexmark全部已通过查询仍FINISHED且结果与归档一致（翻译层改动走共用通道，影响面收宽到全量）；e2e回归库复检q0（流水线活性）、q3（regular join不回归）、q17（窗口聚合不回归）——回归库由本任务verify阶段首次建立
