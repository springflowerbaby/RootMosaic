# RootMosaic TrainTicket 数据采集代码

这是从实验工作区提取的独立代码包，采用正式数据收集使用的 Kubernetes拓扑流程。英文使用说明见 [README.md](README.md)。

## 包含内容

- 21 个组件的拓扑和工作负载探针配置。
- 55 个场景的故障计划：5 个无故障、18 个单故障、24 个双故障、8 个三故障。
- 故障注入、恢复，以及故障前／故障中／恢复后三阶段的指标、日志、调用链采集。
- 每场景默认 5 次重复的批量收集入口及断点续跑逻辑。
- 样本质量检查、逐根因信号审计和本地样本索引生成。
- TrainTicket、Jaeger、Prometheus、kube-state-metrics 部署配置。
- 6 项不访问集群的离线检查。

## 先查看计划

在本目录打开 Windows PowerShell：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass
$runner = '.\workspace\trainticket\scripts\rebuild-v3-main-mr1-replicates.ps1'
& $runner -PlanOnly
& $runner -Slots S01,D01,T01 -Replicates r1 -PlanOnly
```

第一条命令只为新开的进程设置脚本执行策略，不修改系统级配置。
`-PlanOnly` 不访问集群、不生成采集数据，也不需要 JWT 密钥。

## 实际收集

按 [环境准备](docs/SETUP.md) 配置独立实验集群，再设置该部署的 JWT 签名密钥。
先收集一次无故障基线，确认质量检查通过，再运行故障场景：

```powershell
$env:TRAINTICKET_JWT_SECRET = '<实验部署的签名密钥>'
& $runner -Slots B01 -Replicates r1
& $runner -Slots S02 -Replicates r1
```

默认每阶段 300 秒、轮询间隔参数 2 秒，每次轮询还有顺序请求的实际开销。
不带 Slots 参数会启动全部场景，完整 275 次运行仅三个阶段就需要至少
68.75 小时，另有部署检查、恢复和导出开销。

