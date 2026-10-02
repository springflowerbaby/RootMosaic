# 部署、启动与验证 RecShop

这套入口在已有 Kubernetes 上创建一个独立的 RecShop 业务实例。它使用自己的命名空间、MySQL 和模型卷；现有 采集 实验环境仍使用 [采集 恢复入口](COLLECTION-ENVIRONMENT.md)。业务实例的就绪不等于已经具备正式故障采集条件。

## 运行前提

- Python 3.10 环境、`kubectl` 和可访问的 Kubernetes context，以及可运行这些 Linux 容器的工作节点。
- 集群可动态创建持久卷，或具备所选 StorageClass；有足够容量运行 25 个应用及新增数据库。
- 配置中列出的应用镜像已在目标节点可用，或可从指定仓库拉取。工具不会替你推送镜像。
- 已合法取得 SASRec 权重、配套缓存和商品元数据。源码仓库不包含这些大文件，工具不会自动下载。来源及历史校验值见[根 README](../README.md)。
- 为新实例提供独立数据库密码、数据库 root 密码和 Flask 会话密钥（每项至少16字符）。配置文件只记录环境变量名称，不保存秘密值。

本工具不安装操作系统、Docker Desktop、Kubernetes 或 Chaos Mesh。采集的CRI导出器、两秒观测配置和既有环境身份不自动移植到这个业务实例。业务容器和工作节点运行Linux；后续实际采集及故障恢复另要求Windows控制宿主，不应据此把业务容器改为Windows容器。

## 最短使用流程

以下命令从仓库根目录执行；`python` 应指向已经选择的项目环境。

```powershell
.\install_dependencies.ps1 -Python python
Copy-Item deployment.example.json deployment.local.json
```

编辑 `deployment.local.json`：

| 配置 | 需要明确的内容 |
|---|---|
| `context` | 当前允许操作的 Kubernetes context |
| `namespace`、`instance_id` | 新实例的名称与身份；业务部署入口为保护已有环境而拒绝 `recweb-chaos`、`default` 或系统命名空间。采集使用自身环境配置绑定的新namespace，不要求复用受保护的旧名称 |
| `images` | 25 个应用 Deployment 的实际镜像名及版本 |
| `assets` | 本地模型目录、三个文件名、可选预期 SHA256、容量和 StorageClass |
| `database` | 本实例 MySQL 镜像、持久卷容量和 StorageClass |
| `secrets` | 数据库密码、root 密码、Flask 密钥和可选 LLM 密钥对应的环境变量名称 |
| `observability` | 默认关闭业务实例的遥测导出；启用时提供可访问的外部 OTLP 接收端 |
| `timeouts` | 命令、部署就绪和大文件传输的有界等待时间 |
| `resources` | 可选的逐服务 requests/limits 覆盖，按实际模型与商品文件容量配置 |

先离线查看计划：

```powershell
.\deploy_recshop.ps1 -Python python -Config deployment.local.json -Render -Out deployment-plan.json
```

`-Render`（Python入口为 `--render`）不访问集群、不创建资源，也不读取实际密码值。它检查配置结构和资源范围；渲染成功不表示镜像、模型、数据库或集群已经可用。渲染结果是检查计划，不用于绕过入口直接批量部署。

按配置中的环境变量名提供秘密。例如在 PowerShell 中，通过交互提示读入密码并赋给当前进程环境，避免将真实值写进命令历史：

```powershell
$env:RECSHOP_DB_PASSWORD = (Get-Credential -UserName recshop -Message 'New instance database password').GetNetworkCredential().Password
$env:RECSHOP_DB_ROOT_PASSWORD = (Get-Credential -UserName root -Message 'New instance database root password').GetNetworkCredential().Password
$env:RECSHOP_FLASK_SECRET = (Get-Credential -UserName session -Message 'New instance Flask session secret').GetNetworkCredential().Password
```

变量名必须与实际配置一致。可选 LLM 密钥仅在需要相关业务功能时提供；部署和最小检查不会调用付费模型。

首次部署：

```powershell
.\deploy_recshop.ps1 -Python python -Config deployment.local.json
```

日后恢复同一实例或只读检查：

```powershell
python scripts/entrypoints/start_recshop.py --config deployment.local.json
python scripts/entrypoints/check_recshop.py --config deployment.local.json
```

根目录保留 `deploy_recshop.ps1` 主入口，使用 `-Config`、`-Render`、`-Out`，并可通过 `-Python` 指定解释器。业务恢复与检查入口，以及可选Python/CMD包装位于 `scripts/entrypoints/`；其中部署、恢复、检查的 `.py` 和 `.cmd` 仍使用Python风格参数，例如 `scripts\entrypoints\deploy_recshop.cmd --config deployment.local.json --render --out deployment-plan.json`。该目录中的业务 `.ps1` 包装使用PowerShell参数。

实例状态保存在配置文件所在目录的 `.recshop-state/<instance_id>.json`，同目录锁文件防止并发操作。它绑定命名空间与资源UID，必须随该实例保留；不要把它当成可随意复制给另一实例的配置模板，也不要在操作进行中手删锁。

## 首次部署实际执行什么

入口从应用清单白名单生成独立资源，创建本实例的数据库和资产卷，通过资产搬运 Pod 触发存储绑定并导入指定文件，校验后只读挂载给应用。共享模型卷的 SASRec 与推荐 Agent 使用同节点调度约束，仍经过调度器检查资源和节点条件；该节点须有足够内存承载二者，不能只看整个集群的内存总量。不会递归应用 `k8s/services/`，网关及观测辅助组件不自动进入业务部署清单。

独立 MySQL 的新数据卷首次初始化时执行仓库完整 DDL，固定使用 `shopify2` 库名；这与现有实验服务器中的同名数据库是不同实例。入口不连接旧宿主数据库执行 DDL，也不删除旧卷。重复操作核对实例归属、已有资产和密码配置，不自动接管外来资源或隐式轮换数据库密码。

最小合成商品仅用于验证商品读取链路，不是研究数据，也不保证与 SASRec 词表对应。原 DDL 中 `model_metrics` 的历史示例数值同样不是本次部署测得的推荐结果。真实推荐实验仍需与模型对应的业务数据、交互记录及明确评测配置。

## 如何判断成功

工具成功返回“业务环境就绪”（机器输出 `BUSINESS_READY`），并完成当前实现支持的资源、模型、数据库及 catalog 读取检查。任何必需项失败都应返回非零；Pod 为 Running 或接口返回 HTTP 200 不能单独证明全部依赖已就绪。

检查不会提交订单、修改库存、注册用户或执行 LLM 推荐。SASRec 的 `model_loaded` 只证明模型加载完成，不是推荐准确率验证；LLM 服务健康也不证明外部密钥和额度有效。推荐 Agent 的商品标题缓存会在实际调用时加载，健康检查没有覆盖其完整内存峰值；示例资源覆盖只是起点，仍需按资产与负载验证。

网页访问按部署输出的 Service 和端口转发提示进行，默认不把所有服务直接暴露到公网。域服务沿用受控内网假设。

## 镜像准备

所有业务镜像使用仓库根目录作为构建上下文。共享基础镜像需要先构建；它与SASRec镜像都先安装CPU版PyTorch2.5.1，再读取同一根requirements.txt。普通服务镜像也会包含PyTorch，这是统一清单的体积代价。示例：

```powershell
docker build -f docker/services.Dockerfile -t recweb-base:latest .
docker build -f services/catalog_service/Dockerfile -t recweb-catalog:latest .
docker build -f services/sasrec_api/Dockerfile -t recweb-sasrec:latest .
```

其余服务按各自 Dockerfile 构建，并使 `images` 配置与实际标签一致。分享可复现配置时优先使用固定版本或镜像 digest；当前检查比对 Deployment 的镜像引用及配置，不认证运行镜像内容与本地源码逐字一致。本地 Docker 构建完成不等于远程 Kubernetes 节点能访问该镜像；需要根据集群环境加载到节点或使用具备访问权限的镜像仓库。镜像发布是独立操作，本入口不会自动执行。

若继续进行host压力采集，还需独立准备固定的 `recweb-user:latest` carrier；业务 `images` 配置不会替换它。构建与节点加载边界见[采集环境的压力镜像说明](COLLECTION-ENVIRONMENT.md#host-pressure-carrier-image)。

## 验证范围

当前代码分发版本已完成离线配置、模拟接口和输入读回验证。已有环境的READY或恢复记录只覆盖对应环境，不能证明干净机器首次部署或新实例冷启动通过。新集群上的实际调度、模型加载、业务请求与故障采集尚未在此版本上完成实测，需要在目标环境中逐项验证。

| 验证 | 检查内容 | 适用范围 |
|---|---|---|
| 配置和离线渲染 | 配置可读、资源在白名单内、秘密未写入计划 | 部署输入和资源定义 |
| 模拟 Kubernetes 接口与临时目录测试 | 命令流程、失败路径、重复执行、资源归属与跨目录运行 | 本地控制流程；未创建实际工作负载 |
| Kubernetes API 服务端 dry-run | 资源结构与目标集群API兼容性 | API校验；未实际部署资源 |
| 独立真实实例 | 部署、模型/DB就绪、商品读取及再次启动 | 被测环境及实际执行的业务路径 |

首次部署前，按配置核对目标节点的CPU、内存和存储容量，尤其注意SASRec与推荐Agent的同节点资源需求。使用资源充足的独立环境进行验证，并保持已有实验实例的资源隔离。

## 与原入口的关系

- `scripts/entrypoints/start_local_services.py`：本地 Python 业务栈，适合开发；依赖和模型仍需预先准备。
- 根 `deploy_recshop.ps1`：部署独立Kubernetes业务实例；之后用 `scripts/entrypoints/start_recshop.py` / `scripts/entrypoints/check_recshop.py` 恢复或检查同一实例。
- 根 `start_collection_environment.ps1`：检查或恢复已部署、身份绑定的采集环境，不能代替首次准备。
- `scripts/entrypoints/collect_dataset.ps1`：故障采集入口，始终是单独且显式的操作。

任何一条启动路径都不会自动开始新的采集轮次或覆盖已有数据集。
