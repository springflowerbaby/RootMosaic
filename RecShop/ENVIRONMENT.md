# 运行环境与外部资产

## 环境分层

- 本地业务栈：Python3.10、MySQL8、`requirements.txt`，可选Nacos；固定服务地址可使用`NACOS_ENABLED=false`。
- 独立Kubernetes业务部署：Python3.10＋kubectl＋`requirements.txt`、Linux工作节点、StorageClass/PVC、25个应用镜像；见[部署说明](docs/DEPLOYMENT.md)。
- 采集环境：当前实际采集及故障恢复要求Windows控制宿主，业务容器和Kubernetes工作节点运行Linux；在业务层之外增加网关、故障组件及观测设施，并绑定真实环境身份。Linux/macOS控制宿主不属于当前实采支持范围；help、纯离线prepare和数据迁移不因此被禁止。见[实验环境准备](docs/COLLECTION-ENVIRONMENT.md)。
- 根目录只维护`requirements.txt`。本地PyTorch保持平台选择；业务基础镜像和SASRec镜像先安装CPU版2.5.1，再安装同一完整清单。普通服务镜像也包含PyTorch，因此体积增加。
- `install_dependencies.ps1`安装完整清单；它的只读检查仅核Python3.10与三个采集宿主必要包，不是全业务依赖/就绪验证。

端口、命名空间、节点、数据目录与Python解释器应由使用者配置。采集依赖检查和运行入口统一优先从PATH定位Docker/kubectl，再检查受支持的Docker Desktop自带CLI目录（ProgramFiles下的Docker/Docker/resources/bin）；其他安装位置需将实际工具加入当前进程PATH，不承诺自动发现任意目录。工具可定位不等于环境就绪。

本副本不携带任何机器已就绪的证明，也不保证任意集群或CPU/CUDA组合已经验证。

2026-10-02已在Windows的全新Conda环境中完成此清单的安装、`pip check`、关键模块导入和离线工具验证，使用Python3.10.21，pip解析到PyTorch2.14.1+cpu。该结果不涵盖GPU、实际模型权重/缓存加载、数据库连接或业务服务启动；容器镜像仍需单独构建验证。

## 模型与数据来源

推荐侧商品、交互及配套模型资产的已记录来源是McAuley Lab发布的Amazon Reviews2023（Electronics）。官方数据入口：<https://amazon-reviews-2023.github.io/>，HuggingFace标识`McAuley-Lab/Amazon-Reviews-2023`。采用时引用Hou et al., *Bridging Language and Items for Retrieval and Recommendation*, arXiv:2403.03952（2024）。本项目与Amazon、McAuley Lab无关联。

原始数据的获取不等于已生成下列配套缓存和权重。当前源码不包含完整训练/预处理复刻流程，也不提供匿名外部文件托管；请使用合法取得、互相匹配的模型、RecBole缓存、商品ID/标题文件，并按实际来源核对使用与再分发条款。不要把许可证未知写成获得了授权。

| 资产 | 用途 | 配置 |
|---|---|---|
| SASRec权重 `.pth` | 推理模型 | `SASREC_MODEL_PATH`或部署assets.model |
| `standard_cache.pkl` | 模型配置、数据集和token/ID映射 | `SASREC_CACHE_PATH`或assets.cache |
| `electronics.item` | 商品ID和标题 | `SASREC_ITEM_FILE`、`ITEM_FILE_PATH`或assets.items |
| 交互文件 `.inter` | 业务/研究数据导入 | 导入器显式`--inter-file`；不属于启动最小前提 |

已记录的配套版本校验值仅帮助识别该资产版本，不意味着这些文件随仓库分发：

| 文件 | SHA256 |
|---|---|
| SASRec-Feb-24-2026_17-54-22.pth | e1bea0f88df4485c52a5f575c5cf0eb7b2dd6d1b049bafafa7625c2cd1097736 |
| standard_cache.pkl | 20cfb2de8958a5b72586622d5d3b4c8e5dc2b43a1008cd71458b770fc1ecc031 |
| electronics.inter | e8c86f0797c323a7620266e1ebc8fe869ffb8a5d1cf1cc4e2b42a81a35ef7d37 |
| electronics.item | 6dc34e4bccf0a98fe8693ced9d244474bbd8d17b6c11606e1bfc6f3e8d7a0717 |

PyTorch和pickle加载可执行代码，只使用可信来源。随包RecBole的版本声明为1.2.1；许可证见[随包LICENSE](services/sasrec_api/vendor/recbole/LICENSE)。

## 数据库准备

`scripts/build_database.sql`引用完整`scripts/database_schema.sql`，仅在新的专用数据库上由使用者明确执行。DDL不等于有研究数据；部署器合成商品也不保证在SASRec词表中。

`scripts/import_data.py`只在使用者显式调用时导入指定文件；数据库密码来自所指定的环境变量。`scripts/seed_demo_data.sql`只添加小量带明确标识的合成演示记录。两者都不是健康检查，不在采集期间执行；已有研究数据库不因启动而重新灌入或修改。
