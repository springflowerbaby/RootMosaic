"""
SASRec 推荐系统 API 服务（最小部署版）
依赖本目录下的权重、缓存与 vendor/recbole 源码即可运行
"""
import torch
import pickle
import os
import sys
import time
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import uvicorn
import pandas as pd
from dotenv import load_dotenv
from opentelemetry import trace as _otel_trace_api

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

# 确保可以找到打包的 recbole 源码
HERE = Path(__file__).parent.resolve()
VENDOR_PATH = HERE / "vendor"
if str(VENDOR_PATH) not in sys.path:
    sys.path.append(str(VENDOR_PATH))

# ==================== 数据模型 ====================

class RecommendRequest(BaseModel):
    """推荐请求模型"""
    item_sequence: List[str] = Field(
        ..., 
        description="用户历史交互的商品ID序列（Amazon ASIN格式，如 ['B001234567', 'B009876543']）",
        min_items=1,
        max_items=200
    )
    top_k: Optional[int] = Field(
        10, 
        description="返回Top-K个推荐结果",
        ge=1,
        le=100
    )
    exclude_history: Optional[bool] = Field(
        True,
        description="是否排除用户历史中已交互的商品"
    )

class RecommendItem(BaseModel):
    """推荐商品模型"""
    item_id: str = Field(..., description="商品ID（Amazon ASIN）")
    score: float = Field(..., description="推荐得分")
    title: Optional[str] = Field(None, description="商品标题")
    rank: int = Field(..., description="推荐排名")

class RecommendResponse(BaseModel):
    """推荐响应模型"""
    success: bool = Field(..., description="请求是否成功")
    recommendations: List[RecommendItem] = Field(..., description="推荐商品列表")
    inference_time: float = Field(..., description="推理耗时（秒）")
    message: Optional[str] = Field(None, description="额外信息")

class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str
    model_loaded: bool
    dataset_info: Optional[Dict[str, Any]] = None

class SampledScoreRequest(BaseModel):
    """采样评估请求（复现 uni-N 协议）"""
    item_sequence: List[str] = Field(
        ...,
        description="用户历史商品ID序列",
        min_items=1,
        max_items=200
    )
    target_item: str = Field(
        ...,
        description="正样本商品ID（ground-truth label）"
    )
    num_negatives: Optional[int] = Field(
        99,
        description="随机负样本数量（默认99，复现uni100协议）",
        ge=1,
        le=999
    )
    exclude_history: Optional[bool] = Field(
        True,
        description="负样本采样时是否排除历史商品"
    )
    return_candidates: Optional[bool] = Field(
        False,
        description="是否在响应中返回完整候选列表（含ASIN、得分、标题）"
    )

class CandidateItem(BaseModel):
    """候选商品"""
    item_id: str
    score: float
    title: Optional[str] = None
    rank: int
    is_target: bool = False

class SampledScoreResponse(BaseModel):
    """采样评估响应"""
    success: bool
    target_rank: Optional[int] = Field(None, description="正样本在候选集中的排名（1-indexed）")
    target_score: Optional[float] = Field(None, description="正样本得分")
    num_candidates: int = Field(..., description="实际候选数量（正样本+有效负样本）")
    target_valid: bool = Field(..., description="正样本是否在模型词表中")
    effective_history_length: int = Field(0, description="历史中有效（在词表内）的商品数量")
    inference_time: float
    message: Optional[str] = None
    candidates: Optional[List[CandidateItem]] = Field(None, description="按得分降序的候选列表（仅当 return_candidates=true 时返回）")

# ==================== 模型管理器 ====================

class SASRecModelManager:
    """SASRec 模型管理器 - 单例模式"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.config = None
        self.model = None
        self.dataset = None
        self.item_info = {}
        self._initialized = True
    
    def load_model(self):
        """加载模型和数据（使用本地最小资源）"""
        if self.model is not None:
            print("模型已加载，跳过重复加载")
            return

        print("开始加载 SASRec 模型...")
        start_time = time.time()

        cache_file = HERE / os.environ.get('SASREC_CACHE_PATH', 'standard_cache.pkl')
        model_path = HERE / os.environ.get('SASREC_MODEL_PATH', 'SASRec-Feb-24-2026_17-54-22.pth')
        device = 'cuda' if torch.cuda.is_available() else 'cpu'

        if not cache_file.exists():
            raise FileNotFoundError(f"缺少缓存文件: {cache_file}")
        if not model_path.exists():
            raise FileNotFoundError(f"缺少模型权重文件: {model_path}")

        print("从缓存加载模型和数据...")
        with open(str(cache_file), 'rb') as f:
            cache_data = pickle.load(f)

        self.config = cache_data['config']
        self.dataset = cache_data['dataset']

        # 加载模型权重
        from recbole.model.sequential_recommender.sasrec import SASRec
        self.model = SASRec(self.config, self.dataset)

        checkpoint = torch.load(str(model_path), map_location=device)
        self.model.load_state_dict(checkpoint['state_dict'])
        self.model = self.model.to(device)
        # 同步设备属性: pickled config 烤进的 device 可能是 'cuda'(cache 在 GPU 机构建),
        # 而 PyTorch 的 .to() 不会更新 model.device 这个普通实例属性。推理时 :260/:419 用的是
        # self.model.device, 不纠正会让 CPU-only 机器执行 .to('cuda') 而崩。有 CUDA 时 cuda→cuda 无操作。
        self.model.device = torch.device(device)
        self.model.eval()

        # 加载商品信息
        self._load_item_info()

        load_time = time.time() - start_time
        print(f"模型加载完成！耗时: {load_time:.1f} 秒，设备: {device}")
        print(f"数据集信息: 用户数={self.dataset.user_num}, 物品数={self.dataset.item_num}")
    
    def _load_item_info(self):
        """加载商品信息"""
        print("加载商品信息...")
        _shared_item = HERE.parent.parent / 'shared' / 'data' / 'electronics.item'
        item_file = Path(os.environ.get('SASREC_ITEM_FILE', str(_shared_item) if _shared_item.exists() else str(HERE / 'electronics.item')))
        
        if not item_file.exists():
            print("警告: 找不到 .item 文件，将只返回商品ID")
            return
        
        try:
            df = pd.read_csv(str(item_file), sep='\t')
            for _, row in df.iterrows():
                item_token = row['item_id']
                title = row.get('title', f'Product_{item_token}')
                if len(str(title)) > 80:
                    title = str(title)[:77] + '...'
                self.item_info[item_token] = {'title': title}
            print(f"成功加载 {len(self.item_info)} 个商品信息")
        except Exception as e:
            print(f"加载商品信息失败: {e}")
    
    def score_sampled(self, item_sequence: List[str], target_item: str,
                       num_negatives: int = 99, exclude_history: bool = True,
                       return_candidates: bool = False) -> Dict[str, Any]:
        """采样评估：对 [target + num_negatives 随机负样本] 打分，返回 target 的排名。

        复现 RecBole uni-N 采样排序评估协议。
        """
        import random

        if self.model is None:
            raise RuntimeError("模型未加载")

        start_time = time.time()

        # 1. 转换历史序列
        history_ids = []
        for token in item_sequence:
            try:
                history_ids.append(self.dataset.token2id(self.dataset.iid_field, token))
            except Exception:
                pass
        if not history_ids:
            raise ValueError("输入序列中没有有效的商品ID")
        history_ids_trunc = history_ids[-50:]

        # 2. 检查 target_item 是否在词表中
        try:
            target_id = self.dataset.token2id(self.dataset.iid_field, target_item)
            target_valid = True
        except Exception:
            target_valid = False

        if not target_valid:
            return {
                'target_rank': None,
                'target_score': None,
                'num_candidates': 0,
                'target_valid': False,
                'inference_time': time.time() - start_time,
            }

        # 3. 采样负样本（从全部商品中排除 history + target）
        total_items = self.dataset.item_num  # includes padding at 0
        exclude_set = set(history_ids) | {target_id} | {0}  # 0 = padding token

        available = [i for i in range(1, total_items) if i not in exclude_set]
        sample_size = min(num_negatives, len(available))
        neg_ids = random.sample(available, sample_size)

        # 4. 全量推理，只提取候选项的分数
        interaction = {
            'user_id': torch.LongTensor([0]),
            'item_id_list': torch.LongTensor([history_ids_trunc]),
            'item_length': torch.LongTensor([len(history_ids_trunc)])
        }
        for key in interaction:
            interaction[key] = interaction[key].to(self.model.device)

        with torch.no_grad():
            all_scores = self.model.full_sort_predict(interaction)  # shape (1, item_num)
            all_scores = all_scores[0]  # shape (item_num,)

        candidate_ids = [target_id] + neg_ids
        candidate_scores = all_scores[candidate_ids].cpu().tolist()

        # 5. 按分数降序排列，找 target 的排名
        scored = sorted(zip(candidate_ids, candidate_scores), key=lambda x: -x[1])
        target_rank = next((rank for rank, (cid, _) in enumerate(scored, 1) if cid == target_id), None)
        target_score = float(all_scores[target_id].item())

        result = {
            'target_rank': target_rank,
            'target_score': target_score,
            'num_candidates': len(candidate_ids),
            'target_valid': True,
            'effective_history_length': len(history_ids_trunc),
            'inference_time': time.time() - start_time,
        }

        # 可选：返回完整候选列表（含ASIN、得分、标题）
        if return_candidates:
            candidate_list = []
            for rank, (cid, cscore) in enumerate(scored, 1):
                token = self.dataset.id2token(self.dataset.iid_field, cid)
                title = self.item_info.get(token, {}).get('title', None)
                candidate_list.append({
                    'item_id': token,
                    'score': cscore,
                    'title': title,
                    'rank': rank,
                    'is_target': (cid == target_id),
                })
            result['candidates'] = candidate_list

        return result

    def get_test_sequences(self, split: str = "test", max_users: int = 5000) -> List[Dict]:
        """从 RecBole 增强后的 inter_feat 中提取 Leave-One-Out 测试序列。

        RecBole 的 SequentialDataset.data_augmentation() 会将 inter_feat 替换为
        增强后的数据，每行包含:
          - uid_field        : 用户 ID
          - item_id_list     : 历史商品序列（右填充到 max_seq_length）
          - item_length      : 实际历史长度
          - iid_field        : 目标（正样本）商品 ID
          - time_field       : 目标商品的时间戳

        对每个用户，按 time_field 排序后：
          split='test'  → 取最后一条增强序列
          split='valid' → 取倒数第二条增强序列
        """
        import random as _random

        dataset = self.dataset
        uid_field = dataset.uid_field
        iid_field = dataset.iid_field
        inter_feat = dataset.inter_feat

        # item_id_list 和 item_length 字段名
        item_list_field = iid_field + '_list'   # 'item_id_list'
        item_length_field = dataset.item_list_length_field  # 'item_length'

        # 找时间戳字段
        ts_field = getattr(dataset, 'time_field', None)

        uid_array = inter_feat[uid_field].numpy()
        iid_array = inter_feat[iid_field].numpy()          # target items
        hist_tensor = inter_feat[item_list_field]          # (N, max_seq_len)
        len_array = inter_feat[item_length_field].numpy()  # actual history lengths
        ts_array = inter_feat[ts_field].numpy() if ts_field else None

        # 按用户分组，保留行索引
        from collections import defaultdict
        by_user: Dict[int, list] = defaultdict(list)
        for idx in range(len(uid_array)):
            uid = int(uid_array[idx])
            ts = float(ts_array[idx]) if ts_array is not None else float(idx)
            by_user[uid].append((ts, idx))

        # 打乱用户顺序，避免只取前 max_users 个用户产生偏差
        user_list = list(by_user.items())
        _random.shuffle(user_list)

        results = []
        for uid, events in user_list:
            if len(results) >= max_users:
                break
            events.sort(key=lambda x: x[0])  # sort by timestamp

            # 对于 LS split：test=最后一条，valid=倒数第二条
            if split == "test":
                if len(events) < 1:
                    continue
                row_idx = events[-1][1]
            else:  # valid
                if len(events) < 2:
                    continue
                row_idx = events[-2][1]

            # 从增强 inter_feat 中读取历史和标签
            act_len = int(len_array[row_idx])
            if act_len < 1:
                continue
            hist_ids = hist_tensor[row_idx, :act_len].tolist()  # 实际历史 IDs（无填充）
            label_id = int(iid_array[row_idx])

            hist_tokens = [dataset.id2token(iid_field, int(i)) for i in hist_ids]
            label_token = dataset.id2token(iid_field, label_id)
            user_token = dataset.id2token(uid_field, uid)

            results.append({
                'user_id': user_token,
                'history': hist_tokens,
                'label': label_token,
                'split': split,
            })

        return results

    def predict(self, item_sequence: List[str], top_k: int = 10, exclude_history: bool = True) -> Dict[str, Any]:
        """执行推荐预测"""
        if self.model is None:
            raise RuntimeError("模型未加载")
        
        start_time = time.time()
        
        # 将商品token转换为内部ID
        try:
            item_ids = []
            for item_token in item_sequence:
                try:
                    item_id = self.dataset.token2id(self.dataset.iid_field, item_token)
                    item_ids.append(item_id)
                except:
                    # 跳过未知商品
                    continue
            
            if len(item_ids) == 0:
                raise ValueError("输入序列中没有有效的商品ID")
            
            # 取最近50个商品
            item_ids = item_ids[-50:]
            
        except Exception as e:
            raise ValueError(f"商品ID转换失败: {str(e)}")
        
        # 构造推理输入
        interaction = {
            'user_id': torch.LongTensor([0]),  # 新用户，使用虚拟ID
            'item_id_list': torch.LongTensor([item_ids]),
            'item_length': torch.LongTensor([len(item_ids)])
        }
        
        # 移到模型所在设备
        for key in interaction:
            interaction[key] = interaction[key].to(self.model.device)
        
        # 推理
        with torch.no_grad():
            scores = self.model.full_sort_predict(interaction)
            
            # 如果需要排除历史商品
            if exclude_history:
                for item_id in item_ids:
                    scores[0][item_id] = float('-inf')
            
            # 获取Top-K
            top_scores, top_indices = torch.topk(scores, top_k)
            recommended_items = top_indices.cpu().numpy().flatten()
            recommended_scores = top_scores.cpu().numpy().flatten()
        
        inference_time = time.time() - start_time
        
        # 构造返回结果
        recommendations = []
        for rank, (item_id, score) in enumerate(zip(recommended_items, recommended_scores), 1):
            item_id = int(item_id)
            score = float(score)
            
            # 转换为原始token
            try:
                item_token = self.dataset.id2token(self.dataset.iid_field, item_id)
                title = self.item_info.get(item_token, {}).get('title', None)
            except:
                item_token = str(item_id)
                title = None
            
            recommendations.append({
                'item_id': item_token,
                'score': score,
                'title': title,
                'rank': rank
            })
        
        # OTel: 给当前 auto server span(POST /recommend)补业务字段(不新建 span)
        # 整段埋点旁路用 try/except 容错,埋点失败不影响推理结果返回。
        try:
            _span = _otel_trace_api.get_current_span()
            _span.set_attribute("recweb.sasrec.top_k", int(top_k))
            _span.set_attribute("recweb.sasrec.history_len", len(item_ids))
            _span.set_attribute("recweb.sasrec.input_seq_len", len(item_sequence))
            _span.set_attribute("recweb.sasrec.inference_ms", round(inference_time * 1000, 2))
            _span.set_attribute("recweb.sasrec.candidates_count", len(recommendations))
            _span.set_attribute("recweb.sasrec.device", str(self.model.device))

            # OTel metric: 推理耗时 + 候选数(低基数 label device=cpu|cuda; 不放高基数 item_id)
            _device = str(self.model.device).split(":")[0]  # "cuda:0" -> "cuda"
            if _SASREC_INFER_HIST is not None:
                _SASREC_INFER_HIST.record(float(inference_time), {"device": _device})
            if _SASREC_CAND_HIST is not None:
                _SASREC_CAND_HIST.record(int(len(recommendations)), {"device": _device})
        except Exception as _e:
            logger.warning("[otel] predict 埋点失败(忽略): %s", _e)

        return {
            'recommendations': recommendations,
            'inference_time': inference_time
        }

# ==================== FastAPI 应用 ====================

app = FastAPI(
    title="SASRec 推荐系统 API",
    description="基于 SASRec 模型的序列推荐系统，支持基于用户历史行为的商品推荐",
    version="1.0.0"
)

# ==================== OpenTelemetry init ====================
# OTel init —— 拆分守卫(方案 c):
#   - TracerProvider 一进程一次(重复 set_tracer_provider 会 WARNING 且 SDK 忽略
#     第二次调用),用进程级 env var 哨兵 `_SASREC_OTEL_INITED` 守住
#   - Instrumentor 全局 patch(idempotent),每次模块 import 都执行
#     关键:用 `FastAPIInstrumentor().instrument()`(全局 monkey-patch FastAPI 类)
#     而非 `instrument_app(app)`(只装单个 app 实例)。uvicorn 字符串 import 路径
#     "api_server:app" 会让模块加载两次,产生两个不同 FastAPI app 实例,只装第一个
#     会导致最终被服务的 app 不带 OTel 中间件 → Jaeger 收不到 span。全局 patch
#     保证所有当前和未来创建的 FastAPI 实例都自动被 instrument。
#     RequestsInstrumentor / LoggingInstrumentor 同理(都是全局 patch、idempotent),
#     重复调用会抛 AlreadyInstrumentedError,用 try/except 吞掉。
if os.environ.get("OTEL_ENABLED", "true").lower() == "true":
    os.environ.setdefault("OTEL_SERVICE_NAME", "sasrec_api")
    # 兜底 root logger 配置,避免 LoggingInstrumentor.basicConfig 未生效时 logger.info 无输出
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logger = logging.getLogger(__name__)
    # M1 metric interval: validate before the optional OTel init handler.
    import sys as _otel_config_sys
    from pathlib import Path as _OtelConfigPath
    _otel_config_root = str(_OtelConfigPath(__file__).resolve().parents[2])
    if _otel_config_root not in _otel_config_sys.path:
        _otel_config_sys.path.insert(0, _otel_config_root)
    from shared.otel_metric_config import metric_export_interval_millis as _otel_metric_interval
    _otel_metric_export_interval_ms = _otel_metric_interval()
    try:
        from opentelemetry import trace as _otel_trace
        from opentelemetry.sdk.resources import Resource as _OtelResource
        from opentelemetry.sdk.trace import TracerProvider as _OtelTracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor as _OtelBSP
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as _OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor as _FastAPIInstr
        from opentelemetry.instrumentation.requests import RequestsInstrumentor as _RequestsInstr
        from opentelemetry.instrumentation.logging import LoggingInstrumentor as _LoggingInstr

        # TracerProvider 一进程一次 —— 用 env var 哨兵守住
        if os.environ.get("_SASREC_OTEL_INITED") != "1":
            import atexit
            # Resource 提取成局部变量, TracerProvider / MeterProvider 共用(同 service.name)
            _resource = _OtelResource.create({"service.name": os.environ["OTEL_SERVICE_NAME"]})
            _otel_provider = _OtelTracerProvider(resource=_resource)
            _otel_provider.add_span_processor(_OtelBSP(_OTLPSpanExporter()))
            _otel_trace.set_tracer_provider(_otel_provider)
            # 进程退出时 flush BSP 队列,确保紧急退出(sys.exit/SIGTERM)未发送的 span 不丢
            atexit.register(_otel_provider.shutdown)

            # --- MeterProvider(与 TracerProvider 共存, 共用同一 Resource/OTLP endpoint) ---
            from opentelemetry import metrics as _otel_metrics
            from opentelemetry.sdk.metrics import MeterProvider as _OtelMeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader as _OtelPEMR
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as _OTLPMetricExporter
            _meter_reader = _OtelPEMR(_OTLPMetricExporter(), export_interval_millis=_otel_metric_export_interval_ms)
            _meter_provider = _OtelMeterProvider(resource=_resource, metric_readers=[_meter_reader])
            _otel_metrics.set_meter_provider(_meter_provider)
            atexit.register(_meter_provider.shutdown)

            # --- LoggerProvider + LoggingHandler bridge(把 Python logging 桥接到 OTLP->Loki) ---
            # logs SDK 仍 experimental(opentelemetry.sdk._logs 带下划线),整块独立 try/except 容错:
            # 失败只 warning 不中断服务,也不影响已就绪的 Tracer/Meter provider。
            # 与 LoggingInstrumentor 分工: 后者注 trace_id 到 stdout 日志格式(下方保留),
            # LoggingHandler 负责把 log record 导出为 OTLP(SDK 自动带 active span 的 trace_id/span_id)。
            try:
                from opentelemetry import _logs as _otel_logs
                from opentelemetry.sdk._logs import LoggerProvider as _OtelLoggerProvider, LoggingHandler as _OtelLoggingHandler
                from opentelemetry.sdk._logs.export import BatchLogRecordProcessor as _OtelBLRP
                from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter as _OTLPLogExporter
                _logger_provider = _OtelLoggerProvider(resource=_resource)  # 复用同一 _resource
                _logger_provider.add_log_record_processor(_OtelBLRP(_OTLPLogExporter()))  # endpoint 从 .env 读
                _otel_logs.set_logger_provider(_logger_provider)
                atexit.register(_logger_provider.shutdown)
                # 挂到 root logger,所有模块 logging.* 调用都桥接导出
                _otel_log_handler = _OtelLoggingHandler(level=logging.INFO, logger_provider=_logger_provider)
                logging.getLogger().addHandler(_otel_log_handler)
                logger.info("[otel] sasrec_api log bridge installed")
            except Exception as _otel_log_e:
                logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

            os.environ["_SASREC_OTEL_INITED"] = "1"
        else:
            # uvicorn 二次 import: provider 已 set, 取回全局供 instrument 传参
            from opentelemetry import metrics as _otel_metrics
            _meter_provider = _otel_metrics.get_meter_provider()

        # Instrumentor 每次 import 都跑 —— 全局 patch FastAPI 类,确保 uvicorn 二次
        # import 产生的新 app 实例也被装上中间件。重复调用抛 AlreadyInstrumentedError,
        # try/except 吞掉(idempotent 语义)。
        # meter_provider= 显式传给 instrumentor → 自动产 http.server.* RED 指标。
        try:
            _FastAPIInstr().instrument(meter_provider=_meter_provider)
        except Exception:
            pass
        try:
            _RequestsInstr().instrument()
        except Exception:
            pass
        try:
            _LoggingInstr().instrument(set_logging_format=True)
        except Exception:
            pass
        logger.info("[otel] sasrec_api instrumented")
    except Exception as _otel_e:
        logger.warning(f"[otel] init failed (ignored): {_otel_e}")
# ============================================================

# ==================== 业务 metric instrument ====================
# 模块级 meter + instrument, 供 predict() 埋点。OTel 未就绪时降级为 None(record 处判空)。
_SASREC_INFER_HIST = None
_SASREC_CAND_HIST = None
try:
    from opentelemetry import metrics as _otel_metrics_api
    _sasrec_meter = _otel_metrics_api.get_meter(__name__)
    _SASREC_INFER_HIST = _sasrec_meter.create_histogram(
        name="recweb_sasrec_inference_duration_seconds",
        unit="s",
        description="SASRec 单次推理耗时(秒)",
    )
    _SASREC_CAND_HIST = _sasrec_meter.create_histogram(
        name="recweb_sasrec_candidates_count",
        unit="1",
        description="SASRec 单次推理返回候选数",
    )
except Exception as _m_e:
    logging.getLogger(__name__).warning(f"[otel] sasrec metric init failed (ignored): {_m_e}")
# ============================================================

# 全局模型管理器
model_manager = SASRecModelManager()

@app.on_event("startup")
async def startup_event():
    """服务启动时加载模型"""
    print("=" * 60)
    print("SASRec 推荐系统 API 服务启动中...")
    print("=" * 60)
    try:
        model_manager.load_model()
        print("✅ 模型加载成功，服务已就绪！")
    except Exception as e:
        print(f"❌ 模型加载失败: {e}")
        import traceback
        traceback.print_exc()

# ==================== Nacos 注册 (Phase 1) ====================
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
try:
    from shared.nacos_client import register_service as _nacos_register, deregister_service as _nacos_deregister
except Exception as _e:
    print(f"[nacos] shared.nacos_client 导入失败,跳过注册: {_e}")
    _nacos_register = None
    _nacos_deregister = None

_NACOS_SERVICE_NAME = "sasrec_api"
_NACOS_IP = "127.0.0.1"
_NACOS_PORT = 8200

@app.on_event("startup")
async def _nacos_startup():
    if _nacos_register is not None:
        try:
            _nacos_register(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as e:
            print(f"[nacos] 注册调用异常,已忽略: {e}")

@app.on_event("shutdown")
async def _nacos_shutdown():
    if _nacos_deregister is not None:
        try:
            _nacos_deregister(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as e:
            print(f"[nacos] 注销调用异常,已忽略: {e}")

@app.get("/", response_model=Dict[str, str])
async def root():
    """根路径"""
    return {
        "message": "SASRec 推荐系统 API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """健康检查"""
    model_loaded = model_manager.model is not None
    
    dataset_info = None
    if model_loaded:
        dataset_info = {
            "user_num": int(model_manager.dataset.user_num),
            "item_num": int(model_manager.dataset.item_num),
            "interaction_num": len(model_manager.dataset.inter_feat)
        }
    
    return HealthResponse(
        status="healthy" if model_loaded else "model_not_loaded",
        model_loaded=model_loaded,
        dataset_info=dataset_info
    )

@app.post("/recommend", response_model=RecommendResponse)
async def recommend(request: RecommendRequest):
    """
    推荐接口
    
    接收用户历史交互的商品序列，返回Top-K推荐结果
    
    示例请求:
    ```json
    {
        "item_sequence": ["B001234567", "B009876543", "B005555555"],
        "top_k": 10,
        "exclude_history": true
    }
    ```
    """
    if model_manager.model is None:
        raise HTTPException(status_code=503, detail="模型未加载，请稍后重试")
    
    try:
        result = model_manager.predict(
            item_sequence=request.item_sequence,
            top_k=request.top_k,
            exclude_history=request.exclude_history
        )
        
        return RecommendResponse(
            success=True,
            recommendations=[RecommendItem(**item) for item in result['recommendations']],
            inference_time=result['inference_time'],
            message=f"成功生成 {len(result['recommendations'])} 个推荐"
        )
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"推理失败: {str(e)}")

@app.post("/score/sampled", response_model=SampledScoreResponse)
async def score_sampled(request: SampledScoreRequest):
    """
    采样排序评估接口（复现 uni-N 协议）

    对 target_item（正样本）和 num_negatives 个随机负样本打分，
    返回 target_item 在候选集中的排名。

    用于评估模型在 Leave-One-Out 切分下的 Recall@K / NDCG@K（采样模式）。
    """
    if model_manager.model is None:
        raise HTTPException(status_code=503, detail="模型未加载，请稍后重试")

    try:
        result = model_manager.score_sampled(
            item_sequence=request.item_sequence,
            target_item=request.target_item,
            num_negatives=request.num_negatives,
            exclude_history=request.exclude_history,
            return_candidates=request.return_candidates,
        )
        candidates = None
        if result.get('candidates'):
            candidates = [CandidateItem(**c) for c in result['candidates']]
        return SampledScoreResponse(
            success=True,
            target_rank=result['target_rank'],
            target_score=result['target_score'],
            num_candidates=result['num_candidates'],
            target_valid=result['target_valid'],
            effective_history_length=result.get('effective_history_length', 0),
            inference_time=result['inference_time'],
            message=f"target排名: {result['target_rank']} / {result['num_candidates']}"
                    if result['target_valid'] else "target_item不在模型词表中",
            candidates=candidates,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"评估失败: {str(e)}")


@app.get("/dataset/test_sequences")
async def get_test_sequences(
    split: str = Query("test", description="'test' or 'valid'"),
    max_users: int = Query(5000, description="最多返回多少用户的序列"),
):
    """
    从 RecBole 预处理后的数据集导出 Leave-One-Out 切分的测试序列。

    返回的 history/label 均为原始 token 字符串（Amazon ASIN），
    且保证 100% 在模型词表内（已经过 k-core 过滤）。

    推荐在 eval 脚本中用 --test-from-api 调用此接口替代读取原始 inter 文件。
    """
    if model_manager.model is None:
        raise HTTPException(status_code=503, detail="模型未加载，请稍后重试")
    if split not in ("test", "valid"):
        raise HTTPException(status_code=400, detail="split 必须是 'test' 或 'valid'")
    try:
        sequences = model_manager.get_test_sequences(split=split, max_users=max_users)
        return {"success": True, "split": split, "num_sequences": len(sequences), "sequences": sequences}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@app.post("/recommend/batch")
async def recommend_batch(requests: List[RecommendRequest]):
    """
    批量推荐接口
    
    支持一次请求多个用户的推荐
    """
    if model_manager.model is None:
        raise HTTPException(status_code=503, detail="模型未加载，请稍后重试")
    
    results = []
    for req in requests:
        try:
            result = model_manager.predict(
                item_sequence=req.item_sequence,
                top_k=req.top_k,
                exclude_history=req.exclude_history
            )
            results.append({
                "success": True,
                "recommendations": result['recommendations'],
                "inference_time": result['inference_time']
            })
        except Exception as e:
            results.append({
                "success": False,
                "error": str(e)
            })
    
    return {"results": results}

# ==================== 主程序 ====================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SASRec 推荐系统 API 服务")
    parser.add_argument("--host", type=str, default=os.environ.get('SASREC_HOST', '0.0.0.0'), help="服务器地址")
    parser.add_argument("--port", type=int, default=int(os.environ.get('SASREC_PORT', '8200')), help="服务器端口")
    parser.add_argument("--reload", action="store_true", help="开启热重载（开发模式）")
    
    args = parser.parse_args()
    
    print(f"\n启动服务器: http://{args.host}:{args.port}")
    print(f"API 文档: http://{args.host}:{args.port}/docs")
    print(f"健康检查: http://{args.host}:{args.port}/health\n")
    
    uvicorn.run(
        "api_server:app",
        host=args.host,
        port=args.port,
        reload=args.reload
    )
