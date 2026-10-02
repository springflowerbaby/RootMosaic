"""
Backend API 的服务发现包装层(Phase 2)

把 `shared.nacos_client.get_service_url` 封装成 get_sasrec_api_url,
并把原 app.py 顶层定义的 SASREC_API_URL 作为 fallback 传入。
Nacos 不可达或关闭时,行为与 Phase 1 前完全一致。

- 每次 HTTP 调用前实时查询,不做缓存
- 任何异常都被 nacos_client 内部吞掉,这里只透传返回值
"""

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from shared.nacos_client import get_service_url as _nacos_get_service_url
except Exception:
    _nacos_get_service_url = None


def _fallback_sasrec_api_url() -> str:
    return os.environ.get('SASREC_API_URL', 'http://127.0.0.1:8200')


def get_sasrec_api_url() -> str:
    """获取 SASRec API 的 URL。失败时 fallback 到 env `SASREC_API_URL`。"""
    fb = _fallback_sasrec_api_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("sasrec_api", fallback_url=fb) or fb
