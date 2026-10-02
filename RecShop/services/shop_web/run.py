import os
from pathlib import Path
from dotenv import load_dotenv

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

from app import create_app

app = create_app()

if __name__ == '__main__':
    host = os.environ.get('SHOPWEB_HOST', '0.0.0.0')
    port = int(os.environ.get('SHOPWEB_PORT', '3000'))
    # FE-05/Z-BE-02: debug 默认 False(env 门控)。debug=True 会启用 Werkzeug 交互式
    # traceback,向客户端暴露源码/栈/SQL/库名,生产/对外不可接受。仅显式
    # SHOPWEB_DEBUG=true 才开启。注意:start_local_services.py 不设此 env(子进程继承 os.environ),
    # 实际取值由根目录 .env 决定(load_dotenv override=False 注入)——故 .env/.env.example
    # 已显式置 SHOPWEB_DEBUG=False,与此处源码默认值一致,保证运行栈 debug=False。
    debug = os.environ.get('SHOPWEB_DEBUG', 'False').lower() == 'true'

    # ---- Nacos 注册 (Phase 1 + Fix) ----
    # 仅在 werkzeug reloader 的子进程或非 debug 模式下注册,
    # 避免父进程注册后被 reloader 替换导致 atexit 注销。
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not debug:
        import sys as _sys
        import atexit as _atexit
        if str(_PROJECT_ROOT) not in _sys.path:
            _sys.path.insert(0, str(_PROJECT_ROOT))
        try:
            from shared.nacos_client import register_service, deregister_service
            _NACOS_SERVICE_NAME = "shop_web"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 3000
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(debug=debug, host=host, port=port)
