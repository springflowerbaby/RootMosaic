"""
RecShop 本地业务栈启动脚本
用法:
    python start_local_services.py              启动全部服务（含 Docker OTel 可观测性栈）
    python start_local_services.py --no-docker  只启 Python 服务，跳过 Docker OTel 栈
    python start_local_services.py --check-only  只读检查配置端点，不启动或停止服务

启动顺序:
    1) Docker OTel 可观测性栈 (otel-collector / jaeger / prometheus / loki / grafana)
       —— 先于 Python 服务起来，避免业务服务早期遥测丢失
    2) 25 个 Python 微服务:sasrec_api(8200) 最先(等模型加载完成),
       backend_api(5000) / recommendation_agent(5001) / llm_rerank_service(5002) /
       review_service(5003) + 各域服务(5004–5022),shop_web(3000) 最后(依赖其余服务)。
       完整清单与启动顺序以下方 SERVICES 为准;服务总览见 services/README.md

shop_web 包含三个 Blueprint: 买家端(/) / 商家端(/merchant) / 管理端(/admin)
按 Ctrl+C 仅停止本次启动的应用进程，保留已存在应用和 Docker/Nacos。
独立 --stop 不再按端口杀进程；启动失败或健康超时会返回非零。
OTel 栈说明: Grafana 映射在宿主 :3001（避开 shop_web 的 3000）。
--no-docker 适用于本机没装 / 不想用 Docker 的场景。
"""

import subprocess
import sys
import os
import time
import signal
import argparse
import socket
import shutil
import urllib.request
import urllib.parse
import json
import re
from pathlib import Path

# Windows GBK 控制台下,emoji (✓ ✅ ❌) print 时会抛 UnicodeEncodeError。
# Python 3.7+ 用 sys.stdout.reconfigure 强制 stdout/stderr 为 UTF-8。
# errors='replace' 兜底:不可表示的字符替换为 ? 而不是崩溃。
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        # Python < 3.7 fallback (本项目用 3.10,实际不会走到这里)
        pass

# 上面的 reconfigure 只修了本脚本自己的 stdout。下面这行是为各子服务进程准备的:
# PYTHONIOENCODING 由 Python 解释器在“启动时”读取,在本进程里改 os.environ 救不了自己,
# 但 subprocess.Popen 启动的子进程是全新解释器、会继承本进程的 os.environ,
# 于是子服务的 stdout/stderr 也变 UTF-8,它们的 emoji 不会在 GBK 控制台崩。
# 写在这里 = 内置进脚本,不必再去 PyCharm Run Config 配环境变量。
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# ==================== 服务定义 ====================

ROOT = Path(__file__).resolve().parents[2]

# Docker OTel 可观测性栈 compose 文件（绝对路径，避免受 cwd 影响）
OTEL_COMPOSE = ROOT / "ops" / "docker-compose.otel.yml"
# Docker Desktop 可执行文件常见安装路径（Windows，Hyper-V backend）
# 注意: 这是 GUI 进程，用于 ensure_docker_running() 里 Popen 拉起界面，
#       不能用它跑 info/compose 子命令——那是下面 DOCKER_CLI 的 docker.exe 的活。
DOCKER_DESKTOP_EXE = Path(r"C:\Program Files\Docker\Docker\Docker Desktop.exe")


def _resolve_docker_cli() -> str:
    """解析 docker CLI 的可执行路径，三段优先级 fallback。

    为什么不直接依赖 PATH: Docker Desktop 默认不把 docker.exe 写进系统 PATH，
    而本脚本常被 PyCharm / 双击 / 计划任务等“干净环境”拉起，裸 "docker" 会
    FileNotFoundError。故先探固定安装路径，再退 shutil.which，最后才裸名兜底。
    """
    # 第一优先: Docker Desktop CLI 固定安装路径（已实测存在）
    for p in (Path(r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"),):
        if p.exists():
            return str(p)
    # 第二优先: 用户把 docker 加进了 PATH / 非默认安装位置
    found = shutil.which("docker")
    if found:
        return found
    # 第三优先(兜底): 保持与 Linux/Mac 及任意 PATH 命中环境的兼容，行为同现状
    return "docker"


# docker CLI 可执行路径（专指 docker.exe，跑 info/compose 子命令用；与 DOCKER_DESKTOP_EXE 各司其职）
DOCKER_CLI = _resolve_docker_cli()
# collector OTLP HTTP 端口（用于探测栈是否就绪）
OTEL_COLLECTOR_PORT = 4318
# Nacos 服务注册中心（本机 standalone 安装；默认在 RecWeb2 同级目录的 nacos/，可用 NACOS_HOME 覆盖）
NACOS_HOME = Path(os.environ.get("NACOS_HOME", str(ROOT.parent / "nacos")))
NACOS_PORT = 8848

SERVICES = [
    {
        "name": "SASRec API",
        "cwd": ROOT / "services" / "sasrec_api",
        "cmd": [sys.executable, "api_server.py"],
        "port": 8200,
        "health": "http://127.0.0.1:8200/health",
        "wait": 15,        # 模型加载较慢，最多等 15 秒
    },
    {
        "name": "Backend API",
        "cwd": ROOT / "services" / "backend_api",
        "cmd": [sys.executable, "app.py"],
        "port": 5000,
        "health": "http://127.0.0.1:5000/health",
        "wait": 3,
    },
    {
        "name": "Recommendation Agent",
        "cwd": ROOT / "services" / "recommendation_agent",
        "cmd": [sys.executable, "app.py"],
        "port": 5001,
        "health": "http://127.0.0.1:5001/recommend/health",
        "wait": 5,
    },
    {
        "name": "LLM Rerank Service",
        "cwd": ROOT / "services" / "llm_rerank_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5002,
        "health": "http://127.0.0.1:5002/health",
        "wait": 3,
    },
    {
        "name": "Review Service",
        "cwd": ROOT / "services" / "review_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5003,
        "health": "http://127.0.0.1:5003/health",
        "wait": 3,
    },
    {
        "name": "Catalog Service",
        "cwd": ROOT / "services" / "catalog_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5005,
        "health": "http://127.0.0.1:5005/health",
        "wait": 3,
    },
    {
        "name": "Cart Service",
        "cwd": ROOT / "services" / "cart_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5006,
        "health": "http://127.0.0.1:5006/health",
        "wait": 3,
    },
    {
        "name": "User Service",
        "cwd": ROOT / "services" / "user_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5004,
        "health": "http://127.0.0.1:5004/health",
        "wait": 3,
    },
    {
        "name": "Address Service",
        "cwd": ROOT / "services" / "address_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5007,
        "health": "http://127.0.0.1:5007/health",
        "wait": 3,
    },
    {
        "name": "AI Memory Service",
        "cwd": ROOT / "services" / "ai_memory_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5008,
        "health": "http://127.0.0.1:5008/health",
        "wait": 3,
    },
    {
        "name": "Announcement Service",
        "cwd": ROOT / "services" / "announcement_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5009,
        "health": "http://127.0.0.1:5009/health",
        "wait": 3,
    },
    {
        "name": "Inventory Service",
        "cwd": ROOT / "services" / "inventory_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5013,
        "health": "http://127.0.0.1:5013/health",
        "wait": 3,
    },
    {
        "name": "Pricing Service",
        "cwd": ROOT / "services" / "pricing_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5014,
        "health": "http://127.0.0.1:5014/health",
        "wait": 3,
    },
    {
        "name": "Promotion Service",
        "cwd": ROOT / "services" / "promotion_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5015,
        "health": "http://127.0.0.1:5015/health",
        "wait": 3,
    },
    {
        "name": "Payment Service",
        "cwd": ROOT / "services" / "payment_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5012,
        "health": "http://127.0.0.1:5012/health",
        "wait": 3,
    },
    {
        "name": "Order Service",
        "cwd": ROOT / "services" / "order_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5010,
        "health": "http://127.0.0.1:5010/health",
        "wait": 3,
    },
    {
        "name": "Checkout Service",
        "cwd": ROOT / "services" / "checkout_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5011,
        "health": "http://127.0.0.1:5011/health",
        "wait": 3,
    },
    {
        "name": "Interaction Service",
        "cwd": ROOT / "services" / "interaction_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5020,
        "health": "http://127.0.0.1:5020/health",
        "wait": 3,
    },
    {
        "name": "Merchant Service",
        "cwd": ROOT / "services" / "merchant_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5019,
        "health": "http://127.0.0.1:5019/health",
        "wait": 3,
    },
    {
        "name": "Admin Audit Service",
        "cwd": ROOT / "services" / "admin_audit_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5022,
        "health": "http://127.0.0.1:5022/health",
        "wait": 3,
    },
    {
        "name": "Notification Service",
        "cwd": ROOT / "services" / "notification_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5021,
        "health": "http://127.0.0.1:5021/health",
        "wait": 3,
    },
    {
        "name": "Review Query Service",
        "cwd": ROOT / "services" / "review_query_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5018,
        "health": "http://127.0.0.1:5018/health",
        "wait": 3,
    },
    {
        "name": "Search Service",
        "cwd": ROOT / "services" / "search_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5017,
        "health": "http://127.0.0.1:5017/health",
        "wait": 3,
    },
    {
        "name": "Shipping Service",
        "cwd": ROOT / "services" / "shipping_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5016,
        "health": "http://127.0.0.1:5016/health",
        "wait": 3,
    },
    {
        "name": "ShopWeb",
        "cwd": ROOT / "services" / "shop_web",
        "cmd": [sys.executable, "run.py"],
        "port": 3000,
        "health": "http://127.0.0.1:3000/health",
        "wait": 3,
    },
]

# ==================== 工具函数 ====================

processes: list[subprocess.Popen] = []
_otel_stack_active = False
_READY_STATES = {"healthy", "ok", "ready", "up"}
_OTEL_SERVICES = {"otel-collector", "jaeger", "prometheus", "loki", "grafana"}
# Host ports come from ops/docker-compose.otel.yml.
# Jaeger exposes its query port, not the admin health port; use its read-only services API.
_OTEL_READY_ENDPOINTS = (
    ("prometheus", "http://127.0.0.1:9090/-/ready"),
    ("loki", "http://127.0.0.1:3100/ready"),
    ("grafana", "http://127.0.0.1:3001/api/health"),
    ("jaeger", "http://127.0.0.1:16686/api/services"),
)


def _color(text: str, code: int) -> str:
    return f"\033[{code}m{text}\033[0m"


def info(msg: str):
    print(_color(f"[INFO]  {msg}", 36))


def ok(msg: str):
    print(_color(f"[  OK]  {msg}", 32))


def warn(msg: str):
    print(_color(f"[WARN]  {msg}", 33))


def err(msg: str):
    print(_color(f"[FAIL]  {msg}", 31))


def load_configuration() -> dict:
    """Read local configuration without printing values or changing global settings."""
    from dotenv import dotenv_values
    values = {key: value for key, value in dotenv_values(ROOT / ".env").items()
              if value is not None}
    values.update(os.environ)  # Explicit process settings have priority.
    values.setdefault("PYTHONIOENCODING", "utf-8")
    return values


def configured_services(env: dict, model_timeout: int, service_timeout: int) -> list:
    prefixes = {"sasrec_api": "SASREC", "backend_api": "BACKEND",
                "recommendation_agent": "RECOMMENDATION",
                "llm_rerank_service": "RERANK", "shop_web": "SHOPWEB"}
    configured = []
    for original in SERVICES:
        svc = dict(original)
        identity = svc["cwd"].name
        prefix = prefixes.get(identity, identity.upper())
        port = int(env.get(prefix + "_PORT", svc["port"]))
        if not 1 <= port <= 65535:
            raise ValueError("invalid configured service port")
        host = env.get(prefix + "_HOST", "0.0.0.0").strip()
        if host in {"0.0.0.0", "::", "localhost", ""}:
            host = "127.0.0.1"
        if not re.fullmatch(r"[A-Za-z0-9_.:\-]+", host):
            raise ValueError("invalid configured service host")
        authority = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        svc.update(port=port, host=host, identity=identity,
                   health=f"http://{authority}/health",
                   wait=model_timeout if identity == "sasrec_api" else service_timeout)
        if identity == "recommendation_agent":
            svc["health"] = f"http://{authority}/recommend/health"
        configured.append(svc)
    return configured


def _health_payload_ok(payload: dict, expected: str) -> bool:
    """Require the known service contract, not just HTTP 200 from an occupied port."""
    if not isinstance(payload, dict):
        return False
    if expected == "sasrec_api":
        data = payload.get("dataset_info")
        return (payload.get("status") == "healthy" and payload.get("model_loaded") is True
                and isinstance(data, dict)
                and {"user_num", "item_num", "interaction_num"} <= data.keys())
    if expected == "backend_api":
        return all(payload.get(key) == "healthy"
                   for key in ("status", "database", "sasrec_api"))
    if expected == "recommendation_agent":
        return (payload.get("recommendation_system") == "healthy"
                and _health_payload_ok(payload.get("sasrec_service"), "sasrec_api"))
    return (payload.get("service") == expected
            and str(payload.get("status", "")).lower() in _READY_STATES
            and str(payload.get("database", "healthy")).lower() in _READY_STATES)


def check_health(url: str, timeout: float = 2, expected: str = "") -> bool:
    """Read a health endpoint directly, with no proxy, inference or business writes."""
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(url, timeout=timeout) as response:
            if response.status != 200:
                return False
            raw = response.read(65537)
            if len(raw) > 65536:
                return False
            text = raw.decode("utf-8").strip()
            if expected == "nacos":
                return text.lower() == "up"
            if expected == "prometheus":
                return text.lower() == "prometheus server is ready."
            if expected == "loki":
                return text.lower() == "ready"
            payload = json.loads(text)
            if expected == "grafana":
                return (isinstance(payload, dict) and payload.get("database") == "ok"
                        and isinstance(payload.get("version"), str) and bool(payload["version"]))
            if expected == "jaeger":
                return (isinstance(payload, dict) and isinstance(payload.get("data"), list)
                        and not payload.get("errors"))
            return _health_payload_ok(payload, expected)
    except Exception:
        return False


def wait_for_service(svc: dict, proc=None) -> bool:
    """Use a wall-time deadline and also stop waiting when our process exits."""
    deadline = time.monotonic() + svc["wait"]
    while True:
        if proc is not None and proc.poll() is not None:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        ready = check_health(svc["health"], timeout=min(2.0, remaining),
                             expected=svc["identity"])
        if ready and (proc is None or proc.poll() is None):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.25, remaining))


def docker_available() -> bool:
    try:
        result = subprocess.run([str(DOCKER_CLI), "info"], capture_output=True,
                                text=True, encoding="utf-8", errors="replace", timeout=10)
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _port_open(port: int, host: str = "127.0.0.1", timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_docker_running() -> bool:
    if docker_available():
        return True
    if not DOCKER_DESKTOP_EXE.exists():
        return False
    try:
        subprocess.Popen([str(DOCKER_DESKTOP_EXE)],
                         creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
    except OSError:
        return False
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if docker_available():
            return True
        time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))
    return False


def otel_stack_ready(timeout: float = 10) -> bool:
    """Require this Compose stack plus all exposed backend readiness contracts."""
    deadline = time.monotonic() + timeout
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        result = subprocess.run(
            [str(DOCKER_CLI), "compose", "-f", str(OTEL_COMPOSE), "ps", "--format", "json"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=min(10.0, remaining))
        if result.returncode != 0:
            return False
        text = result.stdout.strip()
        rows = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines()]
        states = {row.get("Service"): row for row in rows}
        if not (_OTEL_SERVICES <= states.keys()
                and all(states[name].get("State") == "running"
                        and states[name].get("Health", "") in {"", "healthy"}
                        for name in _OTEL_SERVICES)):
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not _port_open(OTEL_COLLECTOR_PORT, timeout=min(1.0, remaining)):
            return False
        for identity, url in _OTEL_READY_ENDPOINTS:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not check_health(url, timeout=min(2.0, remaining), expected=identity):
                return False
        return True
    except (OSError, ValueError, TypeError, AttributeError, subprocess.TimeoutExpired):
        return False


def start_otel_stack() -> bool:
    if not OTEL_COMPOSE.exists():
        return False
    try:
        result = subprocess.run(
            [str(DOCKER_CLI), "compose", "-f", str(OTEL_COMPOSE), "up", "-d"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=180)
        if result.returncode != 0:
            return False
    except (OSError, subprocess.TimeoutExpired):
        return False
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining > 0 and otel_stack_ready(timeout=min(10.0, remaining)):
            return True
        time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    return False


def _nacos_enabled(env=None) -> bool:
    return (os.environ if env is None else env).get("NACOS_ENABLED", "true").strip().lower() == "true"


def _nacos_endpoints(env: dict) -> list:
    addresses = env.get("NACOS_SERVER_ADDRESSES", "127.0.0.1:8848").split(",")
    if not 1 <= len(addresses) <= 8:
        raise ValueError("invalid Nacos address count")
    endpoints = []
    for address in addresses:
        url = urllib.parse.urlsplit(address.strip() if "://" in address else "http://" + address.strip())
        if (url.scheme not in {"http", "https"} or not url.hostname or url.username
                or url.password or url.query or url.fragment or url.path not in {"", "/"}):
            raise ValueError("invalid Nacos address")
        host = "127.0.0.1" if url.hostname == "localhost" else url.hostname
        authority = f"[{host}]:{url.port or 8848}" if ":" in host else f"{host}:{url.port or 8848}"
        endpoints.append(f"{url.scheme}://{authority}/nacos/v1/console/health/readiness")
    return endpoints


def nacos_ready(env: dict) -> bool:
    return any(check_health(url, expected="nacos") for url in _nacos_endpoints(env))


def start_nacos(env=None) -> bool:
    env = dict(os.environ) if env is None else env
    if not _nacos_enabled(env):
        return True
    if nacos_ready(env):
        return True
    endpoints = _nacos_endpoints(env)
    if endpoints != ["http://127.0.0.1:8848/nacos/v1/console/health/readiness"]:
        return False  # Do not start a different local registry for a remote/custom endpoint.
    home = Path(env.get("NACOS_HOME", str(NACOS_HOME)))
    startup = home / "bin" / ("startup.cmd" if sys.platform == "win32" else "startup.sh")
    if not startup.exists():
        return False
    try:
        command = ["cmd", "/c", str(startup), "-m", "standalone"] if sys.platform == "win32" else ["bash", str(startup), "-m", "standalone"]
        subprocess.Popen(command, cwd=str(home / "bin"), env=env,
                         creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW)
                         if sys.platform == "win32" else 0)
    except OSError:
        return False
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if nacos_ready(env):
            return True
        time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    return False


def shutdown_all(owned=None):
    """Stop only application Popen handles created by this invocation."""
    owned = processes if owned is None else owned
    for proc in reversed(owned):
        if proc.poll() is None:
            try:
                proc.terminate()
            except OSError:
                pass
    deadline = time.monotonic() + 5
    for proc in reversed(owned):
        try:
            proc.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except (OSError, subprocess.TimeoutExpired):
                warn("本次应用进程未能确认退出；请检查原启动窗口。")
        except OSError:
            pass
    for proc in list(owned):
        if proc in processes:
            processes.remove(proc)
    # Shared Docker/Nacos resources are deliberately not stopped here.


def signal_handler(sig, frame):
    raise KeyboardInterrupt


def _write_result(result: dict, path=None) -> bool:
    if path is None:
        return True
    try:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return True
    except OSError:
        err("无法保存指定的状态JSON。")
        return False


def start_all(no_docker=False, check_only=False, model_timeout=180, service_timeout=30,
              json_path=None) -> int:
    """Start/check the local stack; failures never authorize global cleanup."""
    global _otel_stack_active
    _otel_stack_active = False
    owned = []
    result = {"platform": "RecShop", "mode": "check" if check_only else "start",
              "status": "CHECKING" if check_only else "STARTING", "phase": "configuration", "dependencies": {},
              "services": [], "readiness_scope": "service health contracts; no LLM or inference call"}
    try:
        if not 1 <= model_timeout <= 900 or not 1 <= service_timeout <= 300:
            raise ValueError("startup timeout outside supported bounds")
        env = load_configuration()
        services = configured_services(env, model_timeout, service_timeout)
        result["phase"] = "otel"
        if no_docker:
            result["dependencies"]["otel"] = "SKIPPED"
        else:
            ready = (docker_available() and otel_stack_ready()) if check_only else (ensure_docker_running() and start_otel_stack())
            result["dependencies"]["otel"] = "READY" if ready else "FAILED"
            if not ready:
                raise RuntimeError("otel_not_ready")
            _otel_stack_active = True

        result["phase"] = "nacos"
        if _nacos_enabled(env):
            ready = nacos_ready(env) if check_only else start_nacos(env)
            result["dependencies"]["nacos"] = "READY" if ready else "FAILED"
            if not ready:
                raise RuntimeError("nacos_not_ready")
        else:
            result["dependencies"]["nacos"] = "SKIPPED"

        result["phase"] = "applications"
        for svc in services:
            state = {"name": svc["name"], "identity": svc["identity"], "port": svc["port"],
                     "status": "CHECKING", "ownership": "external"}
            result["services"].append(state)
            if check_only:
                ready = check_health(svc["health"], expected=svc["identity"])
                state["status"] = "READY" if ready else "FAILED"
                continue
            if _port_open(svc["port"], svc["host"]):
                if not check_health(svc["health"], expected=svc["identity"]):
                    state["status"] = "UNRECOGNIZED_OR_UNHEALTHY"
                    raise RuntimeError("occupied_port_not_ready")
                state["status"] = "READY"
                ok(f"{svc['name']} 现有端点符合健康协议；复用且不接管进程。")
                continue

            info(f"启动 {svc['name']}，等待就绪最多 {svc['wait']} 秒 ...")
            proc = subprocess.Popen(svc["cmd"], cwd=str(svc["cwd"]), env=env,
                                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0)
            owned.append(proc)
            processes.append(proc)
            state["ownership"] = "this_invocation"
            state["pid"] = proc.pid
            if not wait_for_service(svc, proc):
                state["status"] = "EXITED" if proc.poll() is not None else "HEALTH_TIMEOUT"
                raise RuntimeError("application_not_ready")
            state["status"] = "READY"
            ok(f"{svc['name']} 已通过健康检查。")

        if any(s["status"] != "READY" for s in result["services"]):
            raise RuntimeError("health_checks_failed")
        result["status"] = "READY"
        if not _write_result(result, json_path):
            return 1
        ok(f"RecShop {len(services)} 个应用端点均符合当前健康协议。")
        if check_only or not owned:
            return 0
        result["phase"] = "supervision"
        info("保持此窗口运行；Ctrl+C 仅停止本次启动的应用，保留复用进程及Docker/Nacos。")
        while True:
            for svc_state in result["services"]:
                if svc_state["ownership"] != "this_invocation":
                    continue
                proc = next(p for p in owned if p.pid == svc_state["pid"])
                if proc.poll() is not None:
                    svc_state["status"] = "EXITED"
                    raise RuntimeError("application_exited")
            time.sleep(1)
    except KeyboardInterrupt:
        result["status"] = "STOPPED"
        _write_result(result, json_path)
        return 130
    except Exception as exc:
        result["status"] = "FAILED"
        # Deliberately omit exception text: subprocess/config errors may contain secrets.
        result["error_type"] = type(exc).__name__
        _write_result(result, json_path)
        err(f"RecShop 未就绪：{result['phase']} 阶段失败；应用错误见原启动窗口日志。")
        return 1
    finally:
        if owned:
            shutdown_all(owned)


def stop_all() -> int:
    err("独立 --stop 没有可靠进程归属，已拒绝按端口终止服务。")
    info("请在原启动窗口按 Ctrl+C；只会清理该次启动拥有的应用进程。")
    return 2


def _bounded_seconds(upper):
    def convert(value):
        number = int(value)
        if not 1 <= number <= upper:
            raise argparse.ArgumentTypeError(f"必须为 1 至 {upper} 的整数秒")
        return number
    return convert


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="RecShop 本地启动与只读健康检查")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--stop", action="store_true", help="拒绝无归属的独立停止；请用原窗口 Ctrl+C")
    modes.add_argument("--check-only", action="store_true", help="只读健康检查，不启动或停止任何服务")
    parser.add_argument("--no-docker", action="store_true", help="明确跳过 Docker OTel 栈")
    parser.add_argument("--model-timeout", type=_bounded_seconds(900), default=180,
                        help="模型服务启动等待秒数，默认180，最大900")
    parser.add_argument("--service-timeout", type=_bounded_seconds(300), default=30,
                        help="其他应用启动等待秒数，默认30，最大300")
    parser.add_argument("--json", dest="json_path", help="可选状态JSON路径；不包含环境凭据")
    args = parser.parse_args(argv)
    if args.stop:
        return stop_all()
    if not args.check_only:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    return start_all(no_docker=args.no_docker, check_only=args.check_only,
                     model_timeout=args.model_timeout, service_timeout=args.service_timeout,
                     json_path=args.json_path)


if __name__ == "__main__":
    sys.exit(main())
