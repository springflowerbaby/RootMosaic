"""
Nacos 注册工具模块（Phase 1 + Fix）

提供两个函数:
    register_service(service_name, ip, port) -> bool
    deregister_service(service_name, ip, port) -> bool

行为要点:
1. 从环境变量读取 Nacos 配置
2. NACOS_ENABLED != "true" 时直接跳过,返回 True
3. NacosClient 初始化不传 username/password(当前 Nacos 未开启鉴权)
4. 所有异常被捕获,打印 warning 日志后返回 False,绝不中断主进程
5. 注册成功后自动启动一个 daemon 心跳线程,每 5 秒续约一次实例,
   防止 Nacos 15 秒后清理临时实例;deregister 时 set stop_event 优雅停止。
"""

import logging
import os
import random
import socket
import threading
import time

logger = logging.getLogger("nacos_client")

# 心跳配置
_HEARTBEAT_INTERVAL = 5  # seconds
_HEARTBEAT_LOG_EVERY = 10  # 每 N 次成功心跳打一条 debug

# (service_name, ip, port) -> (thread, stop_event)
_heartbeat_registry: dict = {}
_registry_lock = threading.Lock()

# 服务发现日志节流计数(按 service_name)
_discovery_success_counter: dict = {}
_discovery_disabled_counter: dict = {}
_discovery_counter_lock = threading.Lock()
_DISCOVERY_SUCCESS_LOG_EVERY = 20   # 每 20 次成功 Nacos 发现打一条 debug
_DISCOVERY_DISABLED_LOG_EVERY = 50  # 每 50 次走 fallback(开关关闭)打一条 debug

# ---- 无 Nacos 容错(可复现性关键)----
# Nacos 没装/没起/地址不对时,nacos-sdk 的 list_naming_instance / add_naming_instance
# 会重试到 ~2s 才失败,导致每次服务发现都凭空 +2s(高延迟、偶发 network error)。
# 解法:调 SDK 前先做一次极快的 TCP 探活(连不上立即失败),并用熔断缓存"不可达"状态,
# 熔断期内直接走本地 fallback,不碰 SDK。→ 没有 Nacos 也能秒启动、零额外延迟。
_TCP_PROBE_TIMEOUT = 0.3   # 秒:快速探活的 TCP 连接超时
_CIRCUIT_COOLDOWN = 10.0   # 秒:探到不可达后,熔断打开多久(期间直接 fallback)
_circuit_open_until = 0.0  # epoch 秒;time.time() < 此值 → 熔断打开
_circuit_lock = threading.Lock()


def _first_server_addr():
    """取 NACOS_SERVER_ADDRESSES 的第一个 host:port。"""
    raw = _env("NACOS_SERVER_ADDRESSES", "127.0.0.1:8848").split(",")[0].strip()
    host, _, port = raw.partition(":")
    try:
        return (host or "127.0.0.1"), int(port or "8848")
    except ValueError:
        return (host or "127.0.0.1"), 8848


def _nacos_reachable() -> bool:
    """极快 TCP 探活:Nacos 端口能否连上。连接被拒/超时立即返回 False,不走 SDK 长重试。"""
    host, port = _first_server_addr()
    try:
        with socket.create_connection((host, port), timeout=_TCP_PROBE_TIMEOUT):
            return True
    except Exception:
        return False


def _circuit_blocks() -> bool:
    with _circuit_lock:
        return time.time() < _circuit_open_until


def _trip_circuit() -> None:
    """打开熔断 _CIRCUIT_COOLDOWN 秒(探到 Nacos 不可达时调用)。"""
    global _circuit_open_until
    with _circuit_lock:
        _circuit_open_until = time.time() + _CIRCUIT_COOLDOWN


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _is_enabled() -> bool:
    return _env("NACOS_ENABLED", "true").strip().lower() == "true"


def _build_client():
    """构造 NacosClient;失败时返回 None,不抛异常。"""
    try:
        from nacos import NacosClient
    except Exception as e:
        logger.warning("nacos-sdk-python 未安装或导入失败: %s", e)
        return None

    server = _env("NACOS_SERVER_ADDRESSES", "127.0.0.1:8848")
    namespace = _env("NACOS_NAMESPACE", "public")
    try:
        return NacosClient(server, namespace=namespace)
    except Exception as e:
        logger.warning("NacosClient 初始化失败: %s", e)
        return None


def _heartbeat_loop(service_name: str, ip: str, port: int, group: str, stop_event: threading.Event):
    """心跳线程:每 _HEARTBEAT_INTERVAL 秒续约一次,直到 stop_event 被 set。

    使用重复 add_naming_instance 续约(nacos-sdk-python 2.0.3 的 send_heartbeat
    接口与 2.x server 的心跳协议不完全兼容,重复注册是最稳妥的续约方式且幂等)。
    """
    success_count = 0
    while not stop_event.is_set():
        # 先等待,再做动作;这样 stop_event 能立即打断
        if stop_event.wait(_HEARTBEAT_INTERVAL):
            break
        # Nacos 暂不可达就跳过本次续约(不卡 ~2s),下个周期再试
        if not _nacos_reachable():
            continue
        client = _build_client()
        if client is None:
            logger.warning(
                "Nacos heartbeat: client build failed for %s @ %s:%s, will retry",
                service_name, ip, port,
            )
            continue
        try:
            client.add_naming_instance(
                service_name=service_name,
                ip=ip,
                port=port,
                group_name=group,
                metadata={"registered_from": "recweb2"},
                ephemeral=True,
            )
            success_count += 1
            if success_count % _HEARTBEAT_LOG_EVERY == 0:
                logger.debug(
                    "Nacos heartbeat ok x%d: %s @ %s:%s",
                    success_count, service_name, ip, port,
                )
        except Exception as e:
            logger.warning(
                "Nacos heartbeat failed for %s @ %s:%s: %s",
                service_name, ip, port, e,
            )
    logger.info("Nacos heartbeat thread stopped: %s @ %s:%s", service_name, ip, port)


def _start_heartbeat(service_name: str, ip: str, port: int, group: str) -> None:
    """启动一个 daemon 心跳线程。若同一 (service, ip, port) 已在运行则跳过。"""
    key = (service_name, ip, port)
    with _registry_lock:
        existing = _heartbeat_registry.get(key)
        if existing is not None:
            thread, _ = existing
            if thread.is_alive():
                logger.debug("Nacos heartbeat thread already running for %s", key)
                return
            # 线程已死,清理后重启
            _heartbeat_registry.pop(key, None)

        stop_event = threading.Event()
        thread = threading.Thread(
            target=_heartbeat_loop,
            args=(service_name, ip, port, group, stop_event),
            name=f"nacos-heartbeat-{service_name}",
            daemon=True,
        )
        _heartbeat_registry[key] = (thread, stop_event)
        thread.start()
    logger.info(
        "Nacos heartbeat thread started: %s @ %s:%s (every %ss)",
        service_name, ip, port, _HEARTBEAT_INTERVAL,
    )


def _stop_heartbeat(service_name: str, ip: str, port: int) -> None:
    """请求心跳线程停止(不阻塞等待)。"""
    key = (service_name, ip, port)
    with _registry_lock:
        existing = _heartbeat_registry.pop(key, None)
    if existing is None:
        return
    _, stop_event = existing
    try:
        stop_event.set()
    except Exception as e:
        logger.warning("stop heartbeat event.set failed for %s: %s", key, e)


def register_service(service_name: str, ip: str, port: int) -> bool:
    """向 Nacos 注册一个临时实例,并启动心跳续约线程。失败返回 False,不抛异常。"""
    if not _is_enabled():
        logger.info("Nacos disabled, skip register (%s)", service_name)
        return True

    # 快速容错:Nacos 不可达时立即跳过注册(不卡 ~2s),服务照常启动。
    if not _nacos_reachable():
        _trip_circuit()
        logger.info("Nacos 不可达,跳过注册 (%s),服务仍正常启动", service_name)
        return False

    client = _build_client()
    if client is None:
        return False

    group = _env("NACOS_GROUP", "RECWEB2")
    try:
        ok = client.add_naming_instance(
            service_name=service_name,
            ip=ip,
            port=port,
            group_name=group,
            metadata={"registered_from": "recweb2"},
            ephemeral=True,
        )
        if ok:
            logger.info(
                "Nacos register success: %s @ %s:%s (group=%s)",
                service_name, ip, port, group,
            )
            try:
                _start_heartbeat(service_name, ip, port, group)
            except Exception as e:
                logger.warning("启动心跳线程失败,忽略: %s", e)
        else:
            logger.warning(
                "Nacos register returned falsy for %s @ %s:%s",
                service_name, ip, port,
            )
        return bool(ok)
    except Exception as e:
        logger.warning(
            "Nacos register failed for %s @ %s:%s: %s",
            service_name, ip, port, e,
        )
        return False


def _tick_discovery_counter(counter: dict, service_name: str, every: int) -> bool:
    """计数并返回本次是否达到 "every" 的倍数(即该打节流日志)。"""
    with _discovery_counter_lock:
        n = counter.get(service_name, 0) + 1
        counter[service_name] = n
    return (n % every) == 0


def get_service_url(service_name: str, fallback_url: str = None, scheme: str = "http") -> str:
    """从 Nacos 实时查询指定服务的一个健康实例,拼 scheme://ip:port 返回。

    - NACOS_ENABLED 不为 "true" → 直接返回 fallback_url(不调 Nacos)
    - Nacos 查不到健康实例 / 接口异常 → 打 warning 日志,返回 fallback_url
    - fallback_url 为空且查询失败 → 打 error 日志,返回 ""(不抛异常)
    - 不做任何缓存,每次调用都实时查询

    返回的 URL 不含路径后缀,调用方自行拼接。
    """
    fb = fallback_url or ""

    if not _is_enabled():
        if _tick_discovery_counter(_discovery_disabled_counter, service_name, _DISCOVERY_DISABLED_LOG_EVERY):
            logger.debug(
                "Nacos disabled, use fallback for %s -> %s (sampled)",
                service_name, fb,
            )
        return fb

    # 快速容错:Nacos 不可达(没装/没起/熔断中)→ 立即走 fallback,零延迟。
    if _circuit_blocks():
        return fb
    if not _nacos_reachable():
        _trip_circuit()
        return fb

    client = _build_client()
    if client is None:
        _trip_circuit()
        logger.warning(
            "Nacos client unavailable, use fallback for %s -> %s",
            service_name, fb,
        )
        if not fb:
            logger.error("No fallback available for %s, return empty string", service_name)
        return fb

    group = _env("NACOS_GROUP", "RECWEB2")
    try:
        result = client.list_naming_instance(
            service_name=service_name,
            group_name=group,
            healthy_only=True,
        )
        # nacos-sdk-python 返回 dict,健康实例在 "hosts" 字段
        hosts = []
        if isinstance(result, dict):
            hosts = result.get("hosts") or []
        if not hosts:
            logger.warning(
                "Nacos returned no healthy instance for %s, use fallback -> %s",
                service_name, fb,
            )
            if not fb:
                logger.error("No fallback available for %s, return empty string", service_name)
            return fb

        chosen = random.choice(hosts)
        ip = chosen.get("ip")
        port = chosen.get("port")
        if not ip or not port:
            logger.warning(
                "Nacos instance missing ip/port for %s (raw=%s), use fallback -> %s",
                service_name, chosen, fb,
            )
            return fb

        url = f"{scheme}://{ip}:{port}"
        if _tick_discovery_counter(_discovery_success_counter, service_name, _DISCOVERY_SUCCESS_LOG_EVERY):
            logger.debug(
                "Nacos discover %s -> %s (sampled, healthy=%d)",
                service_name, url, len(hosts),
            )
        return url
    except Exception as e:
        _trip_circuit()
        logger.warning(
            "Nacos unreachable/discovery failed for %s: %s, use fallback -> %s",
            service_name, e, fb,
        )
        if not fb:
            logger.error("No fallback available for %s, return empty string", service_name)
        return fb


def deregister_service(service_name: str, ip: str, port: int) -> bool:
    """从 Nacos 注销实例,并停止心跳线程。失败返回 False,不抛异常。"""
    # 无论开关如何都先把心跳线程停掉,防止悬挂
    try:
        _stop_heartbeat(service_name, ip, port)
    except Exception as e:
        logger.warning("停止心跳线程异常,忽略: %s", e)

    if not _is_enabled():
        logger.info("Nacos disabled, skip deregister (%s)", service_name)
        return True

    client = _build_client()
    if client is None:
        return False

    group = _env("NACOS_GROUP", "RECWEB2")
    try:
        ok = client.remove_naming_instance(
            service_name=service_name,
            ip=ip,
            port=port,
            group_name=group,
            ephemeral=True,
        )
        if ok:
            logger.info(
                "Nacos deregister success: %s @ %s:%s (group=%s)",
                service_name, ip, port, group,
            )
        else:
            logger.warning(
                "Nacos deregister returned falsy for %s @ %s:%s",
                service_name, ip, port,
            )
        return bool(ok)
    except Exception as e:
        logger.warning(
            "Nacos deregister failed for %s @ %s:%s: %s",
            service_name, ip, port, e,
        )
        return False
