"""Validated OTel metric period, retaining RecShop's existing 15-second default.

The standard SDK environment variable is honored explicitly because all service
call sites previously passed a literal interval, overriding SDK env handling.
No SDK patching, environment mutation, network access or thread creation occurs.
"""
from __future__ import annotations

import math
import os
from threading import TIMEOUT_MAX
from typing import Mapping

ENV_NAME = "OTEL_METRIC_EXPORT_INTERVAL"
DEFAULT_INTERVAL_MILLIS = 15000


class MetricExportConfigError(ValueError):
    """Invalid explicit interval; message intentionally excludes its value."""


def metric_export_interval_millis(environ: Mapping[str, str] | None = None) -> float:
    environment = os.environ if environ is None else environ
    if ENV_NAME not in environment:
        return DEFAULT_INTERVAL_MILLIS
    raw = environment[ENV_NAME]
    if type(raw) is not str or not raw.strip():
        raise MetricExportConfigError(ENV_NAME + " must be finite positive milliseconds")
    try:
        value = float(raw)
    except (ValueError, OverflowError):
        raise MetricExportConfigError(ENV_NAME + " must be finite positive milliseconds") from None
    if not math.isfinite(value) or value <= 0 or value / 1000 > TIMEOUT_MAX:
        raise MetricExportConfigError(ENV_NAME + " is outside the supported positive timer range")
    return value
