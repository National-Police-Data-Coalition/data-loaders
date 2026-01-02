from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

Handler = Callable[[list[dict[str, Any]]], Awaitable[None]]

REGISTRY: dict[str, Handler] = {}


def register(model: str):
    """Decorator: register an ingest handler for a model."""
    def deco(fn: Handler) -> Handler:
        if model in REGISTRY and REGISTRY[model] is not fn:
            raise RuntimeError(f"Handler for model '{model}' is already registered.")
        REGISTRY[model] = fn
        return fn
    return deco