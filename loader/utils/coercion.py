from typing import Any, Iterable, Type
from neomodel import (
    IntegerProperty,
    FloatProperty,
    BooleanProperty,
    DateProperty,
    DateTimeNeo4jFormatProperty,
    AsyncStructuredNode
)
from loader.domain.properties.datetime import DateNeo4jFormatProperty





def _coerce_for_property(prop, v: Any) -> Any:
    """
    Coerce a value `v` to be suitable for the given NeoModel property `prop`.
    """
    if isinstance(prop, IntegerProperty):
        if isinstance(v, bool):
            raise ValueError("bool is not an int")
        if isinstance(v, int):
            return v
        if isinstance(v, float) and v.is_integer():
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                return int(s)
        raise ValueError(f"cannot coerce {v!r} to int")

    # FloatProperty: accept float/int/"3.14"
    if isinstance(prop, FloatProperty):
        if isinstance(v, bool):
            raise ValueError("bool is not a float")
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            return float(v.strip())
        raise ValueError(f"cannot coerce {v!r} to float")

    # BooleanProperty: accept bool / common strings
    if isinstance(prop, BooleanProperty):
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            s = v.strip().lower()
            if s in {"true", "t", "yes", "y", "1"}:
                return True
            if s in {"false", "f", "no", "n", "0"}:
                return False
        raise ValueError(f"cannot coerce {v!r} to bool")

    # Dates/times: NeoModel expects python date/datetime typically.
    # If your ingestion sometimes gives strings, you can parse here.
    if isinstance(prop, (
        DateProperty,
        DateTimeNeo4jFormatProperty,
        DateNeo4jFormatProperty)):
        if isinstance(v, str):
            # Attempt ISO format parsing; adjust if you have other formats
            try:
                return prop.inflate(v.strip())
            except Exception as e:
                raise ValueError(f"cannot coerce {v!r} to date/datetime: {e}")

        # keep as-is (or add parsing if you actually see strings here)
        return v

    # Fallback: trust NeoModel's deflation (may raise if incompatible)
    return prop.deflate(v)


def build_props_map(
    data: dict[str, Any],
    fields: Iterable[str],
    *,
    model_cls: Type[AsyncStructuredNode] | None = None,
    strict: bool = False,
    drop_invalid: bool = True,
    log=None,
) -> dict[str, Any]:
    """
    Build a props dict suitable for Cypher params, using NeoModel schema to coerce/deflate types.

    - strict=True: raise on any bad value
    - drop_invalid=True: skip fields that fail coercion/deflation
    """
    props: dict[str, Any] = {}
    schema_props = model_cls.defined_properties() if model_cls else {}

    for k in fields:
        v = data.get(k)
        if v is None:
            continue

        prop = schema_props.get(k)
        if prop is None:
            # No schema info; pass through unchanged
            props[k] = v
            continue

        try:
            coerced = _coerce_for_property(prop, v)
        except Exception as e:
            if strict:
                raise
            if log:
                log.warning(f"Dropping invalid {model_cls.__name__}.{k}={v!r}: {e}")
            if drop_invalid:
                continue
            props[k] = v
        else:
            props[k] = coerced

    return props