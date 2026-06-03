from flask import Blueprint, jsonify, render_template, request
from sqlalchemy import text as _t

import state
from extensions import require_auth
from db_config import get_engine
from helpers import get_setting
from config import SETTINGS_SCHEMA

bp = Blueprint("admin_settings", __name__)

# Flat lookup: key → field definition
_SCHEMA_INDEX: dict = {f["key"]: f for fields in SETTINGS_SCHEMA.values() for f in fields}


@bp.get("/admin/settings")
@require_auth
def admin_settings_page():
    return render_template("admin_settings.html")


@bp.get("/api/admin/settings")
@require_auth
def admin_settings_get():
    sections = []
    for section, fields in SETTINGS_SCHEMA.items():
        rows = []
        for f in fields:
            current = get_setting(f["key"], f["default"])
            rows.append({**f, "current": current,
                         "modified": str(current) != str(f["default"])})
        sections.append({"name": section, "fields": rows})
    return jsonify({"sections": sections})


@bp.post("/api/admin/settings")
@require_auth
def admin_settings_save():
    updates = (request.get_json(force=True) or {}).get("settings", {})
    engine  = get_engine()
    saved   = []
    with engine.connect() as conn:
        for key, raw_value in updates.items():
            f = _SCHEMA_INDEX.get(key)
            if not f:
                continue
            try:
                if f["type"] == "int":
                    value = str(int(float(raw_value)))
                elif f["type"] == "float":
                    value = str(round(float(raw_value), 6))
                else:
                    value = str(raw_value)
            except (ValueError, TypeError):
                continue
            conn.execute(_t("""
                INSERT INTO admin_settings (key_name, value)
                VALUES (:k, :v)
                ON DUPLICATE KEY UPDATE value = VALUES(value)
            """), {"k": key, "v": value})
            state.settings[key] = value
            saved.append(key)
        conn.commit()

    # Bust recommendation cache if relevant weights/thresholds changed
    if any(k.startswith("reco_") for k in saved):
        state._reco_cache["ts"] = 0.0

    return jsonify({"ok": True, "saved": len(saved)})


@bp.post("/api/admin/settings/reset")
@require_auth
def admin_settings_reset():
    keys = (request.get_json(force=True) or {}).get("keys", [])
    to_reset = [k for k in keys if k in _SCHEMA_INDEX]
    if not to_reset:
        return jsonify({"ok": True, "reset": 0})
    engine = get_engine()
    with engine.connect() as conn:
        for key in to_reset:
            conn.execute(_t("DELETE FROM admin_settings WHERE key_name = :k"), {"k": key})
            state.settings.pop(key, None)
        conn.commit()
    if any(k.startswith("reco_") for k in to_reset):
        state._reco_cache["ts"] = 0.0
    return jsonify({"ok": True, "reset": len(to_reset)})
