"""Flask extensions and auth — imported by blueprints and sp500_viewer."""
import os
from functools import wraps
from flask import request, session, redirect, jsonify
from flask_limiter import Limiter

# ── Rate limiter (init_app called in sp500_viewer) ────────────────────────────
def _client_ip():
    return request.headers.get("X-Forwarded-For",
                               request.remote_addr or "127.0.0.1").split(",")[0].strip()

limiter = Limiter(key_func=_client_ip, default_limits=["200 per minute"],
                  storage_uri="memory://")

# ── Auth ──────────────────────────────────────────────────────────────────────
_ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "").strip()
print(f"[auth] ADMIN_SECRET length={len(_ADMIN_SECRET)} set={bool(_ADMIN_SECRET)}")

def require_auth(f):
    @wraps(f)
    def _inner(*args, **kwargs):
        if _ADMIN_SECRET and not session.get("authed"):
            return jsonify({"error": "Unauthorized", "login": "/admin/login"}), 401
        return f(*args, **kwargs)
    return _inner

# Public paths that bypass the global auth check
_PUBLIC_PATHS = {"/admin/login", "/api/admin/login", "/api/admin/logout", "/health"}

def init_auth(app):
    """Register the global before_request auth guard on the Flask app."""
    @app.before_request
    def _global_auth():
        if not _ADMIN_SECRET:
            return
        if request.path in _PUBLIC_PATHS or request.path.startswith("/admin"):
            return
        if not session.get("authed"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Unauthorized", "login": "/admin/login"}), 401
            return redirect(f"/admin/login?next={request.path}")
