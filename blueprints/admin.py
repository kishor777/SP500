from flask import Blueprint, render_template, request, session, redirect
from extensions import _ADMIN_SECRET

bp = Blueprint("admin", __name__)

@bp.get("/admin/login")
def admin_login_page():
    logged_in = bool(session.get("authed"))
    info = "Already logged in." if logged_in else None
    return render_template("login.html", error=None, info=info,
                           next=request.args.get("next", "/"), logged_in=logged_in)

@bp.get("/api/admin/login")
def admin_login_redirect():
    return redirect("/admin/login")

@bp.post("/api/admin/login")
def admin_login():
    password = request.form.get("password", "")
    next_url  = request.form.get("next", "/")
    if not next_url.startswith("/") or next_url.startswith("//"):
        next_url = "/"
    if not _ADMIN_SECRET:
        session["authed"] = True
        return redirect(next_url)
    if password == _ADMIN_SECRET:
        session["authed"] = True
        return redirect(next_url)
    return render_template("login.html", error="Incorrect password.",
                           info=None, next=next_url, logged_in=False), 401

@bp.get("/api/admin/logout")
def admin_logout():
    session.clear()
    return redirect("/admin/login")
