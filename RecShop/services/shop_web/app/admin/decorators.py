from functools import wraps

from flask import abort, redirect, url_for
from flask_login import current_user
from app.auth import ROLE_ADMIN, get_current_role


def admin_required(f):
    """Ensure current user is an active admin."""

    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or get_current_role() != ROLE_ADMIN:
            return redirect(url_for("admin.login"))
        if getattr(current_user, "status", None) != "active":
            abort(403)
        return f(*args, **kwargs)

    return decorated


def role_required(*roles):
    """Allow access only when admin role is in the allowed set."""

    def wrapper(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated or get_current_role() != ROLE_ADMIN:
                return redirect(url_for("admin.login"))
            if getattr(current_user, "status", None) != "active":
                abort(403)
            if roles and getattr(current_user, "role", None) not in roles:
                abort(403)
            return f(*args, **kwargs)

        return decorated

    return wrapper
