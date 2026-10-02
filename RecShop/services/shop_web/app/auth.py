from functools import wraps

from flask import abort, redirect, request, session, url_for
from flask_login import current_user


ROLE_BUYER = "buyer"
ROLE_MERCHANT = "merchant"
ROLE_ADMIN = "admin"
VALID_ROLES = {ROLE_BUYER, ROLE_MERCHANT, ROLE_ADMIN}


def get_current_role(default=ROLE_BUYER):
    role = session.get("_user_role", default)
    return role if role in VALID_ROLES else default


def is_buyer():
    return current_user.is_authenticated and bool(getattr(current_user, "user_token", None))


def is_merchant():
    return current_user.is_authenticated and get_current_role() == ROLE_MERCHANT


def is_admin():
    return current_user.is_authenticated and get_current_role() == ROLE_ADMIN


def role_required(*roles, login_endpoint="buyer.login"):
    allowed = set(roles)

    def wrapper(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for(login_endpoint))
            if get_current_role() not in allowed:
                abort(403)
            return f(*args, **kwargs)

        return decorated

    return wrapper


def handle_non_buyer_access():
    if request.path.startswith("/api/"):
        abort(403)

    role = get_current_role()
    if role == ROLE_ADMIN:
        return redirect(url_for("admin.dashboard"))
    if role == ROLE_MERCHANT:
        return redirect(url_for("merchant.dashboard"))
    return redirect(url_for("buyer.login"))
