from flask import Blueprint

merchant_bp = Blueprint('merchant', __name__, url_prefix='/merchant')

from app.merchant import routes  # noqa: E402, F401
