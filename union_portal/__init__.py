from flask import Blueprint

union = Blueprint(
    "union",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/union"
)

from . import routes
