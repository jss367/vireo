"""Small, dependency-free system endpoints used during startup."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path

from flask import Blueprint, jsonify

system_blueprint = Blueprint("system", __name__)


@system_blueprint.get("/api/health")
def health():
    return jsonify({"status": "ok"})


@system_blueprint.get("/api/v1/health")
def stable_health():
    # The marker lets the native single-instance probe distinguish Vireo
    # from an unrelated service that happens to reuse a stale port.
    from runtime import SERVICE_MARKER

    return jsonify({"service": SERVICE_MARKER, "status": "ok"})


def _application_version():
    try:
        return package_version("vireo")
    except PackageNotFoundError:
        import tomllib

        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        try:
            with pyproject.open("rb") as handle:
                return tomllib.load(handle)["project"]["version"]
        except (OSError, KeyError, TypeError, ValueError):
            return "0.0.0"


@system_blueprint.get("/api/version")
@system_blueprint.get("/api/v1/version")
def version():
    return jsonify({"version": _application_version()})
