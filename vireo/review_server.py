"""Flask server for reviewing Vireo predictions.

Usage:
    python vireo/review_server.py [--data-dir /tmp/photo-review] [--port 8080]
"""

import argparse
import json
import logging
import os
import webbrowser

from flask import Flask, jsonify, render_template, request, send_from_directory
from xmp import write_sidecar

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def create_app(data_dir):
    """Create the Flask app configured with a data directory.

    Args:
        data_dir: path containing results.json and thumbnails/
    """
    app = Flask(
        __name__, template_folder=os.path.join(os.path.dirname(__file__), "templates")
    )
    app.config["DATA_DIR"] = data_dir

    def _load_results():
        with open(os.path.join(data_dir, "results.json")) as f:
            return json.load(f)

    def _save_results(data):
        with open(os.path.join(data_dir, "results.json"), "w") as f:
            json.dump(data, f, indent=2)

    def _load_settings():
        path = os.path.join(data_dir, "settings.json")
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        # Default from results.json settings
        data = _load_results()
        return data.get("settings", {})

    def _save_settings(settings):
        path = os.path.join(data_dir, "settings.json")
        with open(path, "w") as f:
            json.dump(settings, f, indent=2)

    @app.route("/")
    def index():
        return render_template("review.html")

    @app.route("/settings")
    def settings_page():
        return render_template("settings.html")

    @app.route("/api/photos")
    def get_photos():
        data = _load_results()
        category = request.args.get("category")
        if category:
            data["photos"] = [
                p for p in data["photos"] if p.get("category") == category
            ]
        return jsonify(data)

    @app.route("/api/accept/<filename>", methods=["POST"])
    def accept(filename):
        body = request.get_json(silent=True) or {}
        model = body.get("model")

        data = _load_results()
        for photo in data["photos"]:
            if photo.get("filename") == filename:
                # Get prediction from the specified model or the first available
                preds = photo.get("predictions", {})
                if model and model in preds:
                    prediction = preds[model]["prediction"]
                elif preds:
                    first_model = next(iter(preds))
                    prediction = preds[first_model]["prediction"]
                else:
                    return jsonify({"error": "no predictions available"}), 400

                write_sidecar(
                    photo["xmp_path"],
                    flat_keywords={prediction},
                    hierarchical_keywords=set(),
                )
                photo["status"] = "accepted"
                _save_results(data)
                return jsonify(
                    {"ok": True, "status": "accepted", "prediction": prediction}
                )
        return jsonify({"error": "not found"}), 404

    @app.route("/api/skip/<filename>", methods=["POST"])
    def skip(filename):
        data = _load_results()
        for photo in data["photos"]:
            if photo.get("filename") == filename:
                photo["status"] = "skipped"
                _save_results(data)
                return jsonify({"ok": True, "status": "skipped"})
        return jsonify({"error": "not found"}), 404

    @app.route("/api/skip-group/<group_id>", methods=["POST"])
    def skip_group(group_id):
        data = _load_results()
        for photo in data["photos"]:
            if photo.get("group_id") == group_id:
                photo["status"] = "skipped"
                _save_results(data)
                return jsonify({"ok": True, "status": "skipped"})
        return jsonify({"error": "group not found"}), 404

    @app.route("/api/accept-group/<group_id>", methods=["POST"])
    def accept_group(group_id):
        body = request.get_json(silent=True) or {}
        model = body.get("model")

        data = _load_results()
        for photo in data["photos"]:
            if photo.get("group_id") == group_id:
                cons = photo.get("consensus", {})
                if model and model in cons:
                    prediction = cons[model]["prediction"]
                elif cons:
                    first_model = next(iter(cons))
                    prediction = cons[first_model]["prediction"]
                else:
                    return jsonify({"error": "no consensus available"}), 400

                xmp_paths = photo.get("member_xmp_paths", [])
                written = 0
                for xp in xmp_paths:
                    try:
                        write_sidecar(
                            xp,
                            flat_keywords={prediction},
                            hierarchical_keywords=set(),
                        )
                        written += 1
                    except Exception:
                        log.warning("Failed to write XMP: %s", xp, exc_info=True)

                photo["status"] = "accepted"
                _save_results(data)
                return jsonify(
                    {"ok": True, "accepted_count": written, "prediction": prediction}
                )
        return jsonify({"error": "group not found"}), 404

    @app.route("/api/accept-batch", methods=["POST"])
    def accept_batch():
        body = request.get_json(silent=True) or {}
        category = body.get("category")
        min_confidence = body.get("min_confidence", 0.0)
        model = body.get("model")

        data = _load_results()
        accepted = 0
        for photo in data["photos"]:
            if photo["status"] != "pending":
                continue

            # Determine effective category for filtering
            if category:
                preds = photo.get("predictions", {})
                cons = photo.get("consensus", {})
                if preds:
                    if model and model in preds:
                        eff_cat = preds[model].get("category")
                    else:
                        eff_cat = preds[next(iter(preds))].get("category")
                elif cons:
                    eff_cat = photo.get("category")
                else:
                    eff_cat = None
                if eff_cat != category:
                    continue

            # Handle individual photos
            preds = photo.get("predictions", {})
            if preds:
                if model and model in preds:
                    pred = preds[model]
                else:
                    pred = preds[next(iter(preds))]

                if pred["confidence"] < min_confidence:
                    continue

                try:
                    write_sidecar(
                        photo["xmp_path"],
                        flat_keywords={pred["prediction"]},
                        hierarchical_keywords=set(),
                    )
                    photo["status"] = "accepted"
                    accepted += 1
                except Exception:
                    log.warning(
                        "Failed to write XMP for %s",
                        photo.get("filename"),
                        exc_info=True,
                    )

            # Handle groups
            elif photo.get("consensus"):
                cons = photo["consensus"]
                if model and model in cons:
                    pred = cons[model]
                else:
                    pred = cons[next(iter(cons))]

                if pred["confidence"] < min_confidence:
                    continue

                for xp in photo.get("member_xmp_paths", []):
                    try:
                        write_sidecar(
                            xp,
                            flat_keywords={pred["prediction"]},
                            hierarchical_keywords=set(),
                        )
                    except Exception:
                        log.warning("Failed to write XMP: %s", xp, exc_info=True)

                photo["status"] = "accepted"
                accepted += 1

        _save_results(data)
        return jsonify({"ok": True, "accepted": accepted})

    @app.route("/api/settings", methods=["GET"])
    def get_settings():
        return jsonify(_load_settings())

    @app.route("/api/settings", methods=["POST"])
    def save_settings():
        body = request.get_json(silent=True) or {}
        settings = _load_settings()
        settings.update(body)
        _save_settings(settings)
        return jsonify({"ok": True})

    @app.route("/thumbnails/<filename>")
    def thumbnail(filename):
        return send_from_directory(os.path.join(data_dir, "thumbnails"), filename)

    return app


def main():
    parser = argparse.ArgumentParser(description="Review Vireo predictions.")
    parser.add_argument(
        "--data-dir", default="/tmp/photo-review", help="Directory with results.json"
    )
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    app = create_app(args.data_dir)
    webbrowser.open(f"http://localhost:{args.port}")
    app.run(host="127.0.0.1", port=args.port, debug=False)


if __name__ == "__main__":
    main()
