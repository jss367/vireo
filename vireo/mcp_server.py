"""Vireo's MCP server: the catalog as tools for Claude Code, Claude Desktop
and any other Model Context Protocol client.

The server speaks MCP over stdio (the client spawns it) and reaches the
running Vireo over its HTTP API with the per-runtime token, exactly like a
script following ``docs/headless-api.md``. It opens no database and spawns no
instance: when nothing is running, every tool answers with the same plain
error telling the user to open Vireo.

Entry points: ``vireo-server --mcp`` from the packaged app and
``python vireo/app.py --mcp`` from a checkout (``app.main`` dispatches here
before the single-instance guard, since this process is not an instance).
``connect_info()`` builds the client-side configuration for this install;
``/api/system/mcp-config`` serves it to the Settings page.

Design: ``docs/plans/2026-10-17-mcp-server-design.md``.
"""

import contextlib
import hashlib
import ipaddress
import json
import os
import secrets
import shlex
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from mcp import types as mcp_types
from mcp.server.mcpserver import Image, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from runtime import read_runtime_json

SERVER_NAME = "vireo"

NOT_RUNNING_MESSAGE = (
    "Vireo is not running. Open the Vireo app (or start the sidecar with "
    "--headless) and try again."
)
BAD_TOKEN_MESSAGE = (
    "Vireo rejected the API token. If VIREO_TOKEN is set, make sure it matches "
    "the token in runtime.json in Vireo's data folder; otherwise restart Vireo so a fresh "
    "runtime.json is written."
)
TIMEOUT_MESSAGE = (
    "Vireo is running but did not answer in {seconds:g} seconds. It may still be "
    "working on the request, so check the result in Vireo before repeating a change."
)
UNPAIRED_URL_MESSAGE = (
    "VIREO_URL and VIREO_TOKEN overrides must be supplied together; "
    "runtime credentials are never borrowed by an override."
)
NON_LOOPBACK_URL_MESSAGE = (
    "VIREO_URL must be a numeric loopback HTTP origin "
    "(http://127.0.0.1:... or http://[::1]:...). "
    "Names, remote targets, credentials and URL paths are not supported."
)
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1"})

INSTRUCTIONS = """\
Vireo is the user's wildlife photo catalog. Tools act on the active
workspace, the same one the Vireo window shows. Photo ids are stable; folder,
collection and keyword ids come from the list_* and search_* tools.

Judging photos: never judge focus or sharpness from a thumbnail or a preview.
Use view_photo(view="preview") for composition and pose,
view_photo(view="subject") to identify the animal, and focus_crop, which shows
unresized pixels, to judge focus. quality_analyzed is true only when Vireo's subject
analysis is complete for the photo's current inputs and settings (the same
readiness check Vireo's own burst ranking uses); analysis_status is not_run,
incomplete, failed, complete or unknown (readiness could not be checked), with
analysis_reasons saying why. Quality fields (quality_score, sharpness,
subject_sharpness) can appear before analysis is complete, for example while
a Process run is between detection and segmentation; then quality_preliminary
is true and the numbers are not comparison evidence. get_photo adds
analysis_explanation.

Bursts: list_bursts lists a folder's bursts and burst ranks one burst's frames.
The ranking is provisional: Vireo names no keeper, so compare the frames with
focus_crop before flagging any. A burst with the reason unanalyzed_frames has
frames the ranking does not cover; run Process on them before choosing.

Edits: suggest_crop, suggest_tone and view_edit change nothing. Look at an edit
with view_edit before save_edit, which is one Undo step and refuses to replace
an edit the photo already has. That edit is the user's: export it as it is, or
put another look in save_edit_copy, which leaves it alone.

Exports: Undo cannot remove exported files. preview_export reports what would
be written and returns a token; show the user the destination, and call
start_export with that token. Follow the job with get_job and report what it
wrote, not what was asked for.

Writing: set_flag, set_rating, add_keyword and remove_keyword change the
catalog through the same routes the UI uses, so each call is one Undo step
in Vireo. Do not flag photos the user already flagged unless asked.

Species: get_species_evidence returns per-subject predictions, score meanings,
uncertainty, species-list provenance and inspection candidates for exact IDs.
It does not verify same-animal continuity or approve label propagation. Inspect
the frame and the specified detection using view_photo(view="subject",
detection_id=...). Use preview_species_assignment, then apply_species_assignment
with the unchanged selection, provenance and returned token. Preserve existing
identifications; model confidence is not a probability of correct identification.

Best photos: best_of_each_species lists one photo per species for a folder, a
collection, a date range or the whole workspace. Each species has a decision
saying why its first photo leads: highlight (it is a highlight of the species
and no other highlight there beats it), tie (several highlights nobody has
compared, listed in tied_photo_ids; the newest is shown meanwhile), pick (a
flagged pick, by Vireo's score), score or unanalyzed (nothing is highlighted
there; Vireo ordered it). A species has one list of highlights, shown as
Highlight 1, 2, ... on the Highlights page; the number is a photo's place
among the highlights in the set being shown. To settle a tie or replace a
guess, list more of that species (species=..., per_species=N), compare the
candidates with view_photo(view="subject") and view_photo(view="preview"),
then call choose_best_photo with the photos it beat in over and a reason. That
makes the photo a highlight ahead of them. A choice claims only what it names:
the photos in over, or the other highlights in the scope you pass. Leave a
decision whose source is the user's alone unless asked to revisit it.
When no highlight leads and no photo here has the species as its subject,
the decision is in_frame or outside_crop. Each photo's species_presence names
its role for this species; subject_species names the photo's subject. A pick
and score describe that subject, not another species merely in the frame.
"""

_QUALITY_METRIC_KEYS = ("quality_score", "sharpness", "subject_sharpness", "subject_size")
_PHOTO_SUMMARY_KEYS = (
    "id", "filename", "folder_id", "timestamp", "rating", "flag", "color_label",
    "width", "height", "camera_make", "camera_model", "iso",
    *_QUALITY_METRIC_KEYS,
    "latitude", "longitude", "location_status", "species",
)
_PHOTO_DETAIL_KEYS = _PHOTO_SUMMARY_KEYS + (
    "path", "folder_name", "companion_path", "working_copy_path",
    "xmp_path", "xmp_exists", "location", "file_size", "wildlife_excluded",
)
# EXIF tags worth surfacing without the whole dump. ExifTool groups them
# (``EXIF``, ``Composite``, ``MakerNotes``...); the first group holding a tag
# wins.
_EXPOSURE_TAGS = ("ExposureTime", "ShutterSpeed", "FNumber", "Aperture",
                  "FocalLength", "LensModel", "LensID", "ExposureCompensation")
_BURST_QUALITY_KEYS = (
    "quality_pct", "quality", "focus", "focus_basis", "sharpness", "subject_detail", "exposure",
    "crop_complete",
)
_BURST_CARD_KEYS = (
    "id", "filename", "rank", "role", "label", *_BURST_QUALITY_KEYS,
    "flag", "rating", "reasons", "reject_reasons", "detection_count", "has_mask",
    "selection_eligible", "analysis_status", "analysis_reasons",
    "automatic_keep_eligible", "keep_status", "keep_reasons",
)
_BURST_KEYS = (
    "seed_photo_id", "scope_method", "count", "coverage", "scored", "unscored_reason",
    "best_photo_id", "best_filename", "best_reasons", "suggested_reject_ids", "alternate_ids",
    "ranked_candidate_id", "ranking_status", "keep_review_message",
)
# The review reasons a burst can carry (``burst_evidence.AMBIGUITY_CODES``; a
# test keeps them equal). The server process never imports the scorer.
BURST_REASONS = (
    "positive_keep_not_validated", "unanalyzed_frames", "close_call", "no_eye_located",
    "subject_touches_edge", "multiple_subjects", "all_rejected", "subject_identity_uncertain",
    "subject_identity_conflict", "multiple_matching_subjects", "subject_analysis_missing",
    "invalid_subject_mask",
)
DEFAULT_BURST_PAGE = 50
MAX_BURST_PAGE = 200
# Frames one ranking covers: the route's default, and the most it accepts.
DEFAULT_RANKED_FRAMES = 120
MAX_RANKED_FRAMES = 500
DEFAULT_FRAME_PAGE = 50
# Side of a focus crop in unresized pixels, within the route's 128-1024.
DEFAULT_FOCUS_SIZE = 768
# Below this share of the original's size a decoded image is a downsized copy
# (``focus_crop.prefer_original`` draws the same line).
FULL_SCALE = 0.98
# The names a recipe may use (``edit_recipe``; tests keep them equal). The app
# ignores a name it does not know, so a misspelled one would save nothing.
RECIPE_KEYS = ("version", "crop", "rotation", "straighten", "flip", "adjustments", "local")
ADJUSTMENT_KEYS = (
    "exposure", "highlights", "shadows", "whites", "blacks", "contrast", "vibrance", "saturation",
    "texture", "clarity", "dehaze", "sharpen", "sharpen_radius", "noise_reduction", "denoise_mode",
    "distortion", "vignetting", "defringe_purple", "defringe_green", "lens_profile",
    "white_balance", "temperature", "tint", "tone_curve", "point_curves", "point_color", "hsl",
    "color_grading",
)
DEFAULT_EDIT_SIZE = 1600
MIN_EDIT_SIZE, MAX_EDIT_SIZE = 256, 2048
OUTDATED_APP_MESSAGE = (
    "The running Vireo is older than this connector and cannot {what}. Update Vireo and try again."
)
# Previews a start_export can still redeem; older ones must be previewed again.
MAX_PENDING_EXPORTS = 20
MAX_JOB_WAIT = 50
JOB_POLL_SECONDS = 1.0
TERMINAL_JOB_STATUSES = ("completed", "failed", "cancelled")
_JOB_KEYS = ("id", "type", "status", "started_at", "finished_at", "current_step", "historical")
_PROGRESS_KEYS = ("current", "total", "current_file", "phase")
# Longest list a job result keeps before it is cut and its length reported.
MAX_JOB_LIST = 50
FIND_HEAD = 'Find the head with view_photo(view="source") and pass its center_x and center_y.'
_COLLECTION_KEYS = ("id", "name", "photo_count", "available_photo_count",
                    "offline_photo_count", "has_visual", "can_add_photos")
_FOLDER_KEYS = ("id", "name", "parent_id", "path", "photo_count", "status",
                "is_workspace_root")

PREVIEW_SIZE = 1920
DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 200
# One ``/api/photos/analysis-readiness`` call takes at most this many ids
# (``services.analysis_retry.MAX_SELECTED_PHOTOS``; a test keeps them equal).
MAX_READINESS_IDS = 200
READINESS_PATH = "/api/photos/analysis-readiness"
READINESS_UNCHECKED = "Analysis readiness was not checked."


class VireoError(ToolError):
    """A tool failure whose message is meant for the person reading the chat.

    A ``ToolError`` reaches the client verbatim; any other exception is
    reported as a bare "Error executing tool".
    """


def _loopback_base_url(value):
    """Canonical numeric-loopback origin only; never trust parser normalization."""
    if not isinstance(value, str) or not value or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value):
        raise VireoError(NON_LOOPBACK_URL_MESSAGE)
    try:
        parts = urllib.parse.urlsplit(value)
        address = ipaddress.ip_address(parts.hostname or "")
        port = parts.port
    except ValueError as exc:
        raise VireoError(NON_LOOPBACK_URL_MESSAGE) from exc
    if (
        parts.scheme != "http" or address.compressed not in _LOOPBACK_HOSTS
        or parts.username is not None or parts.password is not None
        or parts.path not in {"", "/"} or "?" in value or "#" in value
        or "%" in (parts.hostname or "") or parts.netloc.endswith(":")
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise VireoError(NON_LOOPBACK_URL_MESSAGE)
    host = f"[{address.compressed}]" if address.version == 6 else address.compressed
    return f"{parts.scheme}://{host}" + (f":{port}" if port is not None else "")


def _is_loopback_http(url):
    """Whether the exact origin satisfies the capability destination boundary."""
    try:
        _loopback_base_url(url)
        return True
    except VireoError:
        return False


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Never forward the runtime capability to another origin or port."""
        return None


class VireoClient:
    """HTTP access to the running Vireo.

    ``base_url`` / ``token`` default to ``VIREO_URL`` / ``VIREO_TOKEN`` and
    then to ``~/.vireo/runtime.json``, read on every request so an instance
    started after the MCP client connected is found without a restart.
    """

    def __init__(self, base_url=None, token=None, timeout=120.0):
        self._base_url = base_url
        self._token = token
        self.timeout = timeout

    def _resolve(self):
        base_url = self._base_url if self._base_url is not None else os.environ.get("VIREO_URL")
        token = self._token if self._token is not None else os.environ.get("VIREO_TOKEN")
        if base_url is not None or token is not None:
            if not isinstance(base_url, str) or not base_url or not isinstance(token, str) or not token:
                raise VireoError(UNPAIRED_URL_MESSAGE)
            return _loopback_base_url(base_url), token
        runtime = read_runtime_json()
        if not runtime:
            raise VireoError(NOT_RUNNING_MESSAGE)
        port = runtime.get("port")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise VireoError(NOT_RUNNING_MESSAGE)
        token = runtime.get("token")
        if not isinstance(token, str) or not token:
            raise VireoError(BAD_TOKEN_MESSAGE)
        return f"http://127.0.0.1:{port}", token

    def request(self, method, path, params=None, json_body=None):
        """Return ``(status, headers, body_bytes)`` for one request."""
        base_url, token = self._resolve()
        url = base_url + path
        if params:
            query = {k: v for k, v in params.items() if v is not None}
            if query:
                url += "?" + urllib.parse.urlencode(query)
        data = None
        headers = {"X-Vireo-Token": token, "Accept": "application/json, image/jpeg"}
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
            with opener.open(req, timeout=self.timeout) as resp:
                return resp.status, resp.headers, resp.read()
        except urllib.error.HTTPError as err:
            return err.code, err.headers, err.read()
        except TimeoutError as err:
            raise VireoError(TIMEOUT_MESSAGE.format(seconds=self.timeout)) from err
        except (urllib.error.URLError, ConnectionError, OSError) as err:
            raise VireoError(NOT_RUNNING_MESSAGE) from err

    # -- helpers over ``request`` ------------------------------------------

    def _check(self, status, headers, body):
        if 200 <= status < 300:
            return
        if status == 401:
            raise VireoError(BAD_TOKEN_MESSAGE)
        message = ""
        if "json" in (headers.get("Content-Type") or ""):
            try:
                message = json.loads(body.decode("utf-8")).get("error") or ""
            except (ValueError, AttributeError):
                message = ""
        if not message:
            message = body.decode("utf-8", "replace").strip()[:300]
        raise VireoError(f"Vireo answered {status}: {message or 'no detail'}")

    def get_json(self, path, params=None):
        status, headers, body = self.request("GET", path, params=params)
        self._check(status, headers, body)
        return json.loads(body.decode("utf-8"))

    def _send_json(self, method, path, json_body):
        status, headers, body = self.request(method, path, json_body=json_body or {})
        self._check(status, headers, body)
        return json.loads(body.decode("utf-8")) if body else {}

    def post_json(self, path, json_body=None):
        return self._send_json("POST", path, json_body)

    def put_json(self, path, json_body=None):
        return self._send_json("PUT", path, json_body)

    def get_with_headers(self, path, params=None):
        """``(headers, body)``, for a route that describes its image in headers."""
        status, headers, body = self.request("GET", path, params=params)
        self._check(status, headers, body)
        return headers, body

    def get_bytes(self, path, params=None):
        headers, body = self.get_with_headers(path, params)
        return headers.get("Content-Type", ""), body


# -- payload trimming ---------------------------------------------------------

def _pick(row, keys):
    return {key: row[key] for key in keys if key in row}


def _burst_frame(card):
    """A ``burst`` frame with its quality fields nulled when ineligible.

    The best-batch cleanup nulls ``SCORE_FIELDS`` but leaves raw measurements
    like ``subject_tenengrad`` (surfaced here as ``sharpness``) in place. The
    tool guidance says ineligible frames carry no comparison numbers, so null
    every surfaced quality metric together.
    """
    frame = _pick(card, _BURST_CARD_KEYS)
    if frame.get("selection_eligible") is False:
        for key in _BURST_QUALITY_KEYS:
            if key in frame:
                frame[key] = None
    return frame


def _burst_entry(burst):
    """One ``list_bursts`` row: enough to choose a burst, not to judge it."""
    names = burst.get("filenames") or [None, None]
    return {
        **_pick(burst, ("seed_photo_id", "ranked_candidate_id", "scored", "flags")),
        "first_filename": names[0],
        "last_filename": names[-1],
        # This listing ranked ``ranked_frames`` of them; ``burst`` can rank more.
        "frames": burst.get("burst_photo_count"),
        "ranked_frames": burst.get("count"),
        "analyzed_frames": (burst.get("coverage") or {}).get("with_quality"),
        "reasons": _burst_reasons(burst),
    }


def _burst_reasons(burst):
    rows = (burst.get("ambiguity") or {}).get("reasons") or []
    return list(dict.fromkeys(row["code"] for row in rows))


def _burst_unflagged(burst):
    flags = burst.get("flags") or {}
    return not flags.get("flagged") and not flags.get("rejected")


def _header_floats(headers, name):
    """A header's comma-separated numbers, or None when it holds none."""
    try:
        return [float(part) for part in headers[name].split(",")]
    except (KeyError, AttributeError, ValueError):
        return None


def focus_crop_facts(headers):
    """What a focus crop shows, read from its ``X-Vireo-Focus-*`` headers.

    ``notes`` says in words what would make the crop misleading: a window
    that misses the head, or pixels that are not the original's.
    """
    def value(name):
        return headers.get(f"X-Vireo-Focus-{name}")

    scale = _header_floats(headers, "X-Vireo-Focus-Original-Scale")
    facts = {
        "basis": value("Basis"),
        "center": _header_floats(headers, "X-Vireo-Focus-Center"),
        "window": _header_floats(headers, "X-Vireo-Focus-Window"),
        "image_size": _header_floats(headers, "X-Vireo-Focus-Raster-Size"),
        "source": value("Source"),
        "original_scale": scale[0] if scale else None,
        "subject_clipped": {"1": True, "0": False}.get(value("Subject-Clipped")),
        "mask_shaded": value("Mask") == "1",
        "detection_id": int(value("Detection-Id")) if value("Detection-Id") else None,
        "eye_status": value("Eye-Status"),
    }
    facts["notes"] = _focus_notes(facts)
    return facts


def _focus_notes(facts):
    notes = []
    if facts["basis"] == "subject":
        notes.append("Centered on the middle of the subject box, which on a perched bird is "
                     f"usually the body. {FIND_HEAD}")
    elif facts["basis"] == "frame":
        notes.append(f"No subject box was used; this is the middle of the frame. {FIND_HEAD}")
    if facts["subject_clipped"]:
        notes.append("The crop cuts off part of the animal and may not show its head.")
    scale = facts["original_scale"]
    if scale is None:
        notes.append("How these pixels compare in size with the original's is unknown.")
    elif scale < FULL_SCALE:
        fix = (' Pass source="original" before judging focus.' if facts["source"] == "working_copy"
               else " Full-resolution pixels of the original are not available.")
        notes.append(f"This image was decoded at {scale:.2f} of the original's size.{fix}")
    return notes


def analysis_fields(photo, readiness):
    """What the quality numbers on ``photo`` are worth, from the app's readiness.

    ``readiness`` is one row of ``/api/photos/analysis-readiness`` (``status``,
    ``eligible``, ``reasons``, ``explanation``), ``{"error": text}`` when the
    call failed, or ``None`` when it was never made. ``quality_analyzed`` is
    the app's own verdict that subject analysis is complete for the current
    inputs and settings; it is never inferred from a stored score, which a
    Process run writes before segmentation and quality measurement finish.
    """
    has_scores = any(photo.get(key) is not None for key in _QUALITY_METRIC_KEYS)
    if isinstance(readiness, dict) and "status" in readiness:
        out = {
            "quality_analyzed": bool(readiness.get("eligible")),
            "analysis_status": readiness.get("status"),
            "analysis_reasons": list(readiness.get("reasons") or []),
            "analysis_explanation": readiness.get("explanation"),
        }
    else:
        error = readiness.get("error") if isinstance(readiness, dict) else None
        out = {
            "quality_analyzed": False,
            "analysis_status": "unknown",
            "analysis_reasons": ["readiness_unavailable"],
            "analysis_explanation": error or READINESS_UNCHECKED,
        }
    out["quality_preliminary"] = has_scores and not out["quality_analyzed"]
    return out


def fetch_readiness(client, photo_ids):
    """Readiness rows by photo id, batched; a failed batch records its error.

    A photo the app did not report (no longer visible, say) gets an error row
    too, so no caller can mistake silence for completed analysis.
    """
    ids = list(dict.fromkeys(pid for pid in photo_ids if isinstance(pid, int)))
    rows = {}
    for start in range(0, len(ids), MAX_READINESS_IDS):
        chunk = ids[start:start + MAX_READINESS_IDS]
        try:
            data = client.post_json(READINESS_PATH, {"photo_ids": chunk})
        except VireoError as err:
            rows.update({pid: {"error": str(err)} for pid in chunk})
            continue
        for row in data.get("photos") or []:
            if isinstance(row, dict) and row.get("photo_id") in chunk:
                rows[row["photo_id"]] = row
        for pid in chunk:
            rows.setdefault(pid, {"error": "Vireo reported no analysis readiness for this photo."})
    return rows


def photo_summary(photo, readiness=None):
    """The fields a reader needs to recognise and judge a photo.

    ``readiness`` is this photo's row from ``fetch_readiness``; without one
    the summary says the analysis state is unknown rather than guessing.
    """
    out = _pick(photo, _PHOTO_SUMMARY_KEYS)
    analysis = analysis_fields(photo, readiness)
    if analysis["analysis_status"] != "unknown":
        # The sentence is get_photo's; a list row keeps the codes, except
        # when the state is unknown and the reader needs to know why.
        del analysis["analysis_explanation"]
    out.update(analysis)
    detections = photo.get("detections")
    if isinstance(detections, list):
        out["subjects_detected"] = len(detections)
    if "edit_recipe" in photo:
        out["has_edits"] = bool(photo.get("edit_recipe"))
    return out


def _exposure_from_metadata(metadata):
    if not isinstance(metadata, dict):
        return {}
    found = {}
    for group in metadata.values():
        if not isinstance(group, dict):
            continue
        for tag in _EXPOSURE_TAGS:
            if tag in group and tag not in found:
                found[tag] = group[tag]
    return found


def summarize_photos(client, photos):
    """Summaries for one page of rows, with one readiness call for the page."""
    photos = [p for p in photos if isinstance(p, dict)]
    readiness = fetch_readiness(client, [p.get("id") for p in photos])
    return [photo_summary(p, readiness.get(p.get("id"))) for p in photos]


def photo_detail(photo, include_exif=False, readiness=None):
    out = photo_summary(photo, readiness)
    out["analysis_explanation"] = analysis_fields(photo, readiness)["analysis_explanation"]
    out.update(_pick(photo, _PHOTO_DETAIL_KEYS))
    if "flag_note" in photo:
        note = photo["flag_note"]
        out["flag_note"] = (
            _pick(note, ("flag", "source", "reason", "description", "at"))
            if isinstance(note, dict) else None
        )
    out.update(_pick(photo, ("species_assignments", "review_workspace_id", "assignment_reviews")))
    out["keywords"] = [
        _pick(k, ("id", "name", "type", "display_name", "synonyms", "taxon_id",
                  "source_taxon_id", "scientific_name")) for k in photo.get("keywords") or []
    ]
    detections = photo.get("detections")
    if isinstance(detections, list):
        out["detections"] = [
            _pick(d, ("id", "category", "confidence", "detector_model"))
            for d in detections
        ]
    metadata = photo.get("metadata")
    out["exposure"] = _exposure_from_metadata(metadata)
    if include_exif:
        out["exif"] = metadata
    return out


def check_recipe(recipe):
    """Refuse a recipe holding a name the app would silently ignore."""
    adjustments = recipe.get("adjustments") or {}
    if not isinstance(adjustments, dict):
        raise VireoError("adjustments must be an object")
    unknown = [key for key in recipe if key not in RECIPE_KEYS]
    unknown += [f"adjustments.{key}" for key in adjustments if key not in ADJUSTMENT_KEYS]
    if unknown:
        raise VireoError(
            f"Vireo has no recipe setting named {unknown}. A recipe holds {list(RECIPE_KEYS)}; "
            f"adjustments holds {list(ADJUSTMENT_KEYS)}."
        )


def job_summary(job):
    """A job without its worker detail; long result lists are cut and counted."""
    progress = job.get("progress")
    errors = job.get("errors")
    errors = errors if isinstance(errors, list) else []
    return {
        **_pick(job, _JOB_KEYS),
        "progress": _pick(progress, _PROGRESS_KEYS) if isinstance(progress, dict) else None,
        "errors": errors[:MAX_JOB_LIST],
        "error_count": len(errors),
        "result": _cut_lists(job.get("result")),
    }


def _cut_lists(value):
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            out[key] = _cut_lists(item)
            if isinstance(item, list) and len(item) > MAX_JOB_LIST:
                out[f"{key}_total"] = len(item)
        return out
    if isinstance(value, list):
        return [_cut_lists(item) for item in value[:MAX_JOB_LIST]]
    return value


def keyword_paths(rows):
    """Each keyword with ``path``: its ancestors joined with `` / ``."""
    by_id = {row["id"]: row for row in rows}
    out = []
    for row in rows:
        names = []
        seen = set()
        cursor = row
        while cursor is not None and cursor["id"] not in seen:
            seen.add(cursor["id"])
            names.append(cursor["name"])
            cursor = by_id.get(cursor.get("parent_id"))
        entry = _pick(row, ("id", "name", "parent_id", "type"))
        entry["path"] = " / ".join(reversed(names))
        out.append(entry)
    return out


# -- the server -----------------------------------------------------------------

def build_server(client: VireoClient):
    """Build the MCP server whose tools call Vireo through ``client``."""
    server = MCPServer(SERVER_NAME, instructions=INSTRUCTIONS)
    _register_catalog_tools(server, client)
    _register_search_tools(server, client)
    _register_photo_tools(server, client)
    _register_burst_tools(server, client)
    _register_focus_tools(server, client)
    _register_species_tools(server, client)
    _register_region_tools(server, client)
    _register_best_tools(server, client)
    _register_edit_tools(server, client)
    _register_save_tools(server, client)
    _register_export_tools(server, client)
    _register_write_tools(server, client)
    return server


def _register_catalog_tools(server, client):
    """Status, workspaces, folders and collections."""

    @server.tool()
    def vireo_status() -> dict:
        """Whether Vireo is running, its app version and build commit,
        the active workspace and how many photos it holds."""
        version = client.get_json("/api/v1/version")
        workspace = client.get_json("/api/workspaces/active", {"include_folders": "0"})
        photos = client.get_json("/api/v1/photos", {"per_page": 1})
        out = {
            "running": True,
            "version": version.get("version"),
            "build_commit": version.get("build_commit"),
            "active_workspace": _pick(workspace, ("id", "name")),
            "photo_count": photos.get("total"),
        }
        return out

    @server.tool()
    def list_workspaces() -> dict:
        """Every workspace with its id and which one is active. All other
        tools act on the active workspace."""
        active = client.get_json("/api/workspaces/active", {"include_folders": "0"})
        rows = client.get_json("/api/v1/workspaces")
        return {
            "workspaces": [
                {**_pick(ws, ("id", "name")), "active": ws.get("id") == active.get("id")}
                for ws in rows
            ]
        }

    @server.tool()
    def activate_workspace(workspace_id: int) -> dict:
        """Switch the active workspace (the Vireo window follows)."""
        client.post_json(f"/api/v1/workspaces/{workspace_id}/activate")
        active = client.get_json("/api/workspaces/active", {"include_folders": "0"})
        return {"active_workspace": _pick(active, ("id", "name"))}

    @server.tool()
    def list_folders(query: str | None = None) -> dict:
        """Folders in the active workspace with their ids, paths and photo
        counts. ``query`` keeps only folders whose path contains it
        (case-insensitive)."""
        rows = client.get_json("/api/folders")
        needle = (query or "").casefold()
        folders = [
            _pick(row, _FOLDER_KEYS) for row in rows
            if not needle or needle in (row.get("path") or "").casefold()
        ]
        return {"folders": folders, "total": len(folders)}

    @server.tool()
    def list_collections() -> dict:
        """Collections (saved filters and albums) in the active workspace."""
        rows = client.get_json("/api/v1/collections")
        collections = []
        for row in rows:
            entry = _pick(row, _COLLECTION_KEYS)
            rules = row.get("rules")
            if isinstance(rules, str):
                with contextlib.suppress(ValueError):
                    rules = json.loads(rules)
            entry["rules"] = rules
            collections.append(entry)
        return {"collections": collections}


def _register_search_tools(server, client):
    """Finding keywords and photos."""

    @server.tool()
    def search_keywords(query: str = "", keyword_type: str | None = None, limit: int = 50) -> dict:
        """Find keywords (species, places, people, anything tagged) by a
        case-insensitive substring of the name, among the keywords used by
        photos in the active workspace and their ancestors. Returns each
        with its ancestor ``path``. An empty query lists the top-level ones.
        ``keyword_type`` narrows to one keyword type (e.g. ``taxonomy``,
        ``location``, ``general``)."""
        rows = keyword_paths(client.get_json("/api/keywords"))
        needle = (query or "").casefold()
        matches = [
            row for row in rows
            if (needle in row["name"].casefold() if needle else row.get("parent_id") is None)
            and (keyword_type is None or row.get("type") == keyword_type)
        ]
        limit = max(1, min(limit, 500))
        return {"keywords": matches[:limit], "total_matches": len(matches)}

    @server.tool()
    def search_photos(
        folder_id: int | None = None,
        collection_id: int | None = None,
        keyword: str | None = None,
        rating_min: int | None = None,
        flag: str | None = None,
        color_label: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        sort: str = "date",
        page: int = 1,
        per_page: int = DEFAULT_PAGE_SIZE,
    ) -> dict:
        """Photos in the active workspace matching the Browse filters.
        ``keyword`` matches a keyword name substring (a species name works);
        ``flag`` is ``none``, ``flagged`` or ``rejected``; dates are ISO
        (``2024-05-01``). Paged: ``total`` is the full match count."""
        per_page = max(1, min(per_page, MAX_PAGE_SIZE))
        data = client.get_json("/api/photos", {
            "folder_id": folder_id,
            "collection_id": collection_id,
            "keyword": keyword,
            "rating_min": rating_min,
            "flag": flag,
            "color_label": color_label,
            "date_from": date_from,
            "date_to": date_to,
            "sort": sort,
            "page": page,
            "per_page": per_page,
        })
        return {
            "photos": summarize_photos(client, data.get("photos", [])),
            "total": data.get("total"),
            "page": data.get("page"),
            "per_page": data.get("per_page"),
        }

    @server.tool()
    def find_photos_by_text(
        query: str,
        limit: int = 20,
        folder_id: int | None = None,
        collection_id: int | None = None,
    ) -> dict:
        """Search photos by describing them (CLIP text search over the
        active model's embeddings). ``reason`` explains an empty result the
        model cannot serve (no model, no embeddings, a classifier without
        text search)."""
        data = client.get_json("/api/photos/search", {
            "q": query,
            "limit": max(1, min(limit, MAX_PAGE_SIZE)),
            "folder_id": folder_id,
            "collection_id": collection_id,
        })
        items = [item for item in data.get("results", []) if isinstance(item, dict)]
        summaries = summarize_photos(client, [item.get("photo") or {} for item in items])
        results = []
        for item, entry in zip(items, summaries, strict=True):
            entry.update({k: v for k, v in item.items() if k != "photo"})
            results.append(entry)
        out = {
            "results": results,
            "total_matches": data.get("total_matches"),
            "model_used": data.get("model_used"),
        }
        if data.get("reason"):
            out["reason"] = data["reason"]
        return out


def _register_photo_tools(server, client):
    """One photo: its detail, its pixels and its burst."""

    @server.tool()
    def get_photo(photo_id: int, include_exif: bool = False) -> dict:
        """One photo in full: file paths, keywords, species, location,
        detections, exposure settings, quality fields and the analysis
        readiness behind them (quality_analyzed, analysis_status,
        analysis_reasons, analysis_explanation, quality_preliminary).
        ``include_exif`` adds the complete EXIF dump."""
        photo = client.get_json(f"/api/photos/{photo_id}")
        pid = photo.get("id", photo_id)
        return photo_detail(photo, include_exif, fetch_readiness(client, [pid]).get(pid))

    @server.tool()
    def view_photo(photo_id: int, view: str = "preview", detection_id: int | None = None) -> Image:
        """Look at a photo. ``view`` is ``preview`` (the frame at 1920 px,
        for composition, pose and light), ``thumbnail`` (small and fast, for
        composition only), ``source`` (the whole upright frame without
        edits, for choosing a focus_crop center) or ``subject`` (the
        detected animal cropped from the frame, for identification). Supply ``detection_id`` for
        the exact subject from get_species_evidence; it is valid only with
        view="subject". Subject crops are identification views; use focus_crop to
        judge focus."""
        if detection_id is not None and view != "subject":
            raise VireoError("detection_id requires view='subject'")
        if view == "preview":
            content_type, body = client.get_bytes(
                f"/photos/{photo_id}/preview", {"size": PREVIEW_SIZE}
            )
        elif view == "source":
            content_type, body = client.get_bytes(
                f"/photos/{photo_id}/preview", {"size": PREVIEW_SIZE, "view": "source"}
            )
        elif view == "thumbnail":
            content_type, body = client.get_bytes(f"/thumbnails/{photo_id}.jpg")
        elif view == "subject":
            content_type, body = client.get_bytes(
                f"/photos/{photo_id}/crop", {"detection_id": detection_id}
            )
        else:
            raise VireoError("view must be 'preview', 'thumbnail', 'source' or 'subject'")
        fmt = "png" if "png" in (content_type or "") else "jpeg"
        return Image(data=body, format=fmt)


def _register_burst_tools(server, client):
    """Walking a folder's bursts and reading their rankings."""
    read_only = mcp_types.ToolAnnotations(read_only_hint=True)

    @server.tool(annotations=read_only)
    def list_bursts(
        folder_id: int, reasons: list[str] | None = None, unflagged_only: bool = False,
        offset: int = 0, limit: int = DEFAULT_BURST_PAGE,
    ) -> dict:
        """The bursts in one folder (not its subfolders), and why each needs
        a reviewer. Pass a burst's ``seed_photo_id`` to ``burst`` to see its
        frames. ``coverage`` says how many of the folder's photos have each
        kind of analysis, and ``reason_counts`` counts bursts by reason.

        ``reasons`` keeps bursts with any of these codes:
        unanalyzed_frames (some frames have no current quality analysis),
        close_call (the top two are nearly as sharp), no_eye_located,
        subject_touches_edge, multiple_subjects (the wrong animal may have
        been scored), all_rejected, invalid_subject_mask.
        positive_keep_not_validated is on every ranked burst, because Vireo
        names no keeper itself. A burst's reasons and flags describe the
        ``ranked_frames`` (at most 120) of its ``frames``. ``unflagged_only`` keeps bursts whose ranked
        frames have no pick or reject yet; list every page before flagging,
        since a burst that gains a flag drops out and moves the offsets.

        ``limit`` is at most 200; ``next_offset`` continues the list. Every
        call ranks the whole folder again, which takes several seconds for
        thousands of photos, so prefer one long page to many short ones.
        Folders over 5000 photos are refused.
        """
        unknown = sorted(set(reasons or ()) - set(BURST_REASONS))
        if unknown:
            raise VireoError(f"unknown reasons {unknown}; use any of {list(BURST_REASONS)}")
        data = client.get_json(f"/api/v1/folders/{folder_id}/bursts")
        bursts = data.get("bursts") or []
        counts: dict[str, int] = {}
        for item in bursts:
            for code in _burst_reasons(item):
                counts[code] = counts.get(code, 0) + 1
        matching = [
            item for item in bursts
            if (not reasons or set(reasons) & set(_burst_reasons(item)))
            and (not unflagged_only or _burst_unflagged(item))
        ]
        offset, limit = max(0, offset), max(1, min(limit, MAX_BURST_PAGE))
        page = matching[offset:offset + limit]
        more = offset + len(page) < len(matching)
        return {
            **_pick(data, ("folder", "photo_count", "coverage", "burst_count", "photos_in_bursts")),
            "reason_counts": counts,
            "matching": len(matching),
            "offset": offset,
            "next_offset": offset + len(page) if more else None,
            "bursts": [_burst_entry(item) for item in page],
        }

    @server.tool(annotations=read_only)
    def burst(
        photo_id: int, max_photos: int = DEFAULT_RANKED_FRAMES,
        offset: int = 0, limit: int = DEFAULT_FRAME_PAGE,
    ) -> dict:
        """The burst of frames shot around a photo, ranked by Vireo's
        quality analysis. A provisional ranked candidate still needs manual
        review; ranking alone does not establish an automatic keeper. A frame
        ranks only when its subject analysis is complete: in a scored burst
        each frame carries selection_eligible, analysis_status and
        analysis_reasons. An ineligible frame's quality fields are null.

        ``review_reasons`` says why the ranking should not be taken at face
        value, ``coverage`` how many frames have each kind of analysis.
        ``scored`` is false, with ``unscored_reason``, when no ranking was
        possible; the frames are then listed unranked.

        The ranking, ``coverage`` and ``review_reasons`` cover up to
        ``max_photos`` frames around the photo (at most 500), and ``count``
        says how many. Frames past that are not considered, so for a burst
        list_bursts reports with more ``frames``, pass that number.
        ``frames`` holds ``limit`` of them in rank order, unranked last;
        ``next_offset`` continues."""
        data = client.get_json(
            f"/api/v1/photos/{photo_id}/burst-evidence", {"max_photos": max_photos},
        )
        cards = data.get("cards") or []
        offset, limit = max(0, offset), max(1, min(limit, MAX_BURST_PAGE))
        page = cards[offset:offset + limit]
        more = offset + len(page) < len(cards)
        return {
            **_pick(data, _BURST_KEYS),
            "review_reasons": (data.get("ambiguity") or {}).get("reasons") or [],
            "offset": offset,
            "next_offset": offset + len(page) if more else None,
            "frames": [_burst_frame(card) for card in page],
        }


def _register_focus_tools(server, client):
    """Unresized pixels for judging focus."""

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=True))
    def focus_crop(
        photo_id: int, center_x: float | None = None, center_y: float | None = None,
        size: int = DEFAULT_FOCUS_SIZE, source: str = "auto", mask: bool = False,
        detection_id: int | None = None,
    ) -> list:
        """Unresized pixels around the animal, for judging focus. Returns
        facts about the crop, then the image. Read ``notes`` first: they say
        when the crop misses the head or shows a downsized copy.

        By default the crop centers on the head when the analysis located
        it, otherwise on the subject box. To aim it, give ``center_x`` and
        ``center_y`` as fractions (0 to 1) of the unedited frame, which
        view_photo(view="source") shows; the edited preview may be cropped or
        rotated. ``detection_id`` picks another animal in the photo.
        ``size`` is the crop's side in pixels (128 to 1024). ``source`` is
        ``auto``, ``working`` or ``original``. ``mask`` darkens everything
        outside the subject mask, to show which animal was scored.
        """
        if (center_x is None) != (center_y is None):
            raise VireoError("give both center_x and center_y")
        if center_x is not None and detection_id is not None:
            raise VireoError("give a center or a detection_id, not both")
        headers, body = client.get_with_headers(f"/api/v1/photos/{photo_id}/focus-crop", {
            "size": size,
            "source": source,
            "center": None if center_x is None else f"{center_x},{center_y}",
            "mask": "1" if mask else None,
            "detection_id": detection_id,
            # The route reads a bare detection_id as a request for saved
            # comparison evidence; the head basis inspects any animal.
            "basis": "head" if detection_id is not None else None,
        })
        return [focus_crop_facts(headers), Image(data=body, format="jpeg")]


def _species_assignment_payload(photo_ids, species, provenance):
    """Require attributed evidence on the agent's reviewed-assignment path."""
    reviewer = provenance.get("reviewer")
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise VireoError("provenance.reviewer must identify the actual reviewer")
    method = provenance.get("method")
    if not isinstance(method, str) or method not in {"direct_visual", "adjacent_frames"}:
        raise VireoError("provenance.method must be direct_visual or adjacent_frames after reviewing evidence")
    if not any(isinstance(provenance.get(key), str) and provenance[key].strip()
               for key in ("reason", "evidence_source")):
        raise VireoError("provenance.reason or evidence_source must describe the identification evidence")
    return {"photo_ids": photo_ids, "source_photo_ids": photo_ids,
            "species": species, "provenance": provenance, "add": True,
            "preserve_existing_provenance": True, "preserve_keyword_metadata": True,
            "response_mode": "compact"}


def _register_species_tools(server, client):
    """Identification evidence and deliberate, token-guarded additive edits."""

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=True))
    def get_species_evidence(photo_ids: list[int]) -> dict:
        """Read identification evidence for 1–100 exact, unique photo IDs.

        Includes every subject's model candidates and uncertainty, bound crop
        URLs, saved assignment provenance, current species-list defaults and
        stored run sources. Candidate selections are unverified predictions;
        they do not establish continuity or authorize group-wide propagation.
        No analysis, classification, keyword or flag writes are performed.
        """
        return client.post_json("/api/photos/species-evidence", {"photo_ids": photo_ids})

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=True))
    def list_assignment_conflicts() -> dict:
        """List saved species assignments with outstanding conflicts in the active workspace."""
        return client.get_json("/api/species/assignment-conflicts")

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=False, destructive_hint=True))
    def review_species_assignment(photo_id: int, keyword_id: int, observed_workspace_id: int,
                                  review_version: str, action: str, reviewer: str, reason: str,
                                  evidence_source: str, species: str | None = None) -> dict:
        """Report or resolve conflicting evidence on one saved assignment, with Undo.

        Use the review_workspace_id and assignment review_version from get_photo.
        Report with action='report'. Resolve with 'retain', 'remove', or 'amend'
        and a replacement species. Attribute the decision to the actual reviewer.
        """
        return client.post_json(f"/api/photos/{photo_id}/species/{keyword_id}/review", {
            "observed_workspace_id": observed_workspace_id, "review_version": review_version,
            "action": action, "reviewer": reviewer, "reason": reason, "evidence_source": evidence_source,
            "species": species,
        })

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=True))
    def preview_species_assignment(photo_ids: list[int], species: str, provenance: dict) -> dict:
        """Preview adding one reviewed species to exact photo IDs, without writing.

        provenance requires method (direct_visual or adjacent_frames for a
        visual review), reviewer and evidence: reason, evidence_source and
        supporting_photo_ids. Model predictions remain suggestions until reviewed.
        A selection may span cached encounters. Inspect every proposed
        addition or removal before applying the selection as one decision.
        """
        body = _species_assignment_payload(photo_ids, species, provenance)
        return client.post_json("/api/encounters/species", {**body, "preview": True})

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=False, destructive_hint=False))
    def apply_species_assignment(
        photo_ids: list[int], species: str, provenance: dict, preview_token: str,
    ) -> dict:
        """Apply the exact previously inspected species preview, with Undo.

        Reuse unchanged photo IDs, species and provenance from the preview.
        A stale token returns a conflict; obtain and inspect a fresh preview.
        Adds the species without removing existing labels, changing flags or
        extending to encounter siblings. Read back species_assignments afterward.
        """
        if not preview_token:
            raise VireoError("preview_token from preview_species_assignment is required")
        body = _species_assignment_payload(photo_ids, species, provenance)
        return client.post_json("/api/encounters/species", {**body, "preview_token": preview_token})


def _register_region_tools(server, client):
    """Subject verdicts: reject a detector box as background, or retain it again."""

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=False, destructive_hint=False))
    def reject_region(detection_id: int, reason: str, source: str | None = None) -> dict:
        """Reject one detected region as not an animal, after viewing it.

        Only for a region you inspected with view_photo(view="subject",
        detection_id=...) and the frame and found to hold no animal (a
        stump, branch or rock the detector boxed and the classifier then
        ranked species inside). The region's predictions stop counting as
        evidence in every workspace, review and grouping; nothing is
        deleted, the photo's next box becomes primary, and Vireo records
        one Undo step with your ``reason`` (required) and ``source``.
        This neither tags a species nor touches any other photo.
        """
        if not isinstance(reason, str) or not reason.strip():
            raise VireoError("reason must say what the region actually shows")
        body = {"verdict": "not_animal", "reason": reason}
        if source:
            body["source"] = source
        return _verdict_result(client.post_json(f"/api/detections/{detection_id}/subject-verdict", body))

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=False, destructive_hint=False))
    def restore_region(detection_id: int, reason: str | None = None, source: str | None = None) -> dict:
        """Retain a previously rejected region as a subject again (one Undo step)."""
        body = {"verdict": None}
        if reason:
            body["reason"] = reason
        if source:
            body["source"] = source
        return _verdict_result(client.post_json(f"/api/detections/{detection_id}/subject-verdict", body))


def _verdict_result(data):
    """Say what changed, with the photo's remaining and rejected regions."""
    subjects = data.get("subjects") or {}
    return {
        **_pick(data, ("photo_id", "detection_id", "verdict", "previous_verdict", "changed", "warning")),
        "primary_detection_id": subjects.get("primary_detection_id"),
        "retained_detection_ids": [s["id"] for s in subjects.get("subjects", [])],
        "rejected_detection_ids": [s["id"] for s in subjects.get("rejected", [])],
    }


def _date_rules(date_from, date_to):
    """A universal-filter rule tree for an ISO date range, or None."""
    if date_from and date_to:
        return [{"field": "timestamp", "op": "between", "value": [date_from, date_to]}]
    if date_from:
        return [{"field": "timestamp", "op": ">=", "value": date_from}]
    if date_to:
        return [{"field": "timestamp", "op": "<=", "value": date_to}]
    return None


def _register_best_tools(server, client):
    """The best photo of each species in a scope, and the choice behind it."""

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=True))
    def best_of_each_species(
        folder_id: int | None = None,
        collection_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        species: str | None = None,
        per_species: int = 1,
        include_unconfirmed: bool = False,
    ) -> dict:
        """The best photos of each species in a folder (a trip), a
        collection, an ISO date range (``2025-01-01`` to ``2025-12-31`` for a
        year) or, with no scope, the whole workspace; scopes combine.

        Each species lists ``per_species`` photos, best first, and a
        ``decision`` for the first one: ``kind`` (``highlight``, ``tie``,
        ``pick``, ``score``, ``unanalyzed``), who chose it and why,
        ``tied_photo_ids`` for a tie and ``contender_ids`` (every highlight
        of the species here, best first). ``meta`` counts the species led by
        a settled highlight (``highlighted``), the ones that need a
        comparison and the ones Vireo guessed. Species held only
        by an unconfirmed prediction are left out and counted in
        ``meta.unconfirmed_species_omitted`` unless ``include_unconfirmed``.
        """
        rules = _date_rules(date_from, date_to)
        return client.get_json("/api/species-best", {
            "folder_id": folder_id,
            "collection_id": collection_id,
            "rules": json.dumps(rules) if rules else None,
            "species": species,
            "per_species": per_species,
            "include_unconfirmed": "1" if include_unconfirmed else None,
        })

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=False, destructive_hint=False))
    def choose_best_photo(
        species: str,
        photo_id: int,
        reason: str,
        over: list[int] | None = None,
        folder_id: int | None = None,
        collection_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        source: str | None = None,
    ) -> dict:
        """Record that ``photo_id`` is the best photo of ``species`` among
        photos you compared, after viewing them at full detail.

        The photo becomes a highlight of the species, ahead of the photos
        named. ``over`` lists the photo ids it beat. Without ``over``, it
        goes ahead of every other highlight of the species in the scope given
        (folder, collection, dates; none means the whole workspace). Send
        ``over`` or a scope, not both. The photo must already carry the
        species as a confirmed identification. ``reason`` (required, up to
        300 characters) says what made it the best and ``source`` (up to 60)
        who decided; both are shown with the photo wherever it leads.
        ``superseded`` lists older comparisons this choice reversed.
        """
        if not isinstance(reason, str) or not reason.strip():
            raise VireoError("reason must say what makes this photo the best")
        body: dict[str, object] = {"species": species, "photo_id": photo_id, "reason": reason}
        if over is not None:
            body["over"] = over
        if folder_id is not None:
            body["folder_id"] = folder_id
        if collection_id is not None:
            body["collection_id"] = collection_id
        rules = _date_rules(date_from, date_to)
        if rules:
            body["rules"] = rules
        if source:
            body["source"] = source
        data = client.post_json("/api/species-best/choose", body)
        return _pick(data, ("choice_id", "species", "photo_id", "over", "source", "reason", "superseded"))


def _register_edit_tools(server, client):
    """Reading, suggesting and looking at edits; none of these saves anything."""
    read_only = mcp_types.ToolAnnotations(read_only_hint=True)

    @server.tool(annotations=read_only)
    def get_edit(photo_id: int) -> dict:
        """The photo's saved edit and its virtual copies and snapshots.
        ``recipe`` is null when the photo is unedited."""
        data = client.get_json(f"/api/photos/{photo_id}/edit-recipe")
        versions = client.get_json(f"/api/photos/{photo_id}/versions").get("versions") or []
        return {
            **_pick(data, ("photo_id", "recipe")),
            "versions": [_pick(v, ("id", "name", "kind", "recipe")) for v in versions],
        }

    @server.tool(annotations=read_only)
    def suggest_crop(
        photo_id: int, margin: float = 0.35, aspect: str = "auto", detection_id: int | None = None,
    ) -> dict:
        """A crop around one animal, as a recipe ``crop`` (fractions of the
        unedited upright frame). ``margin`` is the padding on each side as a
        fraction of the animal's box, ``aspect`` is ``auto`` (3:2 or 4:5) or
        ``W:H``, and ``detection_id`` picks an animal other than the primary.
        ``output_pixels`` is the size an export would have."""
        return client.get_json(f"/api/photos/{photo_id}/subject-crop", {
            "margin": margin, "aspect": aspect, "detection_id": detection_id,
        })

    @server.tool(annotations=read_only)
    def suggest_tone(photo_id: int, recipe: dict | None = None, style: str = "subject") -> dict:
        """Tone values Vireo fits to the photo as framed by ``recipe`` (its
        crop matters, its tone does not), for a recipe's ``adjustments``.
        ``style`` is ``subject`` (meters the animal), ``balanced`` or
        ``gentle``. The notes say what was metered."""
        check_recipe(recipe or {})
        return client.get_json(f"/api/photos/{photo_id}/auto-tone", {
            "recipe": json.dumps(recipe or {}), "style": style,
        })

    @server.tool(annotations=read_only)
    def view_edit(
        photo_id: int, recipe: dict, size: int = DEFAULT_EDIT_SIZE, whole_frame: bool = False,
    ) -> list:
        """Render a recipe without saving it. Returns where the pixels came
        from, then the image. ``size`` is the long edge (256 to 2048);
        ``whole_frame`` ignores the recipe's crop. To check sharpening halos
        and noise at 1:1, use a crop as many pixels wide as ``size``.

        A recipe holds ``crop`` (``x``, ``y``, ``w``, ``h``) and
        ``adjustments`` such as exposure, highlights, shadows, whites, blacks,
        contrast, texture, clarity, sharpen, sharpen_radius and
        noise_reduction."""
        check_recipe(recipe)
        headers, body = client.get_with_headers(f"/photos/{photo_id}/edit-preview", {
            "recipe": json.dumps(recipe),
            "size": max(MIN_EDIT_SIZE, min(size, MAX_EDIT_SIZE)),
            "apply_crop": None if whole_frame else "1",
        })
        facts = {"photo_id": photo_id, "pixels_from": headers.get("X-Vireo-Preview-Origin")}
        return [facts, Image(data=body, format="jpeg")]


def _register_save_tools(server, client):
    """Saving an edit, each one Undo step."""
    writes = mcp_types.ToolAnnotations(read_only_hint=False, destructive_hint=False)

    @server.tool(annotations=writes)
    def save_edit(
        photo_id: int, recipe: dict, reason: str, source: str | None = None,
        replaces: dict | None = None,
    ) -> dict:
        """Save ``recipe`` as the photo's edit. Refused when the photo already
        has one, because that edit is the user's: use save_edit_copy instead.
        To revise an edit you saved yourself, pass the recipe that save
        returned as ``replaces``; it is refused if the edit changed since.
        ``reason`` says what the edit is for and ``source`` who made it; both
        show in Vireo's history. Vireo queues the edit for its XMP sidecar
        like any other."""
        if not recipe:
            raise VireoError("recipe is empty; save_edit does not clear edits")
        check_recipe(recipe)
        if not isinstance(reason, str) or not reason.strip():
            raise VireoError("reason must say what the edit is for")
        current = client.get_json(f"/api/photos/{photo_id}/edit-recipe").get("recipe")
        if current is not None and replaces is None:
            raise VireoError(
                f"Photo {photo_id} already has an edit. Export it as it is, or keep yours "
                "beside it with save_edit_copy."
            )
        if replaces is not None and current != replaces:
            raise VireoError(f"Photo {photo_id}'s edit is not the one in replaces. Read it with get_edit.")
        body = {"recipe": recipe, "expected_recipe": replaces, "description": "Saved photo edit",
                "reason": reason}
        if source:
            body["source"] = source
        data = client.put_json(f"/api/photos/{photo_id}/edit-recipe", body)
        if data.get("recipe") is None:
            raise VireoError("Every value in the recipe is neutral, so the photo has no edit.")
        return {"photo_id": photo_id, "recipe": data["recipe"]}

    @server.tool(annotations=writes)
    def save_edit_copy(photo_id: int, name: str, recipe: dict) -> dict:
        """Save ``recipe`` as a named virtual copy, leaving the photo's own
        edit unchanged. Export it alone with preview_export's
        ``version_id``."""
        if not recipe:
            raise VireoError("recipe is empty")
        check_recipe(recipe)
        data = client.post_json(f"/api/photos/{photo_id}/versions", {"name": name, "recipe": recipe})
        version = data.get("version") or {}
        return {"photo_id": photo_id, "version_id": version.get("id"), "name": version.get("name"),
                "recipe": data.get("recipe")}


def _export_body(client, photo_ids, preset, destination, subfolder, include_location, max_size, version_id):
    """The export request for these choices over the preset's settings."""
    presets = {p["name"]: p["settings"] for p in client.get_json("/api/export/presets")["presets"]}
    if preset is not None and preset not in presets:
        raise VireoError(f"no export preset named {preset!r}; saved presets: {sorted(presets)}")
    body = dict(presets[preset]) if preset is not None else {}
    fields = [field for field in body.get("metadata_fields") or [] if field != "location"]
    body["metadata_fields"] = [*fields, "location"] if include_location else fields
    if destination is not None:
        body["destination"] = os.path.expanduser(destination)
    if subfolder is not None:
        body.update(export_to_subfolder=True, subfolder_name=subfolder)
    if max_size is not None:
        body["max_size"] = max_size
    body.update(photo_ids=list(dict.fromkeys(photo_ids)), list_files=True)
    if version_id is not None:
        quoted = urllib.parse.quote(version_id, safe="")
        version = client.get_json(f"/api/photos/{photo_ids[0]}/versions/{quoted}")["version"]
        body.update(version_id=version_id, version_revision=version["revision"])
    return body


def _preflight_export(client, body):
    """The app's preflight for ``body`` and a fingerprint of all it depends on."""
    data = client.post_json("/api/jobs/export/preflight", body)
    if "files" not in data:
        raise VireoError(OUTDATED_APP_MESSAGE.format(what="list an export's files"))
    workspace = client.get_json("/api/workspaces/active", {"include_folders": "0"}).get("id")
    state = json.dumps([workspace, body, data], sort_keys=True)
    return data, hashlib.sha256(state.encode()).hexdigest()


def _export_preview(body, data, token):
    files = data["files"]
    return {
        "token": token,
        "destination": body.get("destination") or "beside each original",
        "destination_folders": data.get("destination_folders"),
        "settings": _pick(body, ("format", "quality", "max_size", "naming_template",
                                 "export_to_subfolder", "subfolder_name", "reveal_after_export")),
        "metadata": data["metadata"],
        "file_count": len(files),
        "edited_count": sum(1 for f in files if f["edit_key"]),
        "with_coordinates": sum(1 for f in files if f["has_coordinates"]),
        "without_coordinates": [f["photo_id"] for f in files if not f["has_coordinates"]],
        "skipped": data["skipped"],
        "renamed": data["renames"],
        "files": [{"photo_id": f["photo_id"], "path": f["path"], "edited": bool(f["edit_key"])} for f in files],
    }


def _register_export_tools(server, client):
    """Exporting files, previewed first, and following the job."""
    pending: dict[str, tuple[dict, str]] = {}

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=True))
    def list_export_presets() -> dict:
        """The user's saved export presets and their settings. An empty
        ``destination`` means beside each original."""
        return _pick(client.get_json("/api/export/presets"), ("presets",))

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=True))
    def preview_export(
        photo_ids: list[int], preset: str | None = None, destination: str | None = None,
        subfolder: str | None = None, include_location: bool = True, max_size: int | None = None,
        version_id: str | None = None,
    ) -> dict:
        """What an export would write, and the ``token`` start_export needs.
        Nothing is written.

        ``preset`` starts from a saved preset's settings; presets are not
        applied otherwise. ``destination`` is an absolute folder (``~`` works)
        and overrides the preset's; ``subfolder`` adds one folder inside it.
        Location metadata is included unless ``include_location`` is false,
        and ``without_coordinates`` lists photos that have none to include.
        ``max_size`` limits the long edge in pixels. ``version_id`` exports a
        virtual copy and takes exactly one photo.

        Existing files are never overwritten: ``renamed`` lists the names that
        would change instead. ``skipped`` lists photos that would not be
        written, and why. At most 500 photos."""
        if not photo_ids:
            raise VireoError("give at least one photo id")
        if version_id is not None and len(photo_ids) != 1:
            raise VireoError("version_id exports one photo at a time")
        body = _export_body(client, photo_ids, preset, destination, subfolder,
                            include_location, max_size, version_id)
        data, fingerprint = _preflight_export(client, body)
        if not data["files"]:
            raise VireoError(f"No photo would be written: {data['skipped']}")
        token = secrets.token_urlsafe(16)
        pending[token] = (body, fingerprint)
        while len(pending) > MAX_PENDING_EXPORTS:
            del pending[next(iter(pending))]
        return _export_preview(body, data, token)

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=False, destructive_hint=False))
    def start_export(token: str) -> dict:
        """Run exactly the export a preview_export described. Refused when
        anything it depends on changed since (an edit, a location, the
        workspace, a file in the way); preview again then. A token works once.
        Returns the ``job_id`` to follow with get_job."""
        if token not in pending:
            raise VireoError("That token is unknown or already used. Call preview_export again.")
        body, fingerprint = pending.pop(token)
        _, current = _preflight_export(client, body)
        if current != fingerprint:
            raise VireoError("The export would differ from the preview now. Call preview_export again.")
        data = client.post_json("/api/jobs/export", body)
        return {"job_id": data.get("job_id"), "photo_count": len(body["photo_ids"])}

    @server.tool(annotations=mcp_types.ToolAnnotations(read_only_hint=True))
    def get_job(job_id: str, wait_seconds: int = 0) -> dict:
        """A background job's status, progress, errors and result.
        ``wait_seconds`` (up to 50) waits that long for it to finish; call
        again while ``status`` is not completed, failed or cancelled. A failed
        job can still have done part of its work: read ``result``."""
        path = f"/api/jobs/{urllib.parse.quote(job_id, safe='')}"
        deadline = time.monotonic() + max(0, min(wait_seconds, MAX_JOB_WAIT))
        job = client.get_json(path)
        while job.get("status") not in TERMINAL_JOB_STATUSES and time.monotonic() < deadline:
            time.sleep(JOB_POLL_SECONDS)
            job = client.get_json(path)
        return job_summary(job)


def _register_write_tools(server, client):
    """The catalog writes; each is one Undo step in Vireo."""

    @server.tool()
    def set_flag(
        photo_ids: list[int], flag: str,
        source: str | None = None, reason: str | None = None,
    ) -> dict:
        """Flag photos: ``flagged`` (a pick), ``rejected`` or ``none``.

        Optional ``source`` names who decided (up to 60 characters), and
        ``reason`` explains why (up to 300). The app validates and records
        them in the undoable edit and the current photo's ``flag_note``.
        """
        body: dict[str, object] = {"photo_ids": photo_ids, "flag": flag}
        if source is not None:
            body["source"] = source
        if reason is not None:
            body["reason"] = reason
        data = client.post_json("/api/batch/flag", body)
        return {"updated": data.get("updated"), "flag": flag}

    @server.tool()
    def set_rating(photo_ids: list[int], rating: int) -> dict:
        """Set the star rating (0 to 5) on photos."""
        data = client.post_json("/api/batch/rating", {"photo_ids": photo_ids, "rating": rating})
        return {"updated": data.get("updated"), "rating": rating}

    @server.tool()
    def add_keyword(
        photo_ids: list[int], name: str | None = None, keyword_id: int | None = None
    ) -> dict:
        """Tag photos with a keyword by ``keyword_id`` (from search_keywords)
        or by ``name``, which creates the keyword if it does not exist."""
        body: dict[str, object] = {"photo_ids": photo_ids}
        if keyword_id is not None:
            body["keyword_id"] = keyword_id
        elif name:
            body["name"] = name
        else:
            raise VireoError("give a keyword name or keyword_id")
        data = client.post_json("/api/batch/keyword", body)
        return {"updated": data.get("updated"), "keyword_id": data.get("keyword_id")}

    @server.tool()
    def remove_keyword(photo_ids: list[int], keyword_id: int) -> dict:
        """Remove a keyword from photos."""
        return client.post_json(
            "/api/batch/keyword-remove", {"photo_ids": photo_ids, "keyword_id": keyword_id}
        )


# -- connection info for clients -----------------------------------------------

def server_command():
    """The argv that starts this MCP server for this install.

    A PyInstaller build is the sidecar binary itself; a checkout runs
    ``app.py`` with the interpreter in use.
    """
    if getattr(sys, "frozen", False):
        return [sys.executable, "--mcp"]
    app_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py")
    return [sys.executable, app_path, "--mcp"]


def _shell_command(command):
    """Copyable argv for POSIX shells or Windows PowerShell."""
    if sys.platform == "win32":
        return "& " + " ".join("'" + arg.replace("'", "''") + "'" for arg in command)
    return shlex.join(command)


def connect_info():
    """Client setup for this install; existing Claude fields stay available."""
    command = server_command()
    return {
        "command": command,
        "shell": "PowerShell" if sys.platform == "win32" else "a terminal",
        "claude_code": _shell_command(["claude", "mcp", "add", SERVER_NAME, "--", *command]),
        "claude_desktop": {
            "mcpServers": {SERVER_NAME: {"command": command[0], "args": command[1:]}}
        },
        "codex": {
            "command": _shell_command(["codex", "mcp", "add", SERVER_NAME, "--", *command]),
            "config_toml": (
                f"[mcp_servers.{SERVER_NAME}]\n"
                f"command = {json.dumps(command[0], ensure_ascii=False)}\n"
                f"args = {json.dumps(command[1:], ensure_ascii=False)}\n"
            ),
        },
    }


def main():
    """Serve MCP over stdio until the client closes the pipe."""
    build_server(VireoClient()).run(transport="stdio")


if __name__ == "__main__":
    main()
