"""Conservative Lensfun database matching and radial calibration interpolation.

Model formulas and normalization follow LightCraft's MIT-licensed lensdb
implementation (Copyright 2026 ArtCraft Team and the LightCraft contributors).
See THIRD_PARTY_NOTICES.md. The bundled, unmodified Lensfun XML data has its
own CC-BY-SA-3.0 license and provenance in data/lensfun/.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import xml.etree.ElementTree as ET
from functools import lru_cache
from pathlib import Path

import numpy as np

_DATA = Path(__file__).parent / "data" / "lensfun"
_REVISION = "052c3ec6380aaa172caa6d7ea3a5eaafe0b052da"
_RAW_EXTENSIONS = {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
log = logging.getLogger(__name__)


def _value(photo, name):
    try:
        return photo[name]
    except (KeyError, IndexError, TypeError):
        return None


def _positive(value):
    try:
        result = float(value) if not isinstance(value, bool) else 0
    except (ValueError, TypeError, OverflowError):
        return None
    return result if math.isfinite(result) and 0 < result < 1e6 else None


def _maker(value):
    words = re.findall(r"[a-z]+", str(value or "").casefold())
    first = words[0] if words else ""
    return {"omsystem": "olympus", "om": "olympus"}.get(first, first)


def _camera_key(make, model):
    key = re.sub(r"[^a-z0-9]", "", str(model or "").casefold())
    return _maker(make), key.removeprefix(_maker(make))


def _lens_key(value, maker):
    words = re.findall(r"[a-z]+|\d+(?:\.\d+)?", str(value or "").casefold())
    # Punctuation, spacing, case, an optional maker and the f/ marker vary
    # between EXIF and the database. All other words/numbers must agree.
    return tuple(sorted(format(float(word), ".12g") if word[0].isdigit() else word
                        for word in words if word not in {maker, "f"}))


def lens_metadata(photo=None):
    raw = _value(photo, "exif_data") or _value(photo, "metadata") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            raw = {}
    exif = {}
    if isinstance(raw, dict):
        exif.update(raw)
        for group in ("TIFF", "IFD0", "ExifIFD", "EXIF", "Composite"):
            if isinstance(raw.get(group), dict):
                exif.update(raw[group])
    focal = _positive(_value(photo, "focal_length") or exif.get("FocalLength"))
    crop = _positive(exif.get("ScaleFactor35efl"))
    equivalent = _positive(exif.get("FocalLengthIn35mmFormat"))
    return {
        "make": str(_value(photo, "camera_make") or exif.get("Make") or ""),
        "camera": str(_value(photo, "camera_model") or exif.get("Model") or ""),
        "lens": str(exif.get("LensModel") or _value(photo, "lens") or exif.get("LensID") or ""),
        "lens_make": str(exif.get("LensMake") or ""),
        "focal": focal,
        "aperture": _positive(_value(photo, "aperture") or exif.get("FNumber")),
        "distance": _positive(exif.get("FocusDistance") or exif.get("SubjectDistance")),
        "crop": crop or (equivalent / focal if equivalent and focal else None),
        "extension": str(_value(photo, "extension") or Path(str(_value(photo, "filename") or "")).suffix).lower(),
    }


def _aspect(value):
    parts = str(value or "1.5").split(":")
    a = _positive(parts[0])
    b = _positive(parts[1]) if len(parts) == 2 else 1
    if a is None or b is None:
        return None
    ratio = max(a / b, b / a)
    return ratio if ratio <= 10 else None


def _points(calibration, kind):
    result = []
    for node in calibration.findall(kind):
        if not _positive(node.get("focal")):
            continue
        try:
            values = {key: float(value) for key, value in node.attrib.items() if key != "model"}
        except (ValueError, TypeError):
            continue
        if all(math.isfinite(v) for v in values.values()):
            result.append({**values, "model": node.get("model")})
    return result


def _parse_profiles(root, filename):
    profiles = []
    for lens in root.findall("lens"):
        for index, cal in enumerate(lens.findall("calibration")):
            crop = _positive(cal.get("cropfactor") or lens.findtext("cropfactor"))
            aspect = _aspect(cal.get("aspect-ratio") or lens.findtext("aspect-ratio"))
            # Off-center and non-rectilinear lenses need different geometry.
            if not crop or not aspect or lens.find("center") is not None:
                continue
            if lens.findtext("type", "rectilinear") != "rectilinear":
                continue
            profiles.append({
                "id": f"{filename}:{len(profiles)}:{index}",
                "maker": lens.findtext("maker", ""),
                "names": [n.text.strip() for n in lens.findall("model") if n.text],
                "mounts": [n.text for n in lens.findall("mount") if n.text],
                "crop": crop, "aspect": aspect,
                "distortion": _points(cal, "distortion"),
                "vignetting": _points(cal, "vignetting"),
            })
    return profiles


@lru_cache(maxsize=1)
def _database():
    cameras, names, mounts = {}, {}, {}
    for path in sorted(_DATA.glob("*.xml")):
        try:
            if path.stat().st_size > 16 * 1024 * 1024:
                raise ValueError("lens database file too large")
            root = ET.fromstring(path.read_bytes())
            if root.tag != "lensdatabase":
                raise ValueError("missing lensdatabase root")
            for mount in root.findall("mount"):
                mounts[mount.findtext("name", "")] = [n.text for n in mount.findall("compat") if n.text]
            for camera in root.findall("camera"):
                crop = _positive(camera.findtext("cropfactor"))
                if crop:
                    for name in camera.findall("model"):
                        cameras[_camera_key(camera.findtext("maker"), name.text)] = {
                            "crop": crop, "mount": camera.findtext("mount", ""),
                        }
            for profile in _parse_profiles(root, path.name):
                for name in profile["names"]:
                    key = _lens_key(name, _maker(profile["maker"]))
                    names.setdefault(key, []).append(profile)
        except (ET.ParseError, OSError, ValueError) as error:
            log.warning("Lens calibration file %s skipped: %s", path.name, error)
    return cameras, names, mounts


def _match(metadata):
    cameras, names, mounts = _database()
    if not names:
        return None, None, "Lens calibration database is unavailable; no profile applied."
    camera = cameras.get(_camera_key(metadata["make"], metadata["camera"]))
    crop = metadata["crop"] or (camera or {}).get("crop")
    if not crop:
        return None, None, "Unknown camera crop factor; no profile applied."
    allowed = {camera["mount"], *mounts.get(camera["mount"], [])} if camera else None
    # An indexed lookup keeps per-photo work independent of database size.
    words = re.findall(r"[a-z]+", metadata["lens"].casefold())
    keys = {_lens_key(metadata["lens"], maker) for maker in ("", *words)}
    found = {p["id"]: p for key in keys for p in names.get(key, [])}
    candidates = []
    for profile in found.values():
        maker = _maker(profile["maker"])
        if metadata["lens_make"] and _maker(metadata["lens_make"]) != maker:
            continue
        key = _lens_key(metadata["lens"], maker)
        if not key or not any(key == _lens_key(name, maker) for name in profile["names"]):
            continue
        if allowed is not None and not allowed.intersection(profile["mounts"]):
            continue
        if profile["crop"] > crop * 1.03:
            continue  # Calibration on a smaller sensor cannot cover this frame.
        candidates.append(profile)
    if not candidates:
        return None, crop, "No exact matching lens calibration for this camera and lens."
    nearest = min(abs(p["crop"] - crop) for p in candidates)
    candidates = [p for p in candidates if abs(abs(p["crop"] - crop) - nearest) < 1e-6]
    signatures = {json.dumps({k: p[k] for k in ("maker", "distortion", "vignetting", "crop", "aspect")}, sort_keys=True) for p in candidates}
    if len(signatures) != 1:
        return None, crop, "Ambiguous lens calibration; no profile applied."
    return candidates[0], crop, ""


def _interpolate(points, value, key, evaluate, *, clamp=False):
    points = sorted(points, key=lambda p: p[key])
    if not points:
        return None
    tolerance = max(0.01, value * 0.01)
    if not clamp and (value < points[0][key] - tolerance or value > points[-1][key] + tolerance):
        return None
    lower = max((p for p in points if p[key] <= value), key=lambda p: p[key], default=points[0])
    upper = min((p for p in points if p[key] >= value), key=lambda p: p[key], default=points[-1])
    a, b = evaluate(lower), evaluate(upper)
    if a is None or b is None:
        return None
    t = (value - lower[key]) / (upper[key] - lower[key]) if upper[key] != lower[key] else 0
    return a + (b - a) * t


def _distortion_coefficients(point):
    model = point["model"]
    k1, k2 = point.get("k1", 0), point.get("k2", 0)
    a, b, c = point.get("a", 0), point.get("b", 0), point.get("c", 0)
    if model == "poly3":
        return np.array([1 - k1, 0, k1, 0, 0])
    if model == "poly5":
        return np.array([1, 0, k1, 0, k2])
    if model == "ptlens":
        return np.array([1 - a - b - c, c, b, a, 0])
    if model == "none":
        return np.array([1, 0, 0, 0, 0])
    return None


def _vignetting(profile, metadata):
    if not metadata["aperture"]:
        return None
    points = [p for p in profile["vignetting"] if p["model"] == "pa"
              and _positive(p.get("aperture")) and _positive(p.get("distance", 1000))]
    focals = [{"focal": f} for f in sorted({p["focal"] for p in points})]

    def at_focal(point):
        subset = [p for p in points if p["focal"] == point["focal"]]
        inverse_distance = 1 / metadata["distance"] if metadata["distance"] else 0
        distance = min((p.get("distance", 1000) for p in subset), key=lambda d: abs(1 / d - inverse_distance))
        samples = [{**p, "stops": math.log2(p["aperture"])} for p in subset if p.get("distance", 1000) == distance]
        return _interpolate(samples, math.log2(metadata["aperture"]), "stops",
                            lambda p: np.array([p.get(k, 0) for k in ("k1", "k2", "k3")]), clamp=True)

    return _interpolate(focals, metadata["focal"], "focal", at_focal)


def resolve_profile(photo=None):
    """Return a JSON-safe match/status plus radial coefficients for this photo.

    This reports available calibration; applying it additionally requires a
    decoded scene-linear RAW. Embedded JPEGs and rendered fallbacks skip it.
    """
    metadata = lens_metadata(photo)
    result = {"matched": False, "lens": metadata["lens"], "message": "", "warp": None, "vignette": None}
    if metadata["extension"] not in _RAW_EXTENSIONS:
        return {**result, "message": "Profiles apply only to decoded RAW originals; rendered images may already be corrected."}
    if not metadata["lens"] or not metadata["focal"]:
        return {**result, "message": "Lens name and focal length are required for automatic matching."}
    if not set(re.findall(r"[a-z]+", metadata["lens"].casefold())) - {"mm", "f"}:
        return {**result, "message": "A focal/aperture specification alone cannot identify the lens; no profile applied."}
    profile, crop, error = _match(metadata)
    if profile is None:
        return {**result, "message": error}
    radius_scale = profile["crop"] / crop
    dist_scale = radius_scale * math.sqrt(1 + profile["aspect"] ** 2)
    warp = _interpolate(profile["distortion"], metadata["focal"], "focal", _distortion_coefficients)
    vignette = _vignetting(profile, metadata)
    radii = np.linspace(0, 1, 257)
    if warp is not None:
        warp *= np.power(dist_scale, np.arange(5))
        ratio = np.polynomial.polynomial.polyval(radii, warp)
        derivative = np.polynomial.polynomial.polyval(radii, warp * np.arange(1, 6))
        if np.isfinite(ratio).all() and np.min(ratio) >= 0.3 and np.max(ratio) <= 3 and np.min(derivative) > 0.05:
            result["warp"] = warp.tolist()
    if vignette is not None:
        vignette *= np.power(radius_scale, [2, 4, 6])
        attenuation = 1 + radii ** 2 * np.polynomial.polynomial.polyval(radii ** 2, vignette)
        if np.isfinite(attenuation).all() and np.min(attenuation) >= 0.1 and np.max(attenuation) <= 2:
            result["vignette"] = vignette.tolist()
    kinds = [name for name, key in (("distortion", "warp"), ("vignetting", "vignette")) if result[key] is not None]
    result.update(matched=bool(kinds), lens=profile["names"][0], profile_id=profile["id"],
                  focal=metadata["focal"], crop=crop, aperture=metadata["aperture"], revision=_REVISION)
    result["message"] = ("Matched " + ", ".join(kinds) + "." if kinds else "No supported calibration at this focal length/aperture; no profile applied.")
    if result["vignette"] is not None:
        result["message"] += " Vignetting uses the nearest calibrated focus distance and aperture endpoints when needed."
    return result


def render_cache_key(photo, recipe):
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return None
    value = json.dumps(resolve_profile(photo), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
