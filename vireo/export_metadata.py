"""Selective metadata for rendered derivatives; see docs/export-metadata.md."""

import json
import re
from datetime import datetime


def metadata_object(raw):
    """Decode catalog metadata without failing an otherwise usable export."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return {}
    return raw if isinstance(raw, dict) else {}


def photo_value(photo, key, default=None):
    """Read either a sqlite Row or a mapping."""
    try:
        return photo[key]
    except (KeyError, IndexError, TypeError):
        return default


def assignment(tag, value):
    """Reject line-oriented ExifTool arguments in metadata values."""
    text = str(value).replace("\r", " ").replace("\n", " ")
    return f"-{tag}={text}"


def timestamp_parts(timestamp):
    """Return validated date, IPTC time, EXIF/XMP times, fraction and offset."""
    value = str(timestamp or "").strip()
    match = re.fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})"
        r"(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?", value,
    )
    if not match:
        return (None,) * 6
    year, month, day, hour, minute, second, subsecond, offset = match.groups()
    offset = "+00:00" if offset == "Z" else offset
    if offset and not valid_offset(offset):
        return (None,) * 6
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return (None,) * 6
    date = f"{year}:{month}:{day}"
    time = f"{hour}:{minute}:{second}"
    suffix = (f".{subsecond}" if subsecond else "") + (offset or "")
    return date, time + (offset or ""), f"{date} {time}", f"{year}-{month}-{day}T{time}{suffix}", subsecond, offset


def valid_offset(value):
    """Accept explicit real-world UTC offsets, never a machine-local zone."""
    from capture_time import parse_offset_minutes

    return parse_offset_minutes(value) is not None


def capture_key(parts):
    """Compare wall times at scanner microsecond precision (24 == 240000)."""
    # The scanner truncates fractions beyond six digits; comparing at that
    # precision keeps a source `.1234567` from being read as a changed time
    # against a catalog `.123456` written from the same capture.
    return parts[2], (parts[4] or "")[:6].rstrip("0")


def source_capture(metadata):
    """Read a paired original (or, only if absent, digitized) EXIF time."""
    exif = metadata_object(metadata.get("EXIF"))
    original = bool(exif.get("DateTimeOriginal"))
    value = exif.get("DateTimeOriginal") if original else exif.get("CreateDate")
    text = str(value or "")
    text = text[:10].replace(":", "-") + text[10:]
    subsec = exif.get("SubSecTimeOriginal" if original else "SubSecTimeDigitized")
    if subsec is None:
        subsec = exif.get("SubSecTime")
    # A malformed fraction cannot establish that an offset belongs to this time.
    if subsec is not None:
        digits = str(subsec).strip()
        if not re.fullmatch(r"\d+", digits):
            return (None,) * 6, None, "invalid_source_time"
    parts = timestamp_parts(text)
    if subsec is not None and parts[2] and parts[4] is None:
        base = parts[2][:10].replace(":", "-") + "T" + parts[2][11:]
        parts = timestamp_parts(base + "." + digits + (parts[5] or ""))
    if subsec is not None and parts[2] and (parts[4] or "").rstrip("0") != digits.rstrip("0"):
        return parts, None, "conflicting_source_subseconds"
    offset = exif.get("OffsetTimeOriginal" if original else "OffsetTimeDigitized")
    if offset is None:
        offset = exif.get("OffsetTime")
    if offset is not None and not valid_offset(offset):
        return parts, None, "invalid_source_offset"
    if offset is not None:
        offset = str(offset).strip()
    offsets = {v for v in (offset, parts[5]) if v is not None}
    # XMP is corroborating evidence only when it describes this same capture.
    xmp = metadata_object(metadata.get("XMP"))
    xmp_text = str(xmp.get("DateTimeOriginal") or "")
    xmp_parts = timestamp_parts(xmp_text[:10].replace(":", "-") + xmp_text[10:])
    if parts[2] and capture_key(xmp_parts) == capture_key(parts) and xmp_parts[5]:
        offsets.add(xmp_parts[5])
    if len(offsets) > 1:
        return parts, None, "conflicting_source_offsets"
    status = "source_exif" if original else "source_exif_digitized"
    if offsets and offset is None and parts[5] is None:
        status = "source_xmp"
    return parts, next(iter(offsets), None), status


def resolve_capture(timestamp, metadata, report):
    """Catalog time wins; recover an offset only for an identical wall time."""
    parts = timestamp_parts(timestamp)
    if not parts[2]:
        report["capture_timestamp"] = "unavailable:invalid_or_missing_catalog_time"
        return parts
    report["capture_timestamp"] = "catalog"
    if parts[5]:
        report["capture_offset"] = "catalog"
        return parts
    source_parts, offset, status = source_capture(metadata)
    if not source_parts[2]:
        status = "invalid_or_missing_source_time"
    elif capture_key(parts) != capture_key(source_parts):
        status = "timestamp_changed"
    elif offset:
        report["capture_offset"] = status
        return timestamp_parts(str(timestamp).strip() + offset)
    elif status.startswith("source_"):
        status = "missing_source_offset"
    report["capture_offset"] = "unavailable:" + status
    return parts


def capture_args(selected, photo, metadata, report):
    if not {"capture_date", "capture_time"} & selected:
        return []
    details = {}
    parts = resolve_capture(photo_value(photo, "timestamp"), metadata, details)
    report["capture_timestamp"] = details["capture_timestamp"]
    if "capture_time" in selected:
        report["capture_offset"] = details.get("capture_offset", "unavailable:missing_capture_time")
    date, time, exif_time, xmp_time, subsecond, offset = parts
    if "capture_time" in selected:
        report["capture_subsecond"] = "catalog" if subsecond else "unavailable:missing_subsecond"
        if subsecond and "capture_date" not in selected:
            report["capture_subsecond"] = "unavailable:requires_capture_date"
    args = []
    if "capture_date" in selected and date:
        args.append(assignment("IPTC:DateCreated", date))
    if "capture_time" in selected and time and offset:
        # IPTC IIM cannot represent fractional seconds. ExifTool would invent
        # the host's timezone if asked to write this field without an offset.
        args.append(assignment("IPTC:TimeCreated", time))
        report["iptc_capture_time"] = details["capture_offset"]
    elif "capture_time" in selected:
        report["iptc_capture_time"] = "unavailable:requires_capture_offset"
    if not {"capture_date", "capture_time"} <= selected or not exif_time:
        return args
    for tag in ("EXIF:DateTimeOriginal", "EXIF:CreateDate"):
        args.append(assignment(tag, exif_time))
    for tag in ("XMP-exif:DateTimeOriginal", "XMP-xmp:CreateDate", "XMP-photoshop:DateCreated"):
        args.append(assignment(tag, xmp_time))
    if subsecond:
        for tag in ("EXIF:SubSecTimeOriginal", "EXIF:SubSecTimeDigitized"):
            args.append(assignment(tag, subsecond))
    if offset:
        for tag in ("EXIF:OffsetTimeOriginal", "EXIF:OffsetTimeDigitized"):
            args.append(assignment(tag, offset))
    return args


_PLACE_TAGS = {
    "place_name": ("XMP-iptcCore:Location", "IPTC:Sub-location", "XMP-iptcExt:LocationShownSublocation"),
    "city": ("XMP-photoshop:City", "IPTC:City", "XMP-iptcExt:LocationShownCity"),
    "state": ("XMP-photoshop:State", "IPTC:Province-State", "XMP-iptcExt:LocationShownProvinceState"),
    "country": ("XMP-photoshop:Country", "IPTC:Country-PrimaryLocationName", "XMP-iptcExt:LocationShownCountryName"),
}


def typed_place_fields(location, metadata):
    """Use only typed names present in the current catalog hierarchy."""
    path = location.get("hierarchy", [])
    fields = {"place_name": (path[-1], "catalog")} if path else {}
    if not path:
        return fields
    names = {name.casefold(): name for name in path}
    details = metadata_object(location.get("place_details"))
    # Exact place identity and leaf name keep stale cache entries from assigning
    # unrelated geography after a rename or deliberate catalog edit.
    if details.get("place_id") == location.get("place_id") and details.get("name") == path[-1]:
        component_fields = {"locality": "city", "postal_town": "city",
                            "administrative_area_level_1": "state", "country": "country"}
        for component in details.get("address_components") or []:
            if not isinstance(component, dict):
                continue
            name = component.get("name") or component.get("long_name")
            if not isinstance(name, str) or name.casefold() not in names:
                continue
            for kind in component.get("types") or []:
                if kind in component_fields:
                    fields.setdefault(component_fields[kind], (names[name.casefold()], "cached_place"))
    xmp = metadata_object(metadata.get("XMP"))
    iptc = metadata_object(metadata.get("IPTC"))
    for field, xmp_tag, iptc_tag in (
        ("city", "City", "City"), ("state", "State", "Province-State"),
        ("country", "Country", "Country-PrimaryLocationName"),
    ):
        # Reject conflicting labels, including a stale XMP value beside IPTC.
        values = {v.casefold() for v in (xmp.get(xmp_tag), iptc.get(iptc_tag)) if isinstance(v, str) and v}
        if len(values) == 1:
            value = next(iter(values))
            if value in names:
                fields.setdefault(field, (names[value], "source_metadata"))
    return fields


def location_args(photo, location, metadata, report):
    """Effective catalog GPS plus the current named-place hierarchy."""
    location = dict(location) if location is not None else dict(photo)
    args = []
    latitude, longitude = location.get("latitude"), location.get("longitude")
    report["location_coordinates"] = "unavailable:missing_coordinates"
    if latitude is not None and longitude is not None:
        latitude, longitude = float(latitude), float(longitude)
        for tag, value in (
            ("EXIF:GPSLatitude", abs(latitude)), ("EXIF:GPSLatitudeRef", "N" if latitude >= 0 else "S"),
            ("EXIF:GPSLongitude", abs(longitude)), ("EXIF:GPSLongitudeRef", "E" if longitude >= 0 else "W"),
            ("XMP-exif:GPSLatitude", latitude), ("XMP-exif:GPSLongitude", longitude),
        ):
            args.append(assignment(tag, value))
        report["location_coordinates"] = "catalog_" + location.get("source", "photo")
    path = location.get("hierarchy", [])
    if path:
        args.append(assignment("XMP-lr:HierarchicalSubject", "|".join(path)))
    report["location_hierarchy"] = "catalog" if path else "unavailable:missing_hierarchy"
    fields = typed_place_fields(location, metadata)
    for field, tags in _PLACE_TAGS.items():
        report["location_" + field] = fields[field][1] if field in fields else "unavailable:untyped_or_missing"
        if field in fields:
            value = fields[field][0]
            limit = 64 if field == "country" else 32
            for tag in tags:
                if tag.startswith("IPTC:") and len(value.encode("utf-8")) > limit:
                    report["iptc_location_" + field] = f"unavailable:exceeds_{limit}_bytes"
                else:
                    args.append(assignment(tag, value))
    return args


def export_metadata_args(fields, photo, species, camera_data, location_data=None, *, source_metadata=None, report=None):
    """Allowlist selected metadata; never copy original image geometry."""
    selected = set(fields)
    metadata = metadata_object(source_metadata)
    report = report if report is not None else {}
    args = []
    if "species" in selected:
        report["species_keywords"] = "catalog" if species else "unavailable:no_species"
        for index, name in enumerate(species):
            for tag in ("XMP-dc:Subject", "IPTC:Keywords"):
                args.append(assignment(tag + ("+" if index else ""), name))
    args.extend(capture_args(selected, photo, metadata, report))
    if "rating" in selected:
        rating = photo_value(photo, "rating")
        report["rating"] = "catalog" if rating is not None else "unavailable:no_rating"
        if rating is not None:
            args.append(assignment("XMP-xmp:Rating", int(rating)))
    if "location" in selected:
        args.extend(location_args(photo, location_data, metadata, report))
    if "camera" in selected:
        tags = {"Make": "camera_make", "Model": "camera_model", "LensModel": "lens",
                "FocalLength": "focal_length", "FNumber": "aperture", "ExposureTime": "shutter_speed", "ISO": "iso"}
        camera_written = False
        for tag, key in tags.items():
            if camera_data.get(key) not in (None, ""):
                args.append(assignment("EXIF:" + tag, camera_data[key]))
                camera_written = True
        report["camera"] = "catalog" if camera_written else "unavailable:no_camera_data"
    if any(arg.startswith("-IPTC:") for arg in args):
        args.append("-IPTC:CodedCharacterSet=UTF8")
    return args
